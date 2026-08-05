"""Backfill the 2026-02-21 to 2026-07-24 sync-outage gap (see
kaya_data_gap_lambda_outage memory / the "Sends over Time" chart dip) for
every gym except the 6 LA Touchstone gyms already backfilled by hand.

Run directly from a terminal:
    .venv/bin/python -m kaya.backfill_gap_2026

Long-running (tens of thousands of Kaya API calls across ~83 gyms, likely
several hours) -- safe to Ctrl+C and re-run later. Progress is checkpointed
per gym in a local state file, so a re-run skips gyms already completed and
only retries ones that failed or were interrupted mid-gym.

Writes to the LOCAL SQLite DB only (use_aws=False) -- never touches
S3/AWS/production RDS.
"""
import datetime as dt
import json
import time
import warnings
from pathlib import Path

# Two known-harmless FutureWarnings about pandas' *future* version behavior,
# not current-version bugs -- one from data_puller's existing fillna/astype
# on is_premium/is_private, one from concatenating batches where a column is
# all-NA in a given batch. Flushing every ~500 rows (instead of once per
# gym) means these would otherwise print constantly across an many-hour run.
warnings.filterwarnings(
    'ignore', category=FutureWarning,
    message='.*[Dd]owncasting object dtype arrays.*',
)
warnings.filterwarnings(
    'ignore', category=FutureWarning,
    message='.*DataFrame concatenation with empty or all-NA entries.*',
)

import pandas as pd
from tqdm import tqdm

from kaya.data_puller import _prepare_batch_dataframe, get_data_for_gym
from kaya.db_manager import get_engine, write_dataframe

GAP_START = dt.date(2026, 2, 21)
GAP_END = dt.date(2026, 7, 24)
REQUEST_DELAY = 0.15
MAX_RETRIES = 5

# Backfilled by hand on 2026-07-29/30; verified per-gym (no contiguous
# multi-day zero-send runs remaining) -- see memory for details.
ALREADY_DONE_GYM_IDS = {'260', '122', '261', '1100', '901', '257'}

STATE_FILE = Path.home() / '.kaya' / 'backfill_gap_2026_state.json'


def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {'completed_gym_ids': [], 'failed_gym_ids': [], 'total_written': 0}


def save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def get_all_gym_ids() -> list:
    engine = get_engine(use_aws=False)
    df = pd.read_sql_query('SELECT DISTINCT gym_id FROM sends', engine)
    return df['gym_id'].astype(str).tolist()


def fetch_with_retry(gym_id, offset):
    last_exc = None
    for attempt in range(MAX_RETRIES):
        try:
            return get_data_for_gym(gym_id, offset=offset)
        except Exception as e:
            last_exc = e
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f'Exceeded {MAX_RETRIES} retries at offset={offset} for gym {gym_id}: {last_exc}')


WRITE_BATCH_SIZE = 500  # flush to DB every ~this many collected rows, so a
# Ctrl+C or crash mid-gym loses at most a few minutes of work, not the
# whole gym -- large gyms (100+ sends/day) can take 30-45+ minutes each.


def _flush(collected, total_written):
    if not collected:
        return total_written
    full_df = pd.concat(collected, ignore_index=True)
    full_df = _prepare_batch_dataframe(full_df)
    write_dataframe(full_df, 'sends', use_aws=False, if_exists='upsert')
    return total_written + len(full_df)


def backfill_gap_for_gym(gym_id, gap_start, gap_end, page_bar) -> int:
    """Page most-recent-first, collect only in-gap rows, stop on either a
    truly empty page or a page that entirely predates gap_start. A page
    merely being *shorter* than requested is NOT a reliable end-of-data
    signal -- confirmed empirically (Class 5: a 9-row page mid-history was
    immediately followed by a full page of even older dates) -- so that is
    deliberately NOT used as a stop condition here.
    """
    offset = 0
    collected = []
    written = 0
    pending = 0
    while True:
        df = fetch_with_retry(gym_id, offset)
        if df.empty:
            break
        dates = pd.to_datetime(df['date']).dt.date
        in_gap = df[(dates >= gap_start) & (dates <= gap_end)]
        if len(in_gap):
            collected.append(in_gap)
            pending += len(in_gap)
        page_bar.update(1)
        page_bar.set_postfix(collected=written + pending, refresh=False)
        if pending >= WRITE_BATCH_SIZE:
            written = _flush(collected, written)
            collected = []
            pending = 0
        if (dates < gap_start).all():
            break
        offset += len(df)
        time.sleep(REQUEST_DELAY)

    written = _flush(collected, written)
    return written


def main() -> None:
    all_gym_ids = get_all_gym_ids()
    remaining = [g for g in all_gym_ids if g not in ALREADY_DONE_GYM_IDS]

    state = load_state()
    todo = [g for g in remaining if g not in state['completed_gym_ids']]
    already_done_this_run = len(remaining) - len(todo)

    print(f'{len(all_gym_ids)} total gyms, {len(ALREADY_DONE_GYM_IDS)} already backfilled by hand, '
          f'{len(remaining)} remaining overall, {already_done_this_run} already completed in a prior '
          f'run of this script, {len(todo)} left to process now.\n')

    gym_bar = tqdm(todo, desc='Gyms', unit='gym')
    for gym_id in gym_bar:
        gym_bar.set_postfix_str(f'gym_id={gym_id}')
        page_bar = tqdm(desc=f'  gym {gym_id}', unit='page', leave=False)
        try:
            written = backfill_gap_for_gym(gym_id, GAP_START, GAP_END, page_bar)
            state['completed_gym_ids'].append(gym_id)
            state['total_written'] += written
            if gym_id in state['failed_gym_ids']:
                state['failed_gym_ids'].remove(gym_id)
        except Exception as e:
            tqdm.write(f'[FAILED] gym {gym_id}: {e}')
            if gym_id not in state['failed_gym_ids']:
                state['failed_gym_ids'].append(gym_id)
        finally:
            page_bar.close()
            save_state(state)

    print(f'\nDone. Total rows written across all runs of this script: {state["total_written"]}')
    if state['failed_gym_ids']:
        print(f'Failed gyms (re-run this script to retry just these): {state["failed_gym_ids"]}')
    else:
        print('No failed gyms.')


if __name__ == '__main__':
    main()
