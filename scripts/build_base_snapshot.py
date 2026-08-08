"""Rebuild `runs/base_bouldering.pkl`, the input every fit reads.

Why this script had to exist
----------------------------
Every fit and every payload builder in this repo reads
`runs/base_bouldering.pkl`, and until 2026-08-07 **nothing in the repo wrote
it**. `DATA_STORAGE_NOTES.md` said as much in a footnote — "rebuildable from
S3", with no rebuilder. So the single input the whole project rests on had no
reproducible provenance: it was produced once, by hand, and could not be
regenerated if lost or when new gyms arrived.

That gap had a second, quieter cost. The snapshot is an aggregate — one row per
(climber, gym) — and the aggregation *threw the send dates away*, keeping them
only as a visit count. That is why the model has no time in it at all: no
climber advancement, no gym grading drift, and a paired-difference comparison
that silently treats a send from 2022 and one from 2025 as simultaneous. See
docs/two-stage-and-grade-compression.md for what that costs in grades.

Both are fixed here. `prepare_base_data` now carries `max_send_date`,
`first_send` and `last_send`, and takes a `source`.

Which source, and a warning about the wrong one
-----------------------------------------------
`local_db` resolves through LOCAL_DB_URL to the live sqlite mirror
(~/.kaya/kaya_data.db, 702MB): 2.46M sends, **zero** null dates, and 100%
coverage of the modelled pairs. It is complete for every gym that has been
synced, and it is fast.

`s3_raw` is the pull history itself, and it is the only source that includes
gyms backfilled since the mirror was last synced. Slower — thousands of gzipped
JSONL objects — but authoritative and current.

Do NOT reach for `data/kaya_data.db`. It is an abandoned June 2025 copy that
still sits in the tree at a third the size, and reading it instead of the live
mirror is how a "49.8% date coverage gap" got measured, published, and believed
on 2026-08-07. There is no such gap. Nothing here opens a sqlite file by path;
go through the accessor so LOCAL_DB_URL decides.

Duplicate sends under s3_raw
----------------------------
Raw S3 is not deduplicated, and it genuinely contains duplicates: a backfill
killed mid-gym leaves its already-written batches under an orphan run_id, and
the redo writes the same sends again under a new one. Nothing errors and nothing
warns. `prepare_base_data` reduces to one row per (climber, gym) by max grade,
so duplicate send rows cannot inflate the observation count — but they do
inflate `n_visits` and `n_sends_gym`, which the exposure term reads. Prefer
`local_db` unless you specifically need gyms the mirror has not seen, and clean
orphans with `backfill_new_gyms.py --clean-orphans` before an s3_raw rebuild.

    python scripts/build_base_snapshot.py --dry-run             # report only
    python scripts/build_base_snapshot.py --source local_db     # fast, complete
    python scripts/build_base_snapshot.py --source s3_raw       # includes new gyms

Writes `runs/base_bouldering.pkl`, keeping a timestamped backup of whatever was
there before — that file is the input to every fit, so it does not get replaced
without a way back.

Run from the repo root: src/kaya/secrets.py shadows the stdlib module numpy's
bit generator imports, so running from inside src/kaya breaks numpy.
"""
from __future__ import annotations

import argparse
import pickle
import shutil
import sys
import warnings
from datetime import datetime
from pathlib import Path

warnings.filterwarnings('ignore')

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RUNS = ROOT / 'runs'
OUT = RUNS / 'base_bouldering.pkl'

# Columns the rebuild must produce for the fits to keep working unchanged, plus
# the three date columns that are the point of the exercise.
REQUIRED = {'user_id', 'gym_id', 'climb_id', 'm', 'n_visits', 'n_sends_gym',
            'n_at_max'}
DATE_COLS = {'max_send_date', 'first_send', 'last_send'}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    # local_db by default: it goes through LOCAL_DB_URL to the live mirror,
    # which is complete, fully dated and far faster than re-reading thousands
    # of S3 objects. Switch to s3_raw when the mirror has not seen a recent
    # backfill.
    p.add_argument('--source', default='local_db',
                   choices=['local_db', 's3_raw', 'aws_db'],
                   help='where sends come from (default: local_db, the live '
                        'mirror via LOCAL_DB_URL)')
    p.add_argument('--discipline', default='bouldering',
                   choices=['bouldering', 'routes'])
    p.add_argument('--out', default=str(OUT))
    p.add_argument('--dry-run', action='store_true',
                   help='build and report, but write nothing')
    p.add_argument('--no-backup', action='store_true',
                   help='overwrite without keeping the previous snapshot')
    return p.parse_args(argv)


def describe(base: dict) -> None:
    obs, users = base['observations'], base['users']
    print(f'\n  observations : {len(obs):,} (climber, gym) pairs')
    print(f'  climbers     : {obs.user_id.nunique():,}')
    print(f'  gyms         : {obs.gym_id.nunique():,}')
    print(f'  users table  : {len(users):,}')

    missing = REQUIRED - set(obs.columns)
    if missing:
        print(f'  !! MISSING required columns: {sorted(missing)}')
    have_dates = DATE_COLS & set(obs.columns)
    if have_dates != DATE_COLS:
        print(f'  !! date columns absent: {sorted(DATE_COLS - have_dates)} '
              f'— this snapshot cannot support any time-based work')
        return

    d = pd.to_datetime(obs.max_send_date, errors='coerce')
    ok = d.notna()
    print(f'\n  dated rows   : {int(ok.sum()):,} / {len(obs):,} '
          f'({100 * ok.mean():.1f}%)')
    if ok.any():
        print(f'  date range   : {d[ok].min().date()} -> {d[ok].max().date()}')
        span = (d[ok].max() - d[ok].min()).days / 365.25
        print(f'  span         : {span:.1f} years')


def compare_to_previous(new: dict, previous_path: Path) -> None:
    """Say what changed, so a rebuild is never a silent replacement."""
    if not previous_path.exists():
        print('\n  no previous snapshot to compare against')
        return
    try:
        with open(previous_path, 'rb') as f:
            old = pickle.load(f)
    except Exception as exc:                       # noqa: BLE001
        print(f'\n  could not read the previous snapshot ({exc}); skipping diff')
        return

    o, n = old['observations'], new['observations']
    o_pairs = set(map(tuple, o[['user_id', 'gym_id']].values))
    n_pairs = set(map(tuple, n[['user_id', 'gym_id']].values))
    print('\n  vs the previous snapshot:')
    print(f'    rows      {len(o):,} -> {len(n):,}  ({len(n) - len(o):+,})')
    print(f'    gyms      {o.gym_id.nunique()} -> {n.gym_id.nunique()}  '
          f'({n.gym_id.nunique() - o.gym_id.nunique():+d})')
    print(f'    added     {len(n_pairs - o_pairs):,} (climber, gym) pairs')
    dropped = o_pairs - n_pairs
    if dropped:
        # Worth surfacing loudly: the new source should be a superset, so
        # anything missing means the rebuild lost data rather than gained it.
        print(f'    !! DROPPED {len(dropped):,} pairs that the old snapshot '
              f'had — investigate before trusting this rebuild')


def main(argv=None) -> int:
    args = parse_args(argv)
    sys.path.insert(0, str(ROOT / 'src'))
    from kaya.grading_model_v2 import prepare_base_data

    out = Path(args.out)
    print(f'building {args.discipline} snapshot from source={args.source!r}')
    if args.source == 's3_raw':
        print('  (reading the full S3 pull history — this takes a few minutes)')
    elif args.source == 'local_db':
        from kaya.db_manager import _get_local_db_url
        print(f'  via LOCAL_DB_URL -> {_get_local_db_url()}')

    base = prepare_base_data(discipline=args.discipline, source=args.source)
    describe(base)
    compare_to_previous(base, out)

    if args.dry_run:
        print('\ndry run: nothing written')
        return 0

    if out.exists() and not args.no_backup:
        stamp = datetime.now().strftime('%Y%m%dT%H%M%S')
        backup = out.with_suffix(f'.pkl.bak-{stamp}')
        shutil.copy2(out, backup)
        print(f'\n  backed up previous snapshot -> {backup.name}')

    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, 'wb') as f:
        pickle.dump(base, f)
    print(f'  wrote {out.relative_to(ROOT)} '
          f'({out.stat().st_size / 1e6:.1f} MB)')
    print('\nEvery fit and payload builder reads this file. Re-run the payload '
          'builders\n(scripts/build_v2_*.py) so the viewer reflects the new '
          'snapshot.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
