"""Rebuild `runs/base_bouldering.pkl` from the authoritative S3 history.

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
`first_send` and `last_send`, and reads from S3 rather than the local mirror.

Local mirror vs S3
------------------
`data/kaya_data.db` is a sqlite mirror that was last written in June 2025. The
Aug 2026 snapshot contains 20,014 (climber, gym) pairs; the mirror covers 9,965
of them — 49.8%. Every dated measurement made before this script existed was
therefore made on half the data. S3 (`kaya/raw/sends/`) is the pull history
itself, complete and current, and the only source that includes the gyms
backfilled since the mirror went stale.

Duplicate sends
---------------
Raw S3 is **not** deduplicated, and it genuinely contains duplicates: a
backfill killed mid-gym leaves its already-written batches under an orphan
run_id, and the redo writes the same sends again under a new one. Nothing
errors and nothing warns — the rows are simply counted twice by anything
reading raw S3. `--dedupe` (on by default) drops repeated send_ids before
aggregating, and reports how many it found, because a large count means orphans
worth cleaning up with `backfill_new_gyms.py --clean-orphans`.

    python scripts/build_base_snapshot.py --dry-run     # report, write nothing
    python scripts/build_base_snapshot.py               # rebuild from S3
    python scripts/build_base_snapshot.py --source local_db   # the old, stale path

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
    p.add_argument('--source', default='s3_raw',
                   choices=['s3_raw', 'local_db', 'aws_db'],
                   help='where sends come from (default: s3_raw, the '
                        'authoritative history)')
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
