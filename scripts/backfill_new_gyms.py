"""Initial (`mode='full'`) pull for newly added gyms, straight into S3, resumably.

Why this is not a for-loop over `update_gym_data`
-------------------------------------------------
It nearly is. What it adds is the thing that makes a multi-hour backfill safe
to interrupt, and the reason it is needed is specific:

`update_gym_data` writes each batch to
`raw/sends/run_date=<d>/gym_id=<id>/run_id=<uuid>/batch-NNNNN.jsonl.gz`, and
calls `write_recent_send_state` **only after the last batch**. So a run killed
part-way through leaves:

  * every batch it had already written, still in S3, under a run_id nobody
    recorded, and
  * no state entry at all, so the gym still looks unpulled.

Re-running it then starts from offset 0 with a **fresh run_id** and writes the
same sends again under a different prefix. Nothing errors. Nothing warns. And
`read_sends(source='s3_raw')` does not deduplicate -- only one downstream
method drops duplicate send_ids, defensively -- so those rows are simply
counted twice by everything that reads raw S3.

This driver makes the **gym** the atomic unit:

  1. Record the run_ids that already exist for the gym.
  2. Pull it.
  3. On success, whatever run_id is new is the good one -- record it as `done`.
  4. On interruption, the gym stays `in_progress`, and any run_id that is
     neither pre-existing nor recorded as `done` is an **orphan**: a partial
     write from a killed run.

A completed gym is never pulled twice. An interrupted gym is redone from
scratch, which is correct rather than clever -- the API's offsets shift as new
sends arrive, so resuming mid-gym from a saved offset would silently skip or
double rows.

Orphans are **reported, never deleted by default**. `--clean-orphans` deletes
them, and asks first unless `--yes` is passed. Deleting is the right call
before a redo (otherwise the redo duplicates), but it deletes real data from
S3, so it does not happen as a side effect of anything.

    python scripts/backfill_new_gyms.py --dry-run          # plan only, no API
    python scripts/backfill_new_gyms.py --status           # what is done/left
    python scripts/backfill_new_gyms.py --clean-orphans    # tidy, then pull
    python scripts/backfill_new_gyms.py --gyms 37,48       # a subset

The manifest lives at `runs/backfill_<job>.json` locally AND is mirrored to
`s3://<bucket>/<prefix>/state/backfill/<job>.json` after every gym, so
progress survives losing the machine, not just losing the process.
"""
import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

load_dotenv(override=False)

# Same defaults backfill_gap_2026_to_s3.py applies, and for the same reason:
# .env carries only AWS_REGION, so a bare run would fail the S3 config check
# and look like a broken script rather than an unset variable. setdefault, not
# assignment -- an explicitly exported value always wins.
DEFAULT_S3_BUCKET = 'my-kaya-data-545009868532-us-east-2'
DEFAULT_S3_PREFIX = 'kaya'
DEFAULT_AWS_PROFILE = 'admin'
DEFAULT_AWS_REGION = 'us-east-2'

os.environ.setdefault('KAYA_S3_BUCKET', DEFAULT_S3_BUCKET)
os.environ.setdefault('KAYA_S3_PREFIX', DEFAULT_S3_PREFIX)
os.environ.setdefault('AWS_PROFILE', DEFAULT_AWS_PROFILE)
if not os.getenv('AWS_REGION') and not os.getenv('AWS_DEFAULT_REGION'):
    os.environ['AWS_REGION'] = DEFAULT_AWS_REGION

from kaya.data_puller import update_gym_data  # noqa: E402
from kaya.gym_config import load_gyms_config  # noqa: E402
from kaya.s3_storage import (  # noqa: E402
    get_s3_bucket,
    get_s3_client,
    get_s3_prefix,
    has_s3_storage_config,
)

logger = logging.getLogger('backfill_new_gyms')

DEFAULT_JOB = 'new-gyms-2026-08'


# ---- manifest ------------------------------------------------------------

class Manifest:
    """Per-gym backfill progress, on disk and mirrored to S3.

    Written after every gym, and again from a `finally` on the way out, so an
    interrupted run records where it got to rather than losing the whole map
    of what had already succeeded.
    """

    def __init__(self, job: str, path: Path) -> None:
        self.job = job
        self.path = path
        self.data: Dict[str, Any] = {'job': job, 'gyms': {}}
        if path.exists():
            self.data = json.loads(path.read_text())
            self.data.setdefault('gyms', {})

    def gym(self, gym_id: str) -> Dict[str, Any]:
        return self.data['gyms'].setdefault(str(gym_id), {'state': 'pending'})

    def is_done(self, gym_id: str) -> bool:
        return self.gym(gym_id).get('state') == 'done'

    def done_run_ids(self, gym_id: str) -> Set[str]:
        g = self.gym(gym_id)
        ids = set(g.get('known_run_ids') or [])
        if g.get('run_id'):
            ids.add(g['run_id'])
        return ids

    def save(self, mirror: bool = True) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.data['updated_at'] = datetime.now(timezone.utc).isoformat()
        self.path.write_text(json.dumps(self.data, indent=2, sort_keys=True))
        if mirror and has_s3_storage_config():
            try:
                get_s3_client().put_object(
                    Bucket=get_s3_bucket(),
                    Key=f'{get_s3_prefix()}/state/backfill/{self.job}.json',
                    Body=json.dumps(self.data, indent=2, sort_keys=True).encode(),
                    ContentType='application/json',
                )
            except Exception as exc:   # noqa: BLE001 - mirroring is best-effort
                logger.warning('could not mirror manifest to S3: %s', exc)


# ---- S3 inspection -------------------------------------------------------

# gym_id is not the FIRST partition of the raw key
# (`raw/sends/run_date=<d>/gym_id=<id>/run_id=<r>/...`), so S3 cannot serve a
# per-gym prefix query -- answering "which runs exist for gym X" means walking
# the whole `raw/sends/` listing. Doing that once per gym was ~4.4s x 109 gyms
# of pure preflight, paid again on every resume. Walk it once, index it by
# gym, and refresh only where a pull has just added something.
_RUN_INDEX: Optional[Dict[str, Dict[str, int]]] = None


def scan_run_index(refresh: bool = False) -> Dict[str, Dict[str, int]]:
    """{gym_id: {run_id: object_count}} for the whole raw sends area."""
    global _RUN_INDEX
    if _RUN_INDEX is not None and not refresh:
        return _RUN_INDEX
    index: Dict[str, Dict[str, int]] = {}
    if not has_s3_storage_config():
        _RUN_INDEX = index
        return index
    client = get_s3_client()
    bucket, prefix = get_s3_bucket(), get_s3_prefix()
    token, pages = None, 0
    while True:
        req: Dict[str, Any] = {'Bucket': bucket, 'Prefix': f'{prefix}/raw/sends/'}
        if token:
            req['ContinuationToken'] = token
        resp = client.list_objects_v2(**req)
        pages += 1
        for item in resp.get('Contents', []):
            key = item['Key']
            if '/gym_id=' not in key or '/run_id=' not in key:
                continue
            gym_id = key.split('/gym_id=', 1)[1].split('/', 1)[0]
            run_id = key.split('/run_id=', 1)[1].split('/', 1)[0]
            runs = index.setdefault(gym_id, {})
            runs[run_id] = runs.get(run_id, 0) + 1
        if not resp.get('IsTruncated'):
            break
        token = resp.get('NextContinuationToken')
    logger.info('indexed %s gyms from %s S3 listing page(s)', len(index), pages)
    _RUN_INDEX = index
    return index


def gym_run_ids(gym_id: str, refresh: bool = False) -> Dict[str, int]:
    """Every run_id present in raw S3 for this gym, and its object count.

    Reads the whole `raw/sends/` area rather than one run_date prefix: a run
    started before midnight UTC and finishing after it writes under the date
    it STARTED, so keying on today's date would miss it.
    """
    return dict(scan_run_index(refresh=refresh).get(str(gym_id), {}))


# States in which this job has a run of its own that may have died part-way.
# A gym it never started cannot have been orphaned BY it. 'failed' belongs here
# for the same reason 'in_progress' does: the pull got far enough to write
# objects before the API gave up on it.
IN_FLIGHT_STATES = {'in_progress', 'blocked_orphans', 'failed'}


def orphan_run_ids(gym_id: str, manifest: Manifest,
                   before: Optional[Set[str]] = None) -> Dict[str, int]:
    """run_ids left behind by an interrupted run OF THIS JOB.

    The `in_flight` guard is the whole correctness of this function, and
    leaving it out was actively dangerous. Every gym already in the pull list
    carries a run_id per daily `kaya-data-updater` invocation -- months of
    them. Without the guard, "any run_id not in my manifest" classified all of
    that production history as orphaned, and `--clean-orphans` would have
    offered to delete the entire raw send history for 89 gyms.

    A gym is only at risk of a partial write if this job actually started it,
    which is precisely the states below. Everything present when it started is
    recorded as `preexisting_run_ids` and is never a candidate.
    """
    rec = manifest.gym(gym_id)
    if rec.get('state') not in IN_FLIGHT_STATES:
        return {}
    known = manifest.done_run_ids(gym_id) | set(rec.get('preexisting_run_ids') or [])
    if before is not None:
        known |= before
    return {r: n for r, n in gym_run_ids(gym_id).items() if r not in known}


def delete_run(gym_id: str, run_id: str) -> int:
    """Delete every object under one gym's run_id prefix."""
    client = get_s3_client()
    bucket, prefix = get_s3_bucket(), get_s3_prefix()
    marker = f'/gym_id={gym_id}/run_id={run_id}/'
    deleted, token = 0, None
    while True:
        req: Dict[str, Any] = {'Bucket': bucket, 'Prefix': f'{prefix}/raw/sends/'}
        if token:
            req['ContinuationToken'] = token
        resp = client.list_objects_v2(**req)
        batch = [{'Key': i['Key']} for i in resp.get('Contents', [])
                 if marker in i['Key']]
        for start in range(0, len(batch), 1000):
            client.delete_objects(
                Bucket=bucket, Delete={'Objects': batch[start:start + 1000]})
            deleted += len(batch[start:start + 1000])
        if not resp.get('IsTruncated'):
            return deleted
        token = resp.get('NextContinuationToken')


# ---- the run ------------------------------------------------------------

def resolve_gyms(arg: Optional[str]) -> List[Dict[str, str]]:
    """Which gyms to back-fill: the config list, optionally narrowed by id."""
    cfg = load_gyms_config()
    rows = [{'gym_id': str(r.gym_id), 'gym_name': str(r.gym_name)}
            for r in cfg.itertuples()]
    if arg:
        wanted = {s.strip() for s in arg.split(',') if s.strip()}
        missing = wanted - {r['gym_id'] for r in rows}
        if missing:
            raise SystemExit(f'gym ids not in gyms_to_update.json: {sorted(missing)}')
        rows = [r for r in rows if r['gym_id'] in wanted]
    return rows


def is_new_gym(gym_id: str) -> bool:
    """No raw sends in S3 at all, i.e. this gym has genuinely never been pulled."""
    return not gym_run_ids(gym_id)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument('--job', default=DEFAULT_JOB)
    ap.add_argument('--gyms', default=None,
                    help='comma-separated gym ids; default is every gym in '
                         'gyms_to_update.json that is not already done')
    ap.add_argument('--manifest', default=None)
    ap.add_argument('--batch-size', type=int, default=1000)
    ap.add_argument('--dry-run', action='store_true',
                    help='print the plan and touch nothing')
    ap.add_argument('--status', action='store_true',
                    help='report progress and orphans, pull nothing')
    ap.add_argument('--between-gyms', type=float, default=30.0,
                    help="seconds to pause between gyms, so a long backfill "
                         "does not present as one uninterrupted burst to "
                         "somebody else's API")
    ap.add_argument('--cooloff', type=float, default=300.0,
                    help='seconds to wait after a gym fails before starting '
                         'the next one. A failure is the API asking for room.')
    ap.add_argument('--max-consecutive-failures', type=int, default=3,
                    help='stop the run after this many gyms fail in a row. '
                         'Several in a row is backpressure, not several broken '
                         'gyms -- see the 2026-08-06 note in the module docstring.')
    ap.add_argument('--clean-only', action='store_true',
                    help='delete orphaned partial writes and stop, without '
                         'starting a pull')
    ap.add_argument('--clean-orphans', action='store_true',
                    help='DELETE partial writes left by killed runs')
    ap.add_argument('--yes', action='store_true',
                    help='skip the confirmation prompt for --clean-orphans')
    ap.add_argument('--force', action='store_true',
                    help='allow a full pull of a gym that ALREADY has raw '
                         'sends in S3. This duplicates its entire history -- '
                         'there is no incremental merge on the raw layer.')
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(message)s')
    if not has_s3_storage_config():
        raise SystemExit(
            'No S3 storage configured (KAYA_S3_BUCKET / KAYA_S3_PREFIX). This '
            'job only writes to S3, so it will not run without it.')

    path = Path(args.manifest) if args.manifest else ROOT / 'runs' / f'backfill_{args.job}.json'
    manifest = Manifest(args.job, path)
    gyms = resolve_gyms(args.gyms)
    todo = [g for g in gyms if not manifest.is_done(g['gym_id'])]

    # The dangerous default this guard removes: `mode='full'` re-pulls a gym's
    # ENTIRE history, and the raw S3 layer has no merge -- it just accumulates
    # another run_id's worth of objects. Run with no --gyms and the 89 gyms
    # already on the daily updater would each be duplicated wholesale. So the
    # job means what its name says: gyms with no raw sends in S3 at all.
    if not args.dry_run:
        # A gym this job already started is EXEMPT: its S3 objects are our own
        # partial write, so "has history" is exactly the resume case rather
        # than a reason to skip. Without this exemption the guard and the
        # orphan handling cancel each other out and an interrupted gym can
        # never be finished -- which is the one job this script has.
        already = [g for g in todo
                   if manifest.gym(g['gym_id']).get('state') not in IN_FLIGHT_STATES
                   and not is_new_gym(g['gym_id'])]
        if already and not args.force:
            for g in already:
                logger.info('skipping %s (%s) -- already has raw sends in S3',
                            g['gym_name'], g['gym_id'])
            print(f'{len(already)} gym(s) already have S3 history and were '
                  f'skipped; pass --force to re-pull them anyway (it duplicates)')
            skip = {g['gym_id'] for g in already}
            todo = [g for g in todo if g['gym_id'] not in skip]

    print(f'job {args.job}   bucket {get_s3_bucket()}   manifest {path}')
    print(f'{len(gyms)} gyms in scope, {len(gyms) - len(todo)} already done, '
          f'{len(todo)} to pull\n')

    if args.status or args.dry_run:
        for g in gyms:
            rec = manifest.gym(g['gym_id'])
            state = rec.get('state', 'pending')
            extra = ''
            if state == 'done':
                # `or` rather than a get() default: a gym finished by an older
                # version of this script has the keys present but set to None,
                # and a default only fires on a missing key.
                rows = rec.get('rows_written')
                run = rec.get('run_id') or '?'
                extra = f"  {'?' if rows is None else rows} rows, run {run[:20]}"
            elif not args.dry_run:
                orph = orphan_run_ids(g['gym_id'], manifest)
                if orph:
                    extra = ('  ORPHANED: ' + ', '.join(
                        f'{r} ({n} objects)' for r, n in orph.items()))
            print(f"  {state:12s} {g['gym_id']:>6}  {g['gym_name'][:44]:46s}{extra}")
        if args.dry_run:
            print('\ndry run: nothing was pulled, nothing was written')
        return 0

    # Ctrl-C and SIGTERM both have to leave the manifest truthful, or the next
    # run cannot tell a finished gym from an abandoned one.
    interrupted = {'flag': False}

    def stop(signum, _frame):
        interrupted['flag'] = True
        logger.warning('signal %s received -- finishing the current gym is not '
                       'safe, so stopping now; it will be redone', signum)
        raise KeyboardInterrupt

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    pulled = 0
    failed: List[str] = []
    consecutive = 0
    cleaned = 0
    try:
        for i, g in enumerate(todo):
            gym_id, gym_name = g['gym_id'], g['gym_name']
            # A gap between gyms, so a 20-gym backfill does not present to the
            # API as one uninterrupted multi-hour burst. Cheap next to a pull
            # that takes half an hour, and the thing being protected is
            # somebody else's service.
            #
            # --clean-only never touches that service -- it lists and deletes
            # in our own bucket -- so pacing it just makes an S3 tidy-up take
            # 30s per gym for nothing.
            if i and args.between_gyms and not args.clean_only:
                print(f'  pausing {args.between_gyms:.0f}s before the next '
                      'gym...', flush=True)
                time.sleep(args.between_gyms)
            rec = manifest.gym(gym_id)

            orph = orphan_run_ids(gym_id, manifest)
            if orph:
                total = sum(orph.values())
                if args.clean_orphans or args.clean_only:
                    if not args.yes:
                        ans = input(
                            f'\nDELETE {total} S3 objects from {len(orph)} '
                            f'orphaned run(s) for {gym_name} ({gym_id})? [y/N] ')
                        if ans.strip().lower() not in {'y', 'yes'}:
                            print('  skipped -- leaving the orphans in place')
                            continue
                    for run_id in orph:
                        n = delete_run(gym_id, run_id)
                        logger.info('deleted %s objects from orphaned run %s', n, run_id)
                    if args.clean_only:
                        # Tidying and pulling are separate decisions. A full
                        # pull is hours long, so "remove the partial write"
                        # should not oblige anyone to start one now.
                        rec.pop('orphans', None)
                        rec['state'] = 'pending'
                        rec['cleaned_at'] = datetime.now(timezone.utc).isoformat()
                        manifest.save()
                        print(f'  cleaned {total} orphaned object(s); '
                              'not pulling (--clean-only)', flush=True)
                        cleaned += 1
                        continue
                else:
                    logger.warning(
                        '%s (%s) has %s orphaned object(s) from %s killed run(s): %s. '
                        'SKIPPING -- pulling now would duplicate them. Re-run with '
                        '--clean-orphans to remove them first.',
                        gym_name, gym_id, total, len(orph), ', '.join(orph))
                    rec['state'] = 'blocked_orphans'
                    rec['orphans'] = orph
                    manifest.save()
                    continue

            if args.clean_only:
                continue

            before = set(gym_run_ids(gym_id))
            rec.update({'state': 'in_progress', 'gym_name': gym_name,
                        'preexisting_run_ids': sorted(before),
                        'started_at': datetime.now(timezone.utc).isoformat()})
            rec.pop('orphans', None)
            manifest.save()

            print(f"\n=== {gym_name} ({gym_id}) — full pull ===", flush=True)
            try:
                update_gym_data(gym_id, mode='full', use_aws=True,
                                storage_backend='s3', batch_size=args.batch_size,
                                log_level=logging.INFO)
            except Exception as exc:   # noqa: BLE001 - one gym must not end the run
                # Kaya's API returns INTERNAL_SERVER_ERROR intermittently; the
                # puller already retries, and exhausting those retries is a
                # statement about one gym at one moment, not about the batch.
                # Letting it propagate cost 17 unattended gyms once.
                #
                # 'failed' is in IN_FLIGHT_STATES, so whatever partial objects
                # this pull wrote are detected as orphans on the next run and
                # block the retry until they are cleaned -- the same protection
                # an interrupted gym gets, for the same reason.
                logger.error('%s (%s) FAILED: %s', gym_name, gym_id, exc)
                rec.update({'state': 'failed', 'error': str(exc)[:500],
                            'failed_at': datetime.now(timezone.utc).isoformat()})
                manifest.save()
                failed.append(gym_id)
                consecutive += 1
                # Backpressure, not just failure tracking. One gym failing is a
                # gym; several in a row is the API telling the whole run to
                # stop, and the only reason it looks like N gym failures is
                # that we kept asking. On 2026-08-06 eleven gyms were burned in
                # a two-minute window this way and every one pulled fine the
                # next morning.
                if consecutive >= args.max_consecutive_failures:
                    print(f'\n{consecutive} gyms failed in a row -- treating '
                          'that as backpressure rather than as '
                          f'{consecutive} broken gyms.', file=sys.stderr)
                    print('Stopping. The remaining gyms are untouched and the '
                          'manifest records where\nthis got to; re-run the '
                          'same command later to continue.', file=sys.stderr)
                    print('Diagnose with: python scripts/probe_kaya_api.py',
                          file=sys.stderr)
                    break
                print(f'  cooling off {args.cooloff:.0f}s after a failure...',
                      flush=True)
                time.sleep(args.cooloff)
                continue

            after = gym_run_ids(gym_id, refresh=True)
            new = sorted(set(after) - before)
            rec.update({
                'state': 'done',
                'run_id': new[0] if len(new) == 1 else None,
                'known_run_ids': sorted(set(after)),
                'objects_written': sum(after[r] for r in new),
                'finished_at': datetime.now(timezone.utc).isoformat(),
            })
            manifest.save()
            pulled += 1
            consecutive = 0
            print(f"    done — {rec['objects_written']} S3 objects, "
                  f"run_id {rec['run_id']}", flush=True)
    except KeyboardInterrupt:
        print('\ninterrupted.', file=sys.stderr)
    finally:
        manifest.save()

    remaining = [g for g in gyms if not manifest.is_done(g['gym_id'])]
    if args.clean_only:
        print(f'\ncleaned {cleaned} gym(s); nothing was pulled')
        return 0
    print(f'\npulled {pulled} gym(s); {len(remaining)} still to do')
    if failed:
        print(f'{len(failed)} gym(s) failed and were skipped: {", ".join(failed)}')
        print('their partial writes are orphans -- clean them before retrying:')
        print(f'  scripts/backfill_new_gyms.py --gyms {",".join(failed)} '
              '--clean-orphans')
    if remaining:
        print('re-run the same command to continue; --status shows where it stopped')
    # A failure that was contained is still a failure worth a non-zero exit, so
    # an unattended wrapper does not report success on a partial batch.
    return 1 if (interrupted['flag'] or failed) else 0


if __name__ == '__main__':
    raise SystemExit(main())
