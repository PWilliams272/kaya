"""Is a failing gym a DATA problem, or are we being throttled?

On 2026-08-06 a backfill lost 11 of 13 gyms to `INTERNAL_SERVER_ERROR` from
Kaya's `webAscentsForGym`. The pattern argued against per-gym data -- gym 47
failed at offset 0 before any record was read, four unrelated gyms failed at
exactly offset 150, and two gyms that had pulled cleanly hours earlier were
among the casualties -- but "argued against" is not a test.

This is the test, and it is an A/B with a control on BOTH sides:

    control (known-good gym)  ->  suspect (known-failing gym)  ->  control again

  * Control fails FIRST                  -> the API is unwell right now;
                                            the run tells us nothing else.
  * Control ok, suspect fails,
    control ok again                     -> the failure follows the GYM.
  * Control ok, suspect fails,
    control now fails too                -> the failure follows US: whatever
                                            the suspect did exhausted a quota,
                                            i.e. throttling.

The trailing control is what the naive version of this test omits, and without
it a failure at the end cannot be told apart from the whole service being down.

Deliberately tiny: one page of 15 sends per probe, one retry, a pause between
requests. This is a diagnostic against somebody else's API, not a pull.

    python scripts/probe_kaya_api.py                      # default suspects
    python scripts/probe_kaya_api.py --suspects 47,904

Run from the repo root.
"""
import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
load_dotenv(override=False)
os.environ.setdefault('AWS_PROFILE', 'admin')

from kaya.data_puller import get_data_for_gym  # noqa: E402

# Pulled cleanly at the start of the failed run, so a failure here is about the
# service or about us -- it cannot be about their data.
DEFAULT_CONTROL = '37'          # Momentum Silver Street
# 47 first: it failed at offset 0, so no volume was involved and it is the
# cleanest possible separation of "bad data" from "too many requests".
DEFAULT_SUSPECTS = '47,904'


def probe(gym_id, label, offset=0, pause=2.0):
    """One page. Returns (ok, detail)."""
    time.sleep(pause)
    t0 = time.time()
    try:
        df = get_data_for_gym(gym_id, offset=offset)
        n = 0 if df is None else len(df)
        return True, f'{n} sends in {time.time() - t0:.1f}s'
    except Exception as exc:      # noqa: BLE001 - the error IS the result
        msg = str(exc)
        short = 'INTERNAL_SERVER_ERROR' if 'INTERNAL_SERVER_ERROR' in msg \
            else msg[:120]
        return False, f'{short} (after {time.time() - t0:.1f}s)'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--control', default=DEFAULT_CONTROL,
                    help='a gym known to have pulled cleanly')
    ap.add_argument('--suspects', default=DEFAULT_SUSPECTS)
    ap.add_argument('--offset', type=int, default=0)
    ap.add_argument('--pause', type=float, default=2.0,
                    help='seconds between requests; this is somebody '
                         "else's service")
    args = ap.parse_args()
    suspects = [s.strip() for s in args.suspects.split(',') if s.strip()]

    print(f'control gym {args.control}, suspects {", ".join(suspects)}, '
          f'one 15-send page each, {args.pause}s apart\n')

    ok, detail = probe(args.control, 'control (before)', args.offset, args.pause)
    print(f'  {"control BEFORE":>22}  gym {args.control:>5}  '
          f'{"OK " if ok else "FAIL"}  {detail}')
    if not ok:
        print('\nINCONCLUSIVE: the control failed before any suspect was tried.')
        print('The API is refusing a gym that pulled cleanly hours ago, so '
              'nothing here\nseparates a data problem from a service problem. '
              'Wait and re-run.')
        return 2

    results = []
    for gid in suspects:
        ok_s, det_s = probe(gid, 'suspect', args.offset, args.pause)
        print(f'  {"suspect":>22}  gym {gid:>5}  '
              f'{"OK " if ok_s else "FAIL"}  {det_s}')
        ok_c, det_c = probe(args.control, 'control (after)', args.offset, args.pause)
        print(f'  {"control AFTER":>22}  gym {args.control:>5}  '
              f'{"OK " if ok_c else "FAIL"}  {det_c}')
        results.append((gid, ok_s, ok_c))

    print()
    verdicts = []
    for gid, ok_s, ok_c in results:
        if ok_s:
            v = 'RECOVERED -- pulls fine now, so it was never this gym\'s data'
        elif ok_c:
            v = 'GYM-SPECIFIC -- it fails while the control still works'
        else:
            v = 'THROTTLING -- the control fails too, right after this gym'
        verdicts.append(v.split(' --')[0])
        print(f'  gym {gid:>5}: {v}')

    print()
    if all(v in ('RECOVERED',) for v in verdicts):
        print('Every suspect now pulls. The failures were transient and tied to '
              'sustained\nrequest volume, not to any gym. Retry the backfill, '
              'slower.')
    elif 'THROTTLING' in verdicts:
        print('A gym that worked hours ago stops working immediately after a '
              'suspect.\nThat is a limit on US, not their data -- and it is '
              'surfacing as a generic 500\nrather than a 429, which is why the '
              "puller's retry loop hammers instead of\nbacking off. Fix the "
              'backoff before retrying the backfill.')
    else:
        print('The control keeps working while specific gyms do not. That is a '
              'real\nper-gym problem on their side; those gyms need excluding '
              'or reporting.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
