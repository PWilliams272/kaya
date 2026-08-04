"""Enumerate every gym Kaya's search API will return, into one roster file.

Why this exists: `src/kaya/config/gyms_to_update.json` (89 gyms) was built by
hand, reading IDs off gym URLs. There is no "list all gyms" endpoint, but
`webSearchForGym` does substring matching on the name, so sweeping short terms
and deduping by id reaches essentially all of them.

Writes `src/kaya/config/gyms_available.json`, sorted by follower count, each
row flagged with whether it is already in the update roster. Use it to pick
gyms to add rather than hunting URLs.

    python scripts/discover_gyms.py                  # a-z + 0-9 sweep
    python scripts/discover_gyms.py --deep           # refine any saturated term
    python scripts/discover_gyms.py --terms bp movement

Read-only: it only issues search queries, and never touches the sends tables.
"""
import argparse
import json
import string
import sys
import time
from pathlib import Path

import pandas as pd

from kaya.data_puller import search_for_gym

ROOT = Path(__file__).resolve().parents[1]
ROSTER = ROOT / 'src' / 'kaya' / 'config' / 'gyms_to_update.json'
OUT = ROOT / 'src' / 'kaya' / 'config' / 'gyms_available.json'

PAGE = 100
KEEP = ['id', 'name', 'city', 'region', 'country', 'boulder_count',
        'route_count', 'follower_count', 'is_official', 'slug', 'website']


def paged_search(term, max_pages=10, pause=0.35):
    """All results for one term, paging until a short page comes back."""
    rows = []
    for page in range(max_pages):
        try:
            df = search_for_gym(term, offset=page * PAGE, count=PAGE)
        except Exception as e:                      # transient API failures
            print(f'  {term!r} page {page}: {type(e).__name__} {e}', file=sys.stderr)
            time.sleep(2)
            continue
        if df.empty:
            break
        rows.append(df)
        if len(df) < PAGE:
            break
        time.sleep(pause)
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--terms', nargs='*', default=None,
                    help='search terms to sweep (default: a-z and 0-9)')
    ap.add_argument('--deep', action='store_true',
                    help='for any term that fills every page, sweep its '
                         'two-letter extensions as well')
    ap.add_argument('--pause', type=float, default=0.35)
    args = ap.parse_args()

    terms = args.terms or list(string.ascii_lowercase) + list(string.digits)
    found = {}
    saturated = []

    for i, term in enumerate(terms, 1):
        df = paged_search(term, pause=args.pause)
        new = 0
        for row in df.to_dict('records'):
            gid = str(row.get('id'))
            if gid not in found:
                found[gid] = row
                new += 1
        if len(df) >= PAGE * 10:
            saturated.append(term)
        print(f'[{i}/{len(terms)}] {term!r}: {len(df)} results, {new} new '
              f'({len(found)} total)', flush=True)
        time.sleep(args.pause)

    if args.deep and saturated:
        ext = [t + c for t in saturated for c in string.ascii_lowercase]
        print(f'deep sweep: {len(ext)} extended terms from {saturated}')
        for i, term in enumerate(ext, 1):
            df = paged_search(term, pause=args.pause)
            for row in df.to_dict('records'):
                found.setdefault(str(row.get('id')), row)
            if i % 20 == 0:
                print(f'  deep [{i}/{len(ext)}] {len(found)} total', flush=True)
            time.sleep(args.pause)

    roster = json.loads(ROSTER.read_text())
    have = {str(v) for v in roster.values()}

    out = []
    for gid, row in found.items():
        rec = {k: row.get(k) for k in KEEP if k in row}
        rec['id'] = gid
        rec['in_roster'] = gid in have
        out.append(rec)
    out.sort(key=lambda r: -(r.get('follower_count') or 0))

    OUT.write_text(json.dumps(out, indent=1))
    missing = [g for g in have if g not in found]
    print(f'\nwrote {OUT}')
    print(f'  {len(out)} gyms discovered, {sum(r["in_roster"] for r in out)} '
          f'already in the update roster ({len(roster)} configured)')
    if missing:
        # Not an error: a gym can be in the roster and unreachable by name
        # search (renamed, delisted, or matched by none of the swept terms).
        print(f'  {len(missing)} roster gyms were not found by search: '
              f'{sorted(missing)}')


if __name__ == '__main__':
    main()
