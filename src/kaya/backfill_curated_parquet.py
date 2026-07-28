import argparse
import json
import sqlite3
from typing import List

from kaya.data_access import KayaDataAccessor


def list_local_months(accessor: KayaDataAccessor) -> List[str]:
    """Every distinct YYYY-MM present in the local sends table, ascending."""
    conn = sqlite3.connect(str(accessor.local_db_path))
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT substr(date, 1, 7) AS ym FROM sends ORDER BY ym")
        return [row[0] for row in cur.fetchall() if row[0]]
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            'One-time backfill of the curated Parquet layer '
            '(kaya/curated/sends/year=YYYY/month=MM/data.parquet) '
            'for every month already present in the local SQLite DB. '
            'The daily viewer-cache Lambda only rebuilds months touched by '
            'that day\'s sync, so it never backfills existing history on '
            'its own — this script is what actually populates it.'
        )
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='List the months that would be written without uploading anything.',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    accessor = KayaDataAccessor()
    months = list_local_months(accessor)
    print(f'Found {len(months)} months in local DB: {months[0]} .. {months[-1]}')

    if args.dry_run:
        print(json.dumps(months, indent=2))
        return

    results = []
    for year_month in months:
        result = accessor.write_curated_month_parquet(year_month)
        print(json.dumps(result))
        results.append(result)

    print(json.dumps({
        'months_written': len(results),
        'total_rows': sum(r['rows'] for r in results),
    }, indent=2))


if __name__ == '__main__':
    main()
