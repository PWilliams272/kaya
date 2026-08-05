import argparse
import json
import os
from typing import Any, Dict

from kaya.data_access import KayaDataAccessor

DEFAULT_LOCAL_S3_BUCKET = 'my-kaya-data-545009868532-us-east-2'
DEFAULT_LOCAL_S3_PREFIX = 'kaya'
DEFAULT_LOCAL_AWS_PROFILE = 'admin'
DEFAULT_LOCAL_AWS_REGION = 'us-east-2'


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='Sync Kaya S3 raw data into the local SQLite database.'
    )
    parser.add_argument(
        '--run-date',
        action='append',
        dest='run_dates',
        help='Specific run_date partition(s) to sync, e.g. 2026-07-26.',
    )
    parser.add_argument(
        '--gym-id',
        action='append',
        dest='gym_ids',
        help='Optional gym_id filter. May be provided multiple times.',
    )
    parser.add_argument(
        '--latest-run-dates',
        type=int,
        default=None,
        help='Sync the most recent N live run_date partitions.',
    )
    parser.add_argument(
        '--max-objects',
        type=int,
        default=None,
        help='Optional cap on the number of raw S3 objects to sync.',
    )
    parser.add_argument(
        '--export-name',
        default=None,
        help='Optional backfill export source name, such as rds-backfill.',
    )
    parser.add_argument(
        '--bucket',
        default=None,
        help='Optional S3 bucket override. Defaults to the Kaya project bucket locally.',
    )
    parser.add_argument(
        '--prefix',
        default=None,
        help='Optional S3 prefix override. Defaults to kaya locally.',
    )
    parser.add_argument(
        '--aws-profile',
        default=None,
        help='Optional AWS profile override for local sync commands.',
    )
    parser.add_argument(
        '--aws-region',
        default=None,
        help='Optional AWS region override for local sync commands.',
    )
    return parser.parse_args()


def configure_sync_environment(args: argparse.Namespace) -> None:
    """Populate local S3 config defaults for sync commands when absent."""
    if not os.getenv('KAYA_S3_BUCKET'):
        os.environ['KAYA_S3_BUCKET'] = args.bucket or DEFAULT_LOCAL_S3_BUCKET
    if not os.getenv('KAYA_S3_PREFIX'):
        os.environ['KAYA_S3_PREFIX'] = args.prefix or DEFAULT_LOCAL_S3_PREFIX
    if args.aws_profile:
        os.environ['AWS_PROFILE'] = args.aws_profile
    elif not os.getenv('AWS_PROFILE'):
        os.environ['AWS_PROFILE'] = DEFAULT_LOCAL_AWS_PROFILE
    if args.aws_region:
        os.environ['AWS_REGION'] = args.aws_region
    elif not os.getenv('AWS_REGION') and not os.getenv('AWS_DEFAULT_REGION'):
        os.environ['AWS_REGION'] = DEFAULT_LOCAL_AWS_REGION


def run_sync(args: argparse.Namespace) -> Dict[str, Any]:
    configure_sync_environment(args)
    accessor = KayaDataAccessor()

    if args.latest_run_dates:
        return accessor.sync_latest_s3_to_local_db(
            latest_run_dates=args.latest_run_dates,
            gym_ids=args.gym_ids,
            max_objects=args.max_objects,
        )

    return accessor.sync_s3_sends_to_local_db(
        gym_ids=args.gym_ids,
        run_dates=args.run_dates,
        max_objects=args.max_objects,
        export_name=args.export_name,
    )


def main() -> None:
    args = parse_args()
    result = run_sync(args)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
