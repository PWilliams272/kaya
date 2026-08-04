"""Push the already-backfilled 2026-02-21 to 2026-07-24 gap rows (see
kaya_data_gap_lambda_outage memory / backfill_gap_2026.py) from the local
SQLite DB into S3 raw storage, so the durable S3 history is complete too --
not just the local dev DB.

Mirrors export_rds_to_s3.py's pattern (S3HistoricalSendExporter under
kaya/raw/sends/source=<export_name>/gym_id=<id>/batch-NNNNN.jsonl.gz), but
reads from the local DB (which already has the full gap backfilled) instead
of RDS, and only touches gap-window rows rather than a full-history export.
Uses a distinct export_name ('gap-2026-backfill') so it's clearly
identifiable and doesn't collide with the existing 'rds-backfill' migration
objects.

Run directly from a terminal:
    .venv/bin/python -m kaya.backfill_gap_2026_to_s3

Requires AWS credentials (the 'admin' profile, matching sync_local_data.py's
defaults) with S3 write access to the Kaya bucket. Does not touch RDS --
per DATA_STORAGE_NOTES.md, RDS is legacy/frozen and new data should not be
written there.
"""
import argparse
import logging
import os
from datetime import date, datetime, timezone
from typing import Any, Dict

import pandas as pd
from dotenv import load_dotenv

from kaya.db_manager import get_engine
from kaya.gym_config import load_gyms_config
from kaya.s3_storage import S3HistoricalSendExporter

DEFAULT_LOCAL_S3_BUCKET = 'my-kaya-data-545009868532-us-east-2'
DEFAULT_LOCAL_S3_PREFIX = 'kaya'
DEFAULT_LOCAL_AWS_PROFILE = 'admin'
DEFAULT_LOCAL_AWS_REGION = 'us-east-2'

GAP_START = date(2026, 2, 21)
GAP_END = date(2026, 7, 24)
EXPORT_NAME = 'gap-2026-backfill'
CHUNK_SIZE = 5000

load_dotenv(override=False)

logger = logging.getLogger(__name__)


def _apply_local_s3_defaults() -> None:
    if not os.getenv('KAYA_S3_BUCKET'):
        os.environ['KAYA_S3_BUCKET'] = DEFAULT_LOCAL_S3_BUCKET
    if not os.getenv('KAYA_S3_PREFIX'):
        os.environ['KAYA_S3_PREFIX'] = DEFAULT_LOCAL_S3_PREFIX
    if not os.getenv('AWS_PROFILE'):
        os.environ['AWS_PROFILE'] = DEFAULT_LOCAL_AWS_PROFILE
    if not os.getenv('AWS_REGION') and not os.getenv('AWS_DEFAULT_REGION'):
        os.environ['AWS_REGION'] = DEFAULT_LOCAL_AWS_REGION


def iter_gym_gap_rows(gym_id: str, chunk_size: int):
    engine = get_engine(use_aws=False)
    query = """
        SELECT * FROM sends
        WHERE gym_id = :gym_id AND date >= :gap_start AND date <= :gap_end
        ORDER BY date ASC, send_id ASC
    """
    return pd.read_sql_query(
        query,
        engine,
        params={
            'gym_id': gym_id,
            'gap_start': GAP_START.isoformat(),
            'gap_end': GAP_END.isoformat(),
        },
        chunksize=chunk_size,
    )


def export_gap_to_s3(export_name: str = EXPORT_NAME, chunk_size: int = CHUNK_SIZE) -> Dict[str, Any]:
    started_at = datetime.now(timezone.utc)
    gyms_df = load_gyms_config()
    manifest: Dict[str, Any] = {
        'schema_version': 1,
        'export_name': export_name,
        'gap_start': GAP_START.isoformat(),
        'gap_end': GAP_END.isoformat(),
        'started_at': started_at.isoformat(),
        'completed_at': None,
        'chunk_size': chunk_size,
        'gyms': [],
        'total_rows': 0,
        'total_objects': 0,
    }

    for _, row in gyms_df.iterrows():
        gym_name = row['gym_name']
        gym_id = str(row['gym_id'])
        exporter = S3HistoricalSendExporter(gym_id, export_name=export_name)
        deleted_objects = exporter.clear_existing_batches()

        row_count = 0
        object_count = 0
        for batch_index, batch_df in enumerate(iter_gym_gap_rows(gym_id, chunk_size)):
            if batch_df.empty:
                continue
            exporter.write_batch(batch_df, batch_index=batch_index)
            row_count += len(batch_df)
            object_count += 1

        logger.info(
            'Exported %s gap rows for %s (gym_id=%s) into %s objects (deleted %s stale objects).',
            row_count, gym_name, gym_id, object_count, deleted_objects,
        )
        manifest['gyms'].append({
            'gym_name': gym_name,
            'gym_id': gym_id,
            'rows': row_count,
            'objects': object_count,
            'deleted_objects': deleted_objects,
        })
        manifest['total_rows'] += row_count
        manifest['total_objects'] += object_count

    manifest['completed_at'] = datetime.now(timezone.utc).isoformat()
    if not gyms_df.empty:
        first_exporter = S3HistoricalSendExporter(
            str(gyms_df.iloc[0]['gym_id']), export_name=export_name,
        )
        manifest_key = first_exporter.write_manifest(manifest)
    else:
        manifest_key = ''
    manifest['manifest_key'] = manifest_key
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description='Push the locally-backfilled 2026 gap rows into S3 raw storage.'
    )
    parser.add_argument('--export-name', default=EXPORT_NAME)
    parser.add_argument('--chunk-size', type=int, default=CHUNK_SIZE)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    _apply_local_s3_defaults()

    manifest = export_gap_to_s3(export_name=args.export_name, chunk_size=args.chunk_size)
    logger.info(
        'Done: %s rows across %s objects for %s gyms.',
        manifest['total_rows'], manifest['total_objects'], len(manifest['gyms']),
    )
    if manifest['manifest_key']:
        logger.info('Manifest written to %s', manifest['manifest_key'])


if __name__ == '__main__':
    main()
