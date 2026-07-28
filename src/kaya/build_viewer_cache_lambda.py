import logging
from typing import Any, Dict

from botocore.exceptions import ClientError

from kaya.data_access import KayaDataAccessor
from kaya.s3_storage import get_s3_bucket, get_s3_client, get_s3_prefix
from kaya.viewer_payloads import ViewerPayloadBuilder

# Set up logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

MATERIALIZED_DB_S3_SUBPREFIX = 'materialized'
MATERIALIZED_DB_FILENAME = 'kaya_data.db'

# /tmp is not reliably persistent between Lambda invocations (a once-daily
# schedule will hit a cold, empty /tmp most of the time), so the full-history
# local SQLite baseline lives in S3 and is downloaded fresh each run instead
# of relying on warm-container reuse. A small buffer beyond "1 day" of
# run_date partitions guards against a missed or delayed trigger.
LATEST_RUN_DATES_TO_SYNC = 3


def _materialized_db_key() -> str:
    return f'{get_s3_prefix()}/{MATERIALIZED_DB_S3_SUBPREFIX}/{MATERIALIZED_DB_FILENAME}'


def _download_materialized_db(accessor: KayaDataAccessor) -> bool:
    """Fetch the persisted full-history SQLite snapshot into /tmp.

    Returns False (leaving no local file) on the very first-ever run, before
    any snapshot has been uploaded — sync_latest_s3_to_local_db will then
    only cover LATEST_RUN_DATES_TO_SYNC days, which is a real gap. The deploy
    runbook seeds this key from a known-complete local DB before the
    schedule is ever turned on, so this should only trigger in practice if
    that snapshot object is ever lost.
    """
    client = get_s3_client()
    bucket = get_s3_bucket()
    key = _materialized_db_key()
    accessor.local_db_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        client.download_file(bucket, key, str(accessor.local_db_path))
    except ClientError as exc:
        error_code = exc.response.get('Error', {}).get('Code')
        if error_code in {'404', 'NoSuchKey'}:
            logger.warning(
                'No materialized DB snapshot found at s3://%s/%s — '
                'starting from an empty local DB.', bucket, key,
            )
            return False
        raise
    return True


def _upload_materialized_db(accessor: KayaDataAccessor) -> None:
    client = get_s3_client()
    bucket = get_s3_bucket()
    key = _materialized_db_key()
    client.upload_file(str(accessor.local_db_path), bucket, key)


def lambda_handler(event, context) -> Dict[str, Any]:
    accessor = KayaDataAccessor()

    had_baseline = _download_materialized_db(accessor)

    sync_result = accessor.sync_latest_s3_to_local_db(
        latest_run_dates=LATEST_RUN_DATES_TO_SYNC,
    )
    logger.info(
        'Synced %s rows across run_dates=%s, months_touched=%s',
        sync_result.get('rows_written'), sync_result.get('run_dates'),
        sync_result.get('months_touched'),
    )

    # Curated Parquet layer (DATA_STORAGE_NOTES.md's outstanding "define and
    # build the curated parquet layer" task): only the month partitions that
    # actually received new/changed rows get rebuilt, not the full history —
    # unlike the materialized SQLite snapshot below, whose full-file re-upload
    # cost is deliberately left as-is for now (see VIEWER_CACHE_RUNBOOK.md).
    curated_results = [
        accessor.write_curated_month_parquet(year_month)
        for year_month in sync_result.get('months_touched', [])
    ]
    logger.info('Rebuilt curated Parquet partitions: %s', curated_results)

    builder = ViewerPayloadBuilder(accessor=accessor)
    write_result = builder.write_static_artifacts()
    upload_result = builder.upload_static_artifacts_to_s3()

    _upload_materialized_db(accessor)

    return {
        'had_baseline': had_baseline,
        'sync': sync_result,
        'curated_parquet': curated_results,
        'artifacts_written': write_result,
        'artifacts_uploaded': {
            'bucket': upload_result['bucket'],
            'prefix': upload_result['prefix'],
            'files_uploaded': upload_result['files_uploaded'],
        },
    }
