import json
import os
import pandas as pd
import logging
from typing import Any, Dict, List, Optional, Sequence

import boto3

from kaya.data_puller import update_gym_data
from kaya.gym_config import load_gyms_config

# Set up logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def _get_target_gyms(
    gym_ids: Optional[Sequence[Any]] = None
) -> pd.DataFrame:
    """Load configured gyms and optionally filter to a selected subset."""
    gyms_df = load_gyms_config()
    if not gym_ids:
        return gyms_df

    requested_gym_ids = {str(gym_id) for gym_id in gym_ids}
    filtered_gyms = gyms_df[
        gyms_df['gym_id'].astype(str).isin(requested_gym_ids)
    ].copy()
    found_gym_ids = set(filtered_gyms['gym_id'].astype(str))
    missing_gym_ids = requested_gym_ids - found_gym_ids
    if missing_gym_ids:
        raise ValueError(
            "Unknown gym_id values requested: "
            f"{sorted(missing_gym_ids)}"
        )
    return filtered_gyms


def _update_gyms_dataframe(
    gyms_df: pd.DataFrame,
    mode: str,
    use_aws: bool,
    storage_backend: str,
    batch_size: int,
    max_offset_retries: int,
    log_level: int,
) -> Dict[str, str]:
    """Run the updater for the provided gym subset."""
    results = {}
    for _, row in gyms_df.iterrows():
        gym_name = row['gym_name']
        gym_id = row['gym_id']
        logger.info(f"Updating gym: {gym_name} (id={gym_id})")
        try:
            update_gym_data(
                gym_id,
                mode=mode,
                use_aws=use_aws,
                storage_backend=storage_backend,
                batch_size=batch_size,
                max_offset_retries=max_offset_retries,
                log_level=log_level
            )
            results[gym_name] = "Success"
        except Exception as e:
            logger.error(f"Failed to update {gym_name} (id={gym_id}): {e}")
            results[gym_name] = f"Error: {e}"
    return results


def _get_sqs_client() -> Any:
    """Create an SQS client for local or Lambda execution."""
    region = os.getenv('AWS_REGION') or os.getenv('AWS_DEFAULT_REGION')
    if region:
        return boto3.client('sqs', region_name=region)
    return boto3.client('sqs')


def _get_queue_url(event: Dict[str, Any]) -> str:
    """Resolve the queue URL from the event or environment."""
    queue_url = event.get('queue_url') or os.getenv('KAYA_SQS_QUEUE_URL')
    if not queue_url:
        raise ValueError(
            "KAYA_SQS_QUEUE_URL must be set for dispatch mode."
        )
    return str(queue_url)


def _get_dlq_url() -> Optional[str]:
    """Resolve the optional DLQ URL for failed gym jobs."""
    dlq_url = os.getenv('KAYA_SQS_DLQ_URL')
    if not dlq_url:
        return None
    return str(dlq_url)


def _send_failed_job_to_dlq(
    record: Dict[str, Any],
    error: Exception,
) -> None:
    """Persist a terminally failed gym job to the configured DLQ."""
    dlq_url = _get_dlq_url()
    if not dlq_url:
        logger.warning(
            "No KAYA_SQS_DLQ_URL configured; dropping failed gym job %s "
            "after terminal worker failure.",
            record.get('messageId'),
        )
        return

    try:
        payload = json.loads(record['body'])
    except json.JSONDecodeError:
        payload = {'raw_body': record.get('body')}
    payload['failure'] = {
        'message_id': record.get('messageId'),
        'error': str(error),
    }
    _get_sqs_client().send_message(
        QueueUrl=dlq_url,
        MessageBody=json.dumps(payload),
    )


def dispatch_gym_updates(
    queue_url: str,
    mode: str = 'incremental',
    storage_backend: str = 'auto',
    batch_size: int = 1000,
    max_offset_retries: int = 3,
    log_level: int = logging.INFO,
    gym_ids: Optional[Sequence[Any]] = None,
) -> Dict[str, Any]:
    """Enqueue one update job per gym for SQS-backed fanout execution."""
    gyms_df = _get_target_gyms(gym_ids=gym_ids)
    sqs_client = _get_sqs_client()
    dispatched_count = 0

    for batch_start in range(0, len(gyms_df), 10):
        message_entries = []
        chunk = gyms_df.iloc[batch_start:batch_start + 10]
        for offset, (_, row) in enumerate(chunk.iterrows()):
            message_entries.append(
                {
                    'Id': str(batch_start + offset),
                    'MessageBody': json.dumps(
                        {
                            'gym_id': row['gym_id'],
                            'gym_name': row['gym_name'],
                            'mode': mode,
                            'storage_backend': storage_backend,
                            'batch_size': batch_size,
                            'max_offset_retries': max_offset_retries,
                            'log_level': log_level,
                        }
                    ),
                }
            )

        response = sqs_client.send_message_batch(
            QueueUrl=queue_url,
            Entries=message_entries,
        )
        failed_messages = response.get('Failed', [])
        if failed_messages:
            raise RuntimeError(
                f"Failed to enqueue {len(failed_messages)} gym updates: "
                f"{failed_messages}"
            )
        dispatched_count += len(response.get('Successful', []))

    logger.info(
        "Dispatched %s gym update jobs to %s.",
        dispatched_count,
        queue_url,
    )
    return {
        'queue_url': queue_url,
        'dispatched_count': dispatched_count,
        'mode': mode,
        'storage_backend': storage_backend,
    }


def process_sqs_records(
    records: Sequence[Dict[str, Any]]
) -> Dict[str, Any]:
    """Process one or more SQS gym-update jobs."""
    for record in records:
        message_id = record['messageId']
        try:
            payload = json.loads(record['body'])
            gym_id = payload['gym_id']
            gym_name = payload.get('gym_name', str(gym_id))
            logger.info(
                "Processing queued gym update for %s (id=%s).",
                gym_name,
                gym_id,
            )
            update_gym_data(
                gym_id,
                mode=payload.get('mode', 'incremental'),
                use_aws=True,
                storage_backend=payload.get('storage_backend', 'auto'),
                batch_size=int(payload.get('batch_size', 1000)),
                max_offset_retries=int(
                    payload.get('max_offset_retries', 3)
                ),
                log_level=int(payload.get('log_level', logging.INFO)),
            )
        except Exception as exc:
            logger.error(
                "Failed queued gym update for message %s: %s",
                message_id,
                exc,
            )
            _send_failed_job_to_dlq(record, exc)

    return {'batchItemFailures': []}


def update_all_gyms(
    mode: str = 'incremental',
    use_aws: bool = True,
    storage_backend: str = 'auto',
    batch_size: int = 1000,
    max_offset_retries: int = 3,
    gym_ids: Optional[Sequence[Any]] = None,
    log_level: int = logging.INFO
) -> Dict[str, str]:
    """Update all gyms listed in the configuration file.

    Args:
        mode (str, optional): Update mode ('incremental' or 'full'). Defaults
            to 'incremental'.
        use_aws (bool, optional): Whether to use AWS database. Defaults to
            True.
        storage_backend (str, optional): Storage backend to use. One of
            'auto', 'db', or 's3'. Defaults to 'auto'.
        batch_size (int, optional): Number of records to write per batch.
            Defaults to 1000.
        max_offset_retries (int, optional): Maximum retries for one Kaya API
            page offset before failing that gym's update. Defaults to 3.
        gym_ids (Optional[Sequence[Any]], optional): Optional subset of gym IDs
            to process. Defaults to None, which processes all configured gyms.
        log_level (int, optional): Logging level. Defaults to logging.INFO.

    Returns:
        Dict[str, str]: Dictionary mapping gym names to update status.
    """
    gyms_df = _get_target_gyms(gym_ids=gym_ids)
    return _update_gyms_dataframe(
        gyms_df,
        mode=mode,
        use_aws=use_aws,
        storage_backend=storage_backend,
        batch_size=batch_size,
        max_offset_retries=max_offset_retries,
        log_level=log_level,
    )


def lambda_handler(
    event: Dict[str, Any],
    context: Any
) -> Dict[str, Any]:
    """AWS Lambda entrypoint.

    Optionally, can pass 'mode', 'batch_size', etc. in the event dict.

    Args:
        event (Dict[str, Any]): Lambda event dict. Can include 'mode',
            'batch_size', 'log_level'.
        context (Any): Lambda context object (unused).

    Returns:
        Dict[str, Any]: Update or dispatch summary.
    """
    if 'Records' in event:
        return process_sqs_records(event['Records'])

    mode = event.get('mode', 'incremental')
    batch_size = event.get('batch_size', 1000)
    max_offset_retries = event.get('max_offset_retries', 3)
    log_level = event.get('log_level', logging.INFO)
    storage_backend = event.get('storage_backend', 'auto')
    gym_ids = event.get('gym_ids')

    if event.get('dispatch', False):
        return dispatch_gym_updates(
            queue_url=_get_queue_url(event),
            mode=mode,
            storage_backend=storage_backend,
            batch_size=batch_size,
            max_offset_retries=max_offset_retries,
            log_level=log_level,
            gym_ids=gym_ids,
        )

    if event.get('gym_id') is not None and gym_ids is None:
        gym_ids = [event['gym_id']]

    return update_all_gyms(
        mode=mode,
        use_aws=True,
        storage_backend=storage_backend,
        batch_size=batch_size,
        max_offset_retries=max_offset_retries,
        gym_ids=gym_ids,
        log_level=log_level
    )


if __name__ == '__main__':
    # For local testing
    results = update_all_gyms(
        mode='incremental',
        use_aws=True,
        storage_backend='auto',
        batch_size=1000,
        max_offset_retries=3,
        log_level=logging.INFO
    )
    print(results)
