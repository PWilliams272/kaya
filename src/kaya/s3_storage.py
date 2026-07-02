import gzip
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import boto3
import pandas as pd
from botocore.exceptions import ClientError


DEFAULT_PREFIX = "kaya"
DEFAULT_STATE_MAX_SEND_IDS = 5000
DEFAULT_HISTORICAL_EXPORT_NAME = "rds-backfill"


def has_s3_storage_config() -> bool:
    """Return True when the S3-backed updater path is configured."""
    return bool(os.getenv("KAYA_S3_BUCKET"))


def _get_bucket() -> str:
    bucket = os.getenv("KAYA_S3_BUCKET")
    if not bucket:
        raise ValueError("KAYA_S3_BUCKET is not set.")
    return bucket


def _get_prefix() -> str:
    return os.getenv("KAYA_S3_PREFIX", DEFAULT_PREFIX).strip("/")


def _get_state_max_send_ids() -> int:
    value = os.getenv("KAYA_S3_STATE_MAX_SEND_IDS")
    if value is None:
        return DEFAULT_STATE_MAX_SEND_IDS
    return int(value)


def _get_s3_client() -> Any:
    profile = os.getenv("AWS_PROFILE")
    region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
    if profile:
        session = boto3.Session(profile_name=profile, region_name=region)
        return session.client("s3")
    if region:
        return boto3.client("s3", region_name=region)
    return boto3.client("s3")


def _state_key(gym_id: str) -> str:
    return f"{_get_prefix()}/state/gym_id={gym_id}.json"


def _merge_recent_send_ids(
    new_send_ids: List[str],
    existing_send_ids: List[str],
    max_send_ids: Optional[int] = None,
) -> List[str]:
    """Keep a newest-first deduplicated frontier of send IDs."""
    if max_send_ids is None:
        max_send_ids = _get_state_max_send_ids()
    merged_ids = list(dict.fromkeys(new_send_ids + existing_send_ids))
    return merged_ids[:max_send_ids]


def read_recent_send_ids(gym_id: str) -> List[str]:
    """Read the recent send-ID frontier for a gym from S3."""
    client = _get_s3_client()
    try:
        response = client.get_object(Bucket=_get_bucket(), Key=_state_key(gym_id))
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code")
        if error_code in {"NoSuchKey", "404"}:
            return []
        raise

    body = response["Body"].read().decode("utf-8")
    state = json.loads(body)
    return [str(send_id) for send_id in state.get("recent_send_ids", [])]


def write_recent_send_state(
    gym_id: str,
    new_send_ids: List[str],
    existing_send_ids: Optional[List[str]] = None,
    total_written: int = 0,
    run_id: Optional[str] = None,
    run_started_at: Optional[datetime] = None,
) -> Dict[str, Any]:
    """Persist the recent send-ID frontier and run metadata for a gym."""
    if existing_send_ids is None:
        existing_send_ids = []
    if run_started_at is None:
        run_started_at = datetime.now(timezone.utc)

    recent_send_ids = _merge_recent_send_ids(new_send_ids, existing_send_ids)
    state = {
        "schema_version": 1,
        "gym_id": str(gym_id),
        "run_id": run_id,
        "last_successful_run_at": run_started_at.isoformat(),
        "total_written": total_written,
        "recent_send_ids": recent_send_ids,
    }
    payload = json.dumps(state, indent=2, sort_keys=True).encode("utf-8")
    _get_s3_client().put_object(
        Bucket=_get_bucket(),
        Key=_state_key(gym_id),
        Body=payload,
        ContentType="application/json",
    )
    return state


class S3SendRunWriter:
    """Write raw send batches for one gym update run into S3."""

    def __init__(self, gym_id: str) -> None:
        self.gym_id = str(gym_id)
        self.bucket = _get_bucket()
        self.prefix = _get_prefix()
        self.client = _get_s3_client()
        self.run_started_at = datetime.now(timezone.utc)
        timestamp = self.run_started_at.strftime("%Y%m%dT%H%M%SZ")
        self.run_id = f"{timestamp}-{uuid4().hex[:8]}"

    def _batch_key(self, batch_index: int) -> str:
        run_date = self.run_started_at.strftime("%Y-%m-%d")
        return (
            f"{self.prefix}/raw/sends/run_date={run_date}/"
            f"gym_id={self.gym_id}/run_id={self.run_id}/"
            f"batch-{batch_index:05d}.jsonl.gz"
        )

    def write_batch(self, df: pd.DataFrame, batch_index: int) -> str:
        """Write one batch of sends as gzipped JSON Lines."""
        payload = df.to_json(orient="records", lines=True, date_format="iso")
        compressed_payload = gzip.compress(payload.encode("utf-8"))
        key = self._batch_key(batch_index)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compressed_payload,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )
        return key


class S3HistoricalSendExporter:
    """Export a stable historical snapshot into the raw sends area."""

    def __init__(
        self,
        gym_id: str,
        export_name: str = DEFAULT_HISTORICAL_EXPORT_NAME,
    ) -> None:
        self.gym_id = str(gym_id)
        self.export_name = export_name
        self.bucket = _get_bucket()
        self.prefix = _get_prefix()
        self.client = _get_s3_client()

    def _gym_prefix(self) -> str:
        return (
            f"{self.prefix}/raw/sends/source={self.export_name}/"
            f"gym_id={self.gym_id}/"
        )

    def batch_key(self, batch_index: int) -> str:
        return f"{self._gym_prefix()}batch-{batch_index:05d}.jsonl.gz"

    def manifest_key(self) -> str:
        return f"{self.prefix}/raw/sends/source={self.export_name}/manifest.json"

    def clear_existing_batches(self) -> int:
        """Delete previously exported objects for this gym's backfill path."""
        deleted = 0
        continuation_token: Optional[str] = None
        while True:
            request: Dict[str, Any] = {
                "Bucket": self.bucket,
                "Prefix": self._gym_prefix(),
            }
            if continuation_token is not None:
                request["ContinuationToken"] = continuation_token
            response = self.client.list_objects_v2(**request)
            contents = response.get("Contents", [])
            if contents:
                objects = [{"Key": item["Key"]} for item in contents]
                self.client.delete_objects(
                    Bucket=self.bucket,
                    Delete={"Objects": objects},
                )
                deleted += len(objects)
            if not response.get("IsTruncated"):
                break
            continuation_token = response.get("NextContinuationToken")
        return deleted

    def write_batch(self, df: pd.DataFrame, batch_index: int) -> str:
        """Write one historical batch as gzipped JSON Lines."""
        payload = df.to_json(orient="records", lines=True, date_format="iso")
        compressed_payload = gzip.compress(payload.encode("utf-8"))
        key = self.batch_key(batch_index)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=compressed_payload,
            ContentType="application/x-ndjson",
            ContentEncoding="gzip",
        )
        return key

    def write_manifest(self, manifest: Dict[str, Any]) -> str:
        """Write the export manifest for the historical backfill."""
        key = self.manifest_key()
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
            ContentType="application/json",
        )
        return key