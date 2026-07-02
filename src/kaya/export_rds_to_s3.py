import argparse
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Union

import pandas as pd
from dotenv import load_dotenv

from kaya.db_manager import get_engine
from kaya.gym_config import load_gyms_config
from kaya.s3_storage import (
    DEFAULT_HISTORICAL_EXPORT_NAME,
    S3HistoricalSendExporter,
    write_recent_send_state,
)


DEFAULT_CHUNK_SIZE = 5000
DEFAULT_STATE_LIMIT = 5000


load_dotenv(override=False)


logger = logging.getLogger(__name__)


def _table_name() -> str:
    schema = os.getenv("AWS_DB_SCHEMA")
    if schema:
        return f"{schema}.sends"
    return "sends"


def _state_limit() -> int:
    value = os.getenv("KAYA_S3_STATE_MAX_SEND_IDS")
    if value is None:
        return DEFAULT_STATE_LIMIT
    return int(value)


def iter_gym_rows(
    gym_id: Union[str, int],
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    """Yield deterministic chunks of existing RDS rows for one gym."""
    engine = get_engine(use_aws=True)
    query = f"""
        SELECT *
        FROM {_table_name()}
        WHERE gym_id = %(gym_id)s
        ORDER BY date ASC, send_id ASC
    """
    return pd.read_sql_query(
        query,
        engine,
        params={"gym_id": gym_id},
        chunksize=chunk_size,
    )


def get_recent_send_ids_from_db(
    gym_id: Union[str, int],
    limit: int,
) -> List[str]:
    """Read the newest send IDs for one gym from the AWS database."""
    engine = get_engine(use_aws=True)
    query = f"""
        SELECT send_id
        FROM {_table_name()}
        WHERE gym_id = %(gym_id)s
        ORDER BY date DESC, send_id DESC
        LIMIT %(limit)s
    """
    df = pd.read_sql_query(
        query,
        engine,
        params={"gym_id": gym_id, "limit": limit},
    )
    return [str(send_id) for send_id in df["send_id"].tolist()]


def export_rds_to_s3(
    chunk_size: int = DEFAULT_CHUNK_SIZE,
    export_name: str = DEFAULT_HISTORICAL_EXPORT_NAME,
) -> Dict[str, Any]:
    """Backfill all configured gym history from RDS into S3 raw storage."""
    started_at = datetime.now(timezone.utc)
    gyms_df = load_gyms_config()
    manifest: Dict[str, Any] = {
        "schema_version": 1,
        "export_name": export_name,
        "started_at": started_at.isoformat(),
        "completed_at": None,
        "chunk_size": chunk_size,
        "gyms": [],
        "total_rows": 0,
        "total_objects": 0,
    }

    for _, row in gyms_df.iterrows():
        gym_name = row["gym_name"]
        gym_id = row["gym_id"]
        exporter = S3HistoricalSendExporter(str(gym_id), export_name=export_name)
        deleted_objects = exporter.clear_existing_batches()
        logger.info(
            "Cleared %s existing backfill objects for %s (gym_id=%s).",
            deleted_objects,
            gym_name,
            gym_id,
        )

        row_count = 0
        object_count = 0
        for batch_index, batch_df in enumerate(iter_gym_rows(gym_id, chunk_size)):
            exporter.write_batch(batch_df, batch_index=batch_index)
            row_count += len(batch_df)
            object_count += 1

        recent_send_ids = get_recent_send_ids_from_db(
            gym_id=gym_id,
            limit=_state_limit(),
        )
        write_recent_send_state(
            gym_id=str(gym_id),
            new_send_ids=recent_send_ids,
            existing_send_ids=[],
            total_written=row_count,
            run_id=f"{export_name}-seed",
            run_started_at=started_at,
        )
        logger.info(
            "Exported %s rows for %s (gym_id=%s) into %s objects.",
            row_count,
            gym_name,
            gym_id,
            object_count,
        )
        manifest["gyms"].append(
            {
                "gym_name": gym_name,
                "gym_id": str(gym_id),
                "rows": row_count,
                "objects": object_count,
                "deleted_objects": deleted_objects,
                "recent_send_ids_seeded": len(recent_send_ids),
            }
        )
        manifest["total_rows"] += row_count
        manifest["total_objects"] += object_count

    manifest["completed_at"] = datetime.now(timezone.utc).isoformat()
    if not gyms_df.empty:
        first_exporter = S3HistoricalSendExporter(
            str(gyms_df.iloc[0]["gym_id"]),
            export_name=export_name,
        )
        manifest_key = first_exporter.write_manifest(manifest)
    else:
        manifest_key = ""
    manifest["manifest_key"] = manifest_key
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Backfill existing Kaya RDS sends into S3 raw storage."
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=DEFAULT_CHUNK_SIZE,
        help="Number of RDS rows per exported object.",
    )
    parser.add_argument(
        "--export-name",
        default=DEFAULT_HISTORICAL_EXPORT_NAME,
        help="Stable backfill partition label under kaya/raw/sends/.",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    manifest = export_rds_to_s3(
        chunk_size=args.chunk_size,
        export_name=args.export_name,
    )
    logger.info(
        "Backfill complete: %s rows across %s objects.",
        manifest["total_rows"],
        manifest["total_objects"],
    )
    if manifest["manifest_key"]:
        logger.info("Manifest written to %s", manifest["manifest_key"])


if __name__ == "__main__":
    main()