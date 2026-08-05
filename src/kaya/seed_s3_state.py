import logging
import os
from typing import List, Union

import pandas as pd
from dotenv import load_dotenv

from kaya.data_puller import get_engine
from kaya.gym_config import load_gyms_config
from kaya.s3_storage import write_recent_send_state

load_dotenv(override=False)


logger = logging.getLogger(__name__)


def get_recent_send_ids_from_db(
    gym_id: Union[str, int],
    limit: int,
) -> List[str]:
    """Read the newest send IDs for one gym from the AWS database."""
    engine = get_engine(use_aws=True)
    schema = os.getenv('AWS_DB_SCHEMA')
    table = 'sends'
    if schema:
        table = f"{schema}.{table}"
    query = f"""
        SELECT send_id
        FROM {table}
        WHERE gym_id = %(gym_id)s
        ORDER BY date DESC, send_id DESC
        LIMIT %(limit)s
    """
    df = pd.read_sql_query(
        query,
        engine,
        params={"gym_id": gym_id, "limit": limit}
    )
    return [str(send_id) for send_id in df['send_id'].tolist()]


def seed_recent_send_state(
    recent_send_limit: int = 5000,
) -> None:
    """Seed S3 incremental state for each configured gym from RDS."""
    gyms_df = load_gyms_config()
    for _, row in gyms_df.iterrows():
        gym_name = row['gym_name']
        gym_id = row['gym_id']
        recent_send_ids = get_recent_send_ids_from_db(
            gym_id=gym_id,
            limit=recent_send_limit,
        )
        write_recent_send_state(
            gym_id=str(gym_id),
            new_send_ids=recent_send_ids,
            existing_send_ids=[],
            total_written=0,
            run_id='seed-from-rds',
        )
        logger.info(
            "Seeded %s recent send IDs for %s (gym_id=%s).",
            len(recent_send_ids),
            gym_name,
            gym_id,
        )


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    seed_recent_send_state()
