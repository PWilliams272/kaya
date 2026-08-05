import logging
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union

import pandas as pd
import requests

from kaya.db_manager import get_engine, write_dataframe
from kaya.s3_storage import (
    S3SendRunWriter,
    has_s3_storage_config,
    read_recent_send_ids,
    write_recent_send_state,
)
from kaya.secrets import load_secrets, write_secrets

USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/135.0.0.0 Safari/537.36"
)

HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://kaya-app.kayaclimb.com',
    'priority': 'u=1, i',
    'referer': 'https://kaya-app.kayaclimb.com/',
    'user-agent': USER_AGENT,
}

# Set up logger for this module
logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def _resolve_storage_backend(
    storage_backend: str,
    use_aws: bool
) -> str:
    """Resolve the send storage backend for the current run."""
    if storage_backend not in {'auto', 'db', 's3'}:
        raise ValueError(
            "storage_backend must be one of 'auto', 'db', or 's3'."
        )
    if storage_backend == 'auto':
        if use_aws and has_s3_storage_config():
            return 's3'
        return 'db'
    if storage_backend == 's3' and not has_s3_storage_config():
        raise ValueError(
            "KAYA_S3_BUCKET must be set when storage_backend='s3'."
        )
    return storage_backend


def _prepare_batch_dataframe(
    batch_df: pd.DataFrame
) -> pd.DataFrame:
    """Normalize batch dtypes before persistence."""
    batch_df = batch_df.copy()
    if 'is_private' in batch_df.columns:
        batch_df['is_private'] = batch_df['is_private'].fillna(0).astype(int)
    if 'is_premium' in batch_df.columns:
        batch_df['is_premium'] = batch_df['is_premium'].fillna(0).astype(int)
    if 'stiffness' in batch_df.columns:
        batch_df['stiffness'] = batch_df['stiffness'].astype(int)
    if 'ascent_count' in batch_df.columns:
        batch_df['ascent_count'] = batch_df['ascent_count'].astype(int)
    return batch_df


def _write_batch(
    batch_df: pd.DataFrame,
    gym_id: Union[str, int],
    use_aws: bool,
    storage_backend: str,
    s3_writer: Optional[S3SendRunWriter],
    batch_index: int,
) -> None:
    """Persist a batch to the selected storage backend."""
    if storage_backend == 's3':
        if s3_writer is None:
            raise ValueError("S3 writer is required for S3 storage backend.")
        key = s3_writer.write_batch(batch_df, batch_index=batch_index)
        logger.info(
            "Wrote %s rows for gym_id=%s to s3://%s/%s",
            len(batch_df),
            gym_id,
            s3_writer.bucket,
            key,
        )
        return

    write_dataframe(
        batch_df,
        'sends',
        use_aws=use_aws,
        if_exists='upsert'
    )


def update_tokens(
    force_aws: bool = False
) -> Tuple[str, str]:
    """Refresh API tokens

    Refreshes the Kaya API access and refresh tokens using the current
    environment variables. Updates os.environ for immediate use, and
    updates the .env file if running locally, or AWS Secrets Manager if
    running in AWS Lambda or force_aws is True. Returns the new access and
    refresh tokens.

    Args:
        force_aws (bool, optional): If True, force update secrets in AWS.
            Defaults to False.

    Returns:
        Tuple[str, str]: The new access token and refresh token.
    """
    load_secrets(force_aws=force_aws)

    json_data = {'refresh_token': os.getenv("KAYA_REFRESH_TOKEN")}
    resp = kaya_api_post(
        'https://kaya-beta.kayaclimb.com/api/user/refresh-token',
        json_data=json_data,
        max_retries=0
    )
    resp.raise_for_status()
    tokens = resp.json()
    new_access_token = tokens.get('token')
    new_refresh_token = tokens.get(
        'refresh_token',
        os.getenv("KAYA_REFRESH_TOKEN")
    )

    # Update environment and persist secrets
    write_secrets(new_access_token, new_refresh_token, force_aws=force_aws)
    return new_access_token, new_refresh_token


def kaya_api_post(
    url: str,
    json_data: Dict,
    max_retries: int = 1,
    **kwargs: Any
) -> requests.Response:
    """Helper for POST requests to Kaya.
    Helper for POST requests to Kaya. If a 401 error is encountered, refresh
    tokens and retry.

    Args:
        url (str): The URL to send the POST request to.
        json_data (Dict): The JSON data to send in the request body.
        max_retries (int, optional): Number of retries on 401 errors.
            Defaults to 1.
        **kwargs: Additional arguments to pass to requests.post.

    Returns:
        requests.Response: The response object from the POST request.

    Raises:
        Exception: If the request fails after retries.
    """
    KAYA_API_TOKEN = os.getenv("KAYA_API_TOKEN")
    HEADERS['authorization'] = f'Bearer {KAYA_API_TOKEN}'
    for attempt in range(max_retries + 1):
        try:
            response = requests.post(
                url,
                headers=HEADERS,
                json=json_data,
                **kwargs
            )
            if response.status_code == 401:
                logger.warning(
                    "401 Unauthorized. Attempting token refresh and retry."
                )
                update_tokens()
                # Update headers with new token
                HEADERS['authorization'] = (
                    f"Bearer {os.getenv('KAYA_API_TOKEN')}"
                )
                continue
            response.raise_for_status()
            return response
        except requests.HTTPError as e:
            if response.status_code == 401 and attempt < max_retries:
                logger.warning(
                    "401 Unauthorized. Attempting token refresh and retry."
                )
                update_tokens()
                HEADERS['authorization'] = (
                    f"Bearer {os.getenv('KAYA_API_TOKEN')}"
                )
                continue
            else:
                logger.error(f"HTTP error: {e}")
                raise
        except Exception as e:
            logger.error(f"Request failed: {e}")
            raise
    raise Exception("Failed POST request after token refresh attempts.")


def search_for_gym(
    search_term: str,
    offset: int = 0,
    count: int = 100
) -> pd.DataFrame:
    """Search for a gym using a search term.

    Args:
        search_term (str): The search term to look for gyms.
        offset (int, optional): Result offset, for paging past the first
            page. Defaults to 0.
        count (int, optional): Results per page. Defaults to 100.

    Returns:
        pd.DataFrame: DataFrame containing gym search results. Columns are
            id, slug, name, boulder_count, route_count, address, city,
            postal_code, region, country, follower_count, is_official,
            website.
    """
    json_data = {
        'operationName': 'webSearchForGym',
        'variables': {
            'term': search_term,
            'offset': offset,
            'count': count,
        },
        'query': (
            'query webSearchForGym($term: String!, $offset: Int!, '
            '$count: Int!) {\n  webSearchForGym(term: $term, offset: '
            '$offset, count: $count) {\n    ...WebGymFields\n    '
            '__typename\n  }\n}\n\nfragment WebGymFields on WebGym {\n  '
            'id\n  slug\n  name\n  boulder_count\n  route_count\n  '
            'address\n  city\n  postal_code\n  region\n  country\n  '
            'follower_count\n  is_official\n  website\n  __typename\n}\n'
        ),
    }

    response = kaya_api_post(
        'https://kaya-beta.kayaclimb.com/graphql',
        json_data=json_data
    )
    data = response.json()
    return pd.DataFrame(data['data']['webSearchForGym'])


def get_data_for_gym(
    gym_id: Union[str, int],
    offset: int = 0
) -> pd.DataFrame:
    """Retrieve data for a specific gym.

    Args:
        gym_id (Union[str, int]): The gym ID to fetch data for.
        offset (int, optional): Offset for pagination. Defaults to 0.

    Returns:
        pd.DataFrame: DataFrame containing ascent data for the gym.
    """
    query = '''
        query webAscentsForGym($gym_id: ID!, $count: Int!, $offset: Int!) {
            webAscentsForGym(gym_id: $gym_id, count: $count, offset: $offset) {
            ...WebAscentFields
            __typename
            }
        }

        fragment WebAscentFields on WebAscent {
            id
            user {
            ...WebUserFields
            __typename
            }
            climb {
            ...WebClimbBasicFields
            __typename
            }
            date
            comment
            rating
            stiffness
            grade {
            ...GradeFields
            __typename
            }
            photo {
            photo_url
            thumb_url
            __typename
            }
            video {
            video_url
            thumb_url
            __typename
            }
            __typename
        }

        fragment WebUserFields on WebUser {
            id
            username
            fname
            lname
            photo_url
            is_private
            bio
            height
            ape_index
            limit_grade_bouldering {
            name
            id
            __typename
            }
            limit_grade_routes {
            name
            id
            __typename
            }
            is_premium
            __typename
        }

        fragment WebClimbBasicFields on WebClimb {
            slug
            name
            rating
            ascent_count
            grade {
            name
            id
            __typename
            }
            climb_type {
            name
            __typename
            }
            color {
            name
            __typename
            }
            gym {
            name
            __typename
            }
            board {
            name
            __typename
            }
            destination {
            name
            __typename
            }
            area {
            name
            __typename
            }
            is_gb_moderated
            is_access_sensitive
            is_closed
            __typename
        }

        fragment GradeFields on Grade {
            id
            name
            climb_type_id
            grade_type_id
            ordering
            mapped_grade_ids
            climb_type_group
            __typename
        }
    '''
    json_data = {
        'operationName': 'webAscentsForGym',
        'variables': {
            'gym_id': str(gym_id),
            'offset': offset,
            'count': 15,
        },
        'query': query
    }

    response = kaya_api_post(
        'https://kaya-beta.kayaclimb.com/graphql',
        json_data=json_data
    )
    if 'errors' in response.json():
        raise Exception(
            f"Error fetching data for gym {gym_id}: "
            f"{response.json()['errors']}"
        )
    data = response.json()
    if not data['data']['webAscentsForGym']:
        return pd.DataFrame()  # Return empty DataFrame if no data

    df = pd.DataFrame(data['data']['webAscentsForGym'])
    for col in [
        'id',
        'username',
        'fname',
        'lname',
        'photo_url',
        'is_private',
        'bio',
        'height',
        'ape_index',
        'limit_grade_bouldering',
        'limit_grade_routes',
        'is_premium'
    ]:
        df[f'user_{col}'] = df['user'].apply(lambda x: x.get(col))
    for col in [
        'slug',
        'name',
        'rating',
        'ascent_count',
        'grade',
        'climb_type',
        'color',
        'gym',
        'board',
        'destination',
        'area',
        'is_gb_moderated',
        'is_access_sensitive',
        'is_closed'
    ]:
        df[f'climb_{col}'] = df['climb'].apply(lambda x: x.get(col))
    df['limit_grade_bouldering'] = df['user_limit_grade_bouldering'].apply(
        lambda x: x.get('name') if isinstance(x, dict) else None
    )
    df['limit_grade_routes'] = df['user_limit_grade_routes'].apply(
        lambda x: x.get('name') if isinstance(x, dict) else None
    )
    df['grade'] = df['grade'].apply(
        lambda x: x.get('name') if isinstance(x, dict) else None
    )

    for col in ['limit_grade_bouldering', 'limit_grade_routes']:
        df[f'user_{col}'] = df[f'user_{col}'].apply(
            lambda x: x.get('name') if isinstance(x, dict) else None
        )
    for col in ['grade', 'climb_type', 'color', 'gym']:
        df[f'climb_{col}'] = df[f'climb_{col}'].apply(
            lambda x: x.get('name') if isinstance(x, dict) else None
        )

    df['climb_id'] = df['climb_slug'].apply(
        lambda x: x.split('-')[-1] if isinstance(x, str) else None
    )
    df['gym_id'] = gym_id
    rename_dict = {
        'id': 'send_id',
        'date': 'date',
        'gym_id': 'gym_id',
        'climb_gym': 'gym',
        'climb_climb_type': 'climb_type',
        'grade': 'grade',
        'stiffness': 'stiffness',
        'user_id': 'user_id',
        'user_username': 'username',
        'user_fname': 'first_name',
        'user_lname': 'last_name',
        'user_height': 'height',
        'user_ape_index': 'ape_index',
        'user_photo_url': 'photo_url',
        'user_is_private': 'is_private',
        'user_bio': 'bio',
        'user_limit_grade_bouldering': 'limit_grade_bouldering',
        'user_limit_grade_routes': 'limit_grade_routes',
        'user_is_premium': 'is_premium',
        'climb_id': 'climb_id',
        'climb_name': 'climb_name',
        'climb_ascent_count': 'ascent_count',
        'climb_color': 'color',
        'comment': 'comment',
        'rating': 'rating',
    }
    df = df[rename_dict.keys()].rename(columns=rename_dict)
    return df


def get_existing_send_ids(
    gym_id: Union[str, int],
    use_aws: bool = False,
    storage_backend: str = 'auto'
) -> List[Any]:
    """Get a list of existing send IDs for a gym from the database.

    Args:
        gym_id (Union[str, int]): The gym ID to query.
        use_aws (bool, optional): Whether to use AWS database. Defaults to
            False.

    Returns:
        List[Any]: List of send IDs.
    """
    resolved_storage_backend = _resolve_storage_backend(
        storage_backend,
        use_aws=use_aws
    )
    if resolved_storage_backend == 's3':
        return read_recent_send_ids(str(gym_id))

    engine = get_engine(use_aws=use_aws)
    schema = os.getenv('AWS_DB_SCHEMA') if use_aws else None
    table = 'sends'
    if schema:
        table = f"{schema}.{table}"
    placeholder = '%s' if use_aws else '?'
    query = (
        f"SELECT DISTINCT send_id FROM {table} WHERE gym_id = {placeholder}"
    )
    return pd.read_sql_query(
        query,
        engine,
        params=(gym_id,)
    )['send_id'].tolist()


def update_gym_data(
    gym_id: Union[str, int],
    mode: str = 'incremental',
    use_aws: bool = False,
    storage_backend: str = 'auto',
    batch_size: int = 1000,
    start_offset: int = 0,
    max_offset_retries: int = 3,
    log_level: Optional[int] = None
) -> Optional[pd.DataFrame]:
    """Pull data for a gym and write to the database in batches.

    Args:
        gym_id (Union[str, int]): The gym ID to update data for.
        mode (str, optional): 'full' for initial pull (all data), 'incremental'
            for daily updates (stop if encounters send_id that exists).
            Defaults to 'incremental'.
        use_aws (bool, optional): Whether to use AWS database. Defaults to
            False.
        storage_backend (str, optional): Storage backend to use for writes and
            incremental state. One of 'auto', 'db', or 's3'. Defaults to
            'auto'.
        batch_size (int, optional): Number of records to write per batch.
            Defaults to 1000.
        start_offset (int, optional): Starting offset for data pull. Defaults
            to 0.
        max_offset_retries (int, optional): Maximum retries for the same API
            offset before failing the current gym update. Defaults to 3.
        log_level (Optional[int], optional): Logging level. Defaults to None.

    Returns:
        Optional[pd.DataFrame]: The final batch DataFrame if any data was
            written, otherwise None.
    """
    if log_level is not None:
        logger.setLevel(log_level)
        if not logger.hasHandlers():
            handler = logging.StreamHandler()
            formatter = logging.Formatter('[%(levelname)s] %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
    storage_backend = _resolve_storage_backend(
        storage_backend,
        use_aws=use_aws
    )
    offset = start_offset
    all_data = []
    total_written = 0
    batch_index = 0
    fetched_send_ids: List[str] = []
    existing_recent_send_ids: List[str] = []
    s3_writer: Optional[S3SendRunWriter] = None
    offset_retry_count = 0

    if storage_backend == 's3':
        s3_writer = S3SendRunWriter(str(gym_id))
        logger.info(
            "Using S3 storage backend for gym_id=%s into bucket '%s'.",
            gym_id,
            s3_writer.bucket,
        )

    if mode == 'incremental':
        logger.debug(
            f"Reading existing send_ids from table 'sends' (use_aws={use_aws})"
        )
        existing_recent_send_ids = [
            str(send_id) for send_id in get_existing_send_ids(
                gym_id,
                use_aws=use_aws,
                storage_backend=storage_backend
            )
        ]
        seen_send_ids = set(existing_recent_send_ids)
    else:
        seen_send_ids = set()

    iteration = 0
    progress_bar_length = 30  # Number of segments in the bar
    while True:
        # Progress bar: update every 10 iterations
        if iteration % 10 == 0 and iteration > 0:
            num_segments = min(iteration // 10, progress_bar_length)
            bar = '[' + '#' * num_segments
            bar += '-' * (progress_bar_length - num_segments) + ']'
            print(f'\rProgress: {bar} {iteration} batches', end='', flush=True)
        else:
            print(
                f'\rPulling batch {iteration} (offset={offset})...', end='',
                flush=True
            )
        logger.debug(f"Fetching data for gym_id={gym_id} at offset={offset}")
        try:
            df = get_data_for_gym(gym_id, offset=offset)
            offset_retry_count = 0
        except Exception as e:
            offset_retry_count += 1
            logger.debug(
                "Error at offset %s for gym_id=%s (attempt %s/%s): %s",
                offset,
                gym_id,
                offset_retry_count,
                max_offset_retries,
                e,
            )
            logger.warning(
                "Error at offset %s for gym_id=%s (attempt %s/%s): %s",
                offset,
                gym_id,
                offset_retry_count,
                max_offset_retries,
                e,
            )
            if offset_retry_count >= max_offset_retries:
                raise RuntimeError(
                    f"Exceeded max retries at offset {offset} for gym "
                    f"{gym_id}."
                ) from e
            time.sleep(min(0.5 * offset_retry_count, 5.0))
            continue
        if df.empty:
            logger.debug(f"No data returned at offset {offset}. Stopping.")
            break
        logger.debug(f"Pulled {len(df)} rows at offset {offset}.")
        fetched_send_ids.extend(str(send_id) for send_id in df['send_id'])
        if mode == 'incremental':
            if seen_send_ids:
                overlap = set(df['send_id']) & seen_send_ids
                if overlap:
                    logger.debug(
                        f"Found {len(overlap)} overlapping send_ids. "
                        f"Filtering out already-seen rows."
                    )
                    df = df[~df['send_id'].isin(seen_send_ids)]
                    if df.empty:
                        logger.debug(
                            f"All rows at offset {offset} already exist. "
                            f"Stopping."
                        )
                        break
            seen_send_ids.update(df['send_id'])
        all_data.append(df)
        if sum(len(d) for d in all_data) >= batch_size:
            batch_df = _prepare_batch_dataframe(
                pd.concat(all_data, ignore_index=True)
            )
            logger.debug(
                f"Writing batch of {len(batch_df)} rows to table 'sends' "
                f"(backend={storage_backend}, use_aws={use_aws})"
            )
            logger.info(
                f"Writing batch of {len(batch_df)} rows to table 'sends' "
                f"(backend={storage_backend}, use_aws={use_aws})"
            )
            _write_batch(
                batch_df,
                gym_id=gym_id,
                use_aws=use_aws,
                storage_backend=storage_backend,
                s3_writer=s3_writer,
                batch_index=batch_index,
            )
            total_written += len(batch_df)
            all_data = []
            batch_index += 1
        if len(df) < 15:
            logger.debug(
                f"Fewer than 15 rows returned at offset {offset}. "
                f"Assuming end of data."
            )
            break
        offset += 15
        iteration += 1
    print()  # Finish progress bar cleanly

    # Write any remaining data
    if all_data:
        batch_df = _prepare_batch_dataframe(
            pd.concat(all_data, ignore_index=True)
        )
        logger.debug(
            f"Writing final batch of {len(batch_df)} rows to table 'sends' "
            f"(backend={storage_backend}, use_aws={use_aws})"
        )
        logger.info(
            f"Writing final batch of {len(batch_df)} rows to table 'sends' "
            f"(backend={storage_backend}, use_aws={use_aws})"
        )
        _write_batch(
            batch_df,
            gym_id=gym_id,
            use_aws=use_aws,
            storage_backend=storage_backend,
            s3_writer=s3_writer,
            batch_index=batch_index,
        )
        total_written += len(batch_df)
        logger.info(f"Done writing data. Total rows written: {total_written}")
        final_batch_df = batch_df
    else:
        logger.info(f"No new data found. Total rows written: {total_written}")
        final_batch_df = None

    if storage_backend == 's3' and s3_writer is not None:
        write_recent_send_state(
            gym_id=str(gym_id),
            new_send_ids=fetched_send_ids,
            existing_send_ids=existing_recent_send_ids,
            total_written=total_written,
            run_id=s3_writer.run_id,
            run_started_at=s3_writer.run_started_at,
        )

    return final_batch_df
