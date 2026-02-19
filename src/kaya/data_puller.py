import requests
import pandas as pd
import os
import time
from typing import Any, Dict, List, Optional, Tuple, Union
from kaya.db_manager import write_dataframe, get_engine
from kaya.secrets import load_secrets, write_secrets
import logging

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
    search_term: str
) -> pd.DataFrame:
    """Search for a gym using a search term.

    Args:
        search_term (str): The search term to look for gyms.

    Returns:
        pd.DataFrame: DataFrame containing gym search results.
    """
    json_data = {
        'operationName': 'webSearchForGym',
        'variables': {
            'term': search_term,
            'offset': 0,
            'count': 100,
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
    use_aws: bool = False
) -> List[Any]:
    """Get a list of existing send IDs for a gym from the database.

    Args:
        gym_id (Union[str, int]): The gym ID to query.
        use_aws (bool, optional): Whether to use AWS database. Defaults to
            False.

    Returns:
        List[Any]: List of send IDs.
    """
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
    batch_size: int = 1000,
    start_offset: int = 0,
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
        batch_size (int, optional): Number of records to write per batch.
            Defaults to 1000.
        start_offset (int, optional): Starting offset for data pull. Defaults
            to 0.
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
    offset = start_offset
    all_data = []
    total_written = 0

    if mode == 'incremental':
        logger.debug(
            f"Reading existing send_ids from table 'sends' (use_aws={use_aws})"
        )
        seen_send_ids = set(get_existing_send_ids(gym_id, use_aws=use_aws))
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
        except Exception as e:
            logger.debug(f"Error at offset {offset}: {e}")
            logger.warning(f"Error at offset {offset}: {e}")
            time.sleep(0.5)
            continue
        if df.empty:
            logger.debug(f"No data returned at offset {offset}. Stopping.")
            break
        logger.debug(f"Pulled {len(df)} rows at offset {offset}.")
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
            batch_df = pd.concat(all_data, ignore_index=True)
            # Ensure correct dtypes before writing
            if 'is_private' in batch_df.columns:
                batch_df['is_private'] = (
                    batch_df['is_private'].fillna(0).astype(int)
                )
            if 'is_premium' in batch_df.columns:
                batch_df['is_premium'] = (
                    batch_df['is_premium'].fillna(0).astype(int)
                )
            if 'stiffness' in batch_df.columns:
                batch_df['stiffness'] = (
                    batch_df['stiffness'].astype(int)
                )
            if 'ascent_count' in batch_df.columns:
                batch_df['ascent_count'] = (
                    batch_df['ascent_count'].astype(int)
                )
            logger.debug(
                f"Writing batch of {len(batch_df)} rows to table 'sends' "
                f"(use_aws={use_aws})"
            )
            logger.info(
                f"Writing batch of {len(batch_df)} rows to table 'sends' "
                f"(use_aws={use_aws})"
            )
            write_dataframe(
                batch_df,
                'sends',
                use_aws=use_aws,
                if_exists='upsert'
            )
            total_written += len(batch_df)
            all_data = []
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
        batch_df = pd.concat(all_data, ignore_index=True)
        # Ensure correct dtypes before writing
        if 'is_private' in batch_df.columns:
            batch_df['is_private'] = (
                batch_df['is_private'].fillna(0).astype(int)
            )
        if 'is_premium' in batch_df.columns:
            batch_df['is_premium'] = (
                batch_df['is_premium'].fillna(0).astype(int)
            )
        if 'stiffness' in batch_df.columns:
            batch_df['stiffness'] = (
                batch_df['stiffness'].astype(int)
            )
        if 'ascent_count' in batch_df.columns:
            batch_df['ascent_count'] = (
                batch_df['ascent_count'].astype(int)
            )
        logger.debug(
            f"Writing final batch of {len(batch_df)} rows to table 'sends' "
            f"(use_aws={use_aws})"
        )
        logger.info(
            f"Writing final batch of {len(batch_df)} rows to table 'sends' "
            f"(use_aws={use_aws})"
        )
        write_dataframe(
            batch_df,
            'sends',
            use_aws=use_aws,
            if_exists='upsert'
        )
        total_written += len(batch_df)
        logger.info(f"Done writing data. Total rows written: {total_written}")
        return batch_df
    else:
        logger.info(f"No new data found. Total rows written: {total_written}")
        return None
