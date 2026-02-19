import json
import pandas as pd
import logging
import os
from typing import Any, Dict, Optional
from kaya.data_puller import update_gym_data

# Set up logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_gyms_config(
    config_path: Optional[str] = None
) -> pd.DataFrame:
    """Load the gym configuration from a JSON file.

    Args:
        config_path (Optional[str], optional): Path to the gyms config JSON.
            Defaults to None (uses default path).

    Returns:
        pd.DataFrame: DataFrame with columns ['gym_name', 'gym_id'].
    """
    if config_path is None:
        config_path = os.path.join(
           os.path.dirname(__file__), 'config', 'gyms_to_update.json'
        )
    with open(config_path, 'r') as f:
        gyms = json.load(f)
    return pd.DataFrame(list(gyms.items()), columns=['gym_name', 'gym_id'])


def update_all_gyms(
    mode: str = 'incremental',
    use_aws: bool = True,
    batch_size: int = 1000,
    log_level: int = logging.INFO
) -> Dict[str, str]:
    """Update all gyms listed in the configuration file.

    Args:
        mode (str, optional): Update mode ('incremental' or 'full'). Defaults
            to 'incremental'.
        use_aws (bool, optional): Whether to use AWS database. Defaults to
            True.
        batch_size (int, optional): Number of records to write per batch.
            Defaults to 1000.
        log_level (int, optional): Logging level. Defaults to logging.INFO.

    Returns:
        Dict[str, str]: Dictionary mapping gym names to update status.
    """
    gyms_df = load_gyms_config()
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
                batch_size=batch_size,
                log_level=log_level
            )
            results[gym_name] = "Success"
        except Exception as e:
            logger.error(f"Failed to update {gym_name} (id={gym_id}): {e}")
            results[gym_name] = f"Error: {e}"
    return results


def lambda_handler(
    event: Dict[str, Any],
    context: Any
) -> Dict[str, str]:
    """AWS Lambda entrypoint.

    Optionally, can pass 'mode', 'batch_size', etc. in the event dict.

    Args:
        event (Dict[str, Any]): Lambda event dict. Can include 'mode',
            'batch_size', 'log_level'.
        context (Any): Lambda context object (unused).

    Returns:
        Dict[str, str]: Dictionary mapping gym names to update status.
    """
    mode = event.get('mode', 'incremental')
    batch_size = event.get('batch_size', 1000)
    log_level = event.get('log_level', logging.INFO)
    return update_all_gyms(
        mode=mode,
        use_aws=True,
        batch_size=batch_size,
        log_level=log_level
    )


if __name__ == '__main__':
    # For local testing
    results = update_all_gyms(
        mode='incremental',
        use_aws=True,
        batch_size=1000,
        log_level=logging.INFO
    )
    print(results)
