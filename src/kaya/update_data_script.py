import json
import pandas as pd
import logging
from kaya.data_puller import update_gym_data
import os

# Set up logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_gyms_config(
    config_path=None
):
    if config_path is None:
        config_path = os.path.join(
           os.path.dirname(__file__), 'config', 'gyms_to_update.json'
        )
    with open(config_path, 'r') as f:
        gyms = json.load(f)
    return pd.DataFrame(list(gyms.items()), columns=['gym_name', 'gym_id'])


def update_all_gyms(
    mode='incremental',
    use_aws=True,
    batch_size=1000,
    log_level=logging.INFO
):
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
    event,
    context
):
    """
    AWS Lambda entrypoint.
    Optionally, you can pass 'mode', 'batch_size', etc. in the event dict.
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
