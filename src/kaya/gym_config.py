import json
import os
from typing import Optional

import pandas as pd


def load_gyms_config(
    config_path: Optional[str] = None
) -> pd.DataFrame:
    """Load the gym configuration from a JSON file."""
    if config_path is None:
        config_path = os.path.join(
            os.path.dirname(__file__), 'config', 'gyms_to_update.json'
        )
    with open(config_path, 'r') as config_file:
        gyms = json.load(config_file)
    return pd.DataFrame(list(gyms.items()), columns=['gym_name', 'gym_id'])