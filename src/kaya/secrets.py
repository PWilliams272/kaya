import os
from dotenv import load_dotenv
import boto3
import json

load_dotenv(override=True)


def is_aws_lambda(
    force_aws: bool = False
) -> bool:
    """Determine if should use AWS.

    Determines if the code is running in an AWS Lambda environment, or will
    also return True if force_aws is set as True.

    Args:
        force_aws (bool, optional): If True, force AWS mode. Defaults to
            False.

    Returns:
        bool: True if running in AWS Lambda or force_aws is True.
    """
    return force_aws or 'AWS_LAMBDA_FUNCTION_NAME' in os.environ


def load_secrets(
    force_aws: bool = False
) -> None:
    """Loads Kaya API secrets.

    Load Kaya API secrets from .env (locally) or AWS Secrets Manager (in AWS
    Lambda or with force_aws). Sets os.environ for KAYA_API_TOKEN and
    KAYA_REFRESH_TOKEN.

    Args:
        force_aws (bool, optional): If True, force AWS mode. Defaults to False.

    Returns:
        None
    """
    if is_aws_lambda(force_aws):
        client = boto3.client(
            'secretsmanager',
            region_name=os.getenv('AWS_REGION')
        )
        secret = client.get_secret_value(
            SecretId=os.getenv('KAYA_API_TOKENS_SECRET_NAME')
        )
        secrets = json.loads(secret['SecretString'])
        os.environ["KAYA_API_TOKEN"] = secrets["KAYA_API_TOKEN"]
        os.environ["KAYA_REFRESH_TOKEN"] = secrets["KAYA_REFRESH_TOKEN"]
    else:
        load_dotenv(override=True)


def write_secrets(
    new_access_token: str,
    new_refresh_token: str,
    force_aws: bool = False
) -> None:
    """Write Kaya API secrets.

    Write Kaya API secrets to .env (locally) or AWS Secrets Manager (in AWS
    Lambda or with force_aws). Updates os.environ for immediate use.

    Args:
        new_access_token (str): The new access token.
        new_refresh_token (str): The new refresh token.
        force_aws (bool, optional): If True, force AWS mode. Defaults to
            False.
    """
    os.environ["KAYA_API_TOKEN"] = new_access_token
    os.environ["KAYA_REFRESH_TOKEN"] = new_refresh_token
    if is_aws_lambda(force_aws):
        client = boto3.client(
            'secretsmanager',
            region_name=os.getenv('AWS_REGION')
        )
        secret_dict = {
            "KAYA_API_TOKEN": new_access_token,
            "KAYA_REFRESH_TOKEN": new_refresh_token
        }
        client.put_secret_value(
            SecretId=os.getenv('KAYA_API_TOKENS_SECRET_NAME'),
            SecretString=json.dumps(secret_dict)
        )
    else:
        # Update .env file for persistence
        env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
        env_path = os.path.abspath(env_path)

        def update_env_var(
            lines: list,
            var: str,
            value: str
        ) -> list:
            found = False
            for i, line in enumerate(lines):
                if line.strip().startswith(f'{var}='):
                    lines[i] = f'{var}={value}\n'
                    found = True
            if not found:
                lines.append(f'{var}={value}\n')
            return lines
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                lines = f.readlines()
        else:
            lines = []
        lines = update_env_var(lines, 'KAYA_API_TOKEN', new_access_token)
        lines = update_env_var(lines, 'KAYA_REFRESH_TOKEN', new_refresh_token)
        with open(env_path, 'w') as f:
            f.writelines(lines)
