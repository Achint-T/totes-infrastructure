import boto3
import os
import json
from typing import Dict, Any, List
import logging
from botocore.client import BaseClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_credentials(secrets_client: BaseClient, secret_name: str) -> Dict[str, Any]:
    """Fetches database credentials from AWS Secrets Manager

    Uses a provided boto3 Secrets Manager client to retrieve a secret value
    and parses it as JSON.

    Returns a dictionary of database credentials on success.
    Raises an exception and stops program execution if fetching or parsing fails.

    Args:
        secrets_client (boto3.client): Pre-initialized boto3 Secrets Manager client.
                                        Must be configured to connect to AWS.
        secret_name (str): Name of the Secrets Manager secret.

    Returns:
        Dict[str, Any]: Dictionary of database credentials.
                         Structure matches the JSON format in Secrets Manager
                         (e.g., keys like 'username', 'password').

    Raises:
        Exception: If fetching from Secrets Manager or JSON parsing fails.
                   Propagates the specific exception encountered. Halts program execution.
    """
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret_string = response["SecretString"].replace("'", '"')

        credentials = json.loads(secret_string)

        logger.info(
            f"Successfully fetched database credentials from Secrets Manager: {secret_name}"
        )
        return credentials

    except Exception as e:
        logger.error(
            f"Error fetching database credentials from Secrets Manager: {secret_name}. Error: {e}"
        )
        raise e


def export_db_creds_to_env(
    credentials: Dict[str, Any], expected_keys: List[str]
) -> None:
    """Exports database credentials to environment variables.

    Takes a dictionary of credentials, validates expected keys, and exports them as uppercase environment variables.

    Raises an exception and stops program execution if validation or export fails.

    Args:
        credentials (Dict[str, Any]): Dictionary of database credentials. Keys will be used as env var names (uppercased).
        expected_keys (List[str]): List of keys expected in `credentials` for validation.

    Raises:
        ValueError: If `credentials` is not a dictionary, missing expected keys, or value is None.
        OSError: If environment variable setting fails.
        Exception: For unexpected errors during export.
    """
    if not isinstance(credentials, dict):
        error_message = "Input credentials must be a dictionary."
        logger.error(error_message)
        raise ValueError(error_message)

    for key in expected_keys:
        if key not in credentials:
            error_message = f"Expected key '{key}' not found in credentials dictionary."
            logger.error(error_message)
            raise ValueError(error_message)
        if credentials[key] is None:
            error_message = f"Value for key '{key}' is None, which is not allowed for environment variables."
            logger.error(error_message)
            raise ValueError(error_message)

    logger.info("Starting to export database credentials to environment variables.")
    try:
        for key, value in credentials.items():
            env_var_name = key.upper()
            env_var_value = str(value)
            os.environ[env_var_name] = env_var_value
            logger.info(f"Exported environment variable: {env_var_name}")
        logger.info(
            "Successfully exported all database credentials to environment variables."
        )

    except Exception as e:
        error_message = f"An unexpected error occurred while exporting credentials to environment variables: {e}"
        logger.error(error_message)
        raise Exception(error_message)