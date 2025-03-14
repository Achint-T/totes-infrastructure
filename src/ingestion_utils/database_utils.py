from pg8000.native import Connection, identifier, literal
import os
from botocore.exceptions import ClientError
import time


def create_connection() -> Connection:
    """Creates a database connection using environment variables

    This function reads connection parameters from environment variables
    and establishes a database connection.

    Returns:
        Connection: A Connection object representing the database connection.

    Raises:
        KeyError: If any of the required environment variables are missing.
        Exception: If there is an error during connection creation (replace with actual connection exception if known).
                  For example, if the Connection class constructor raises an error for invalid parameters.
    """
    try:
        connection = Connection(os.environ['USERNAME'],
                                password=os.environ['PASSWORD'],
                                host=os.environ['HOST'],
                                port=os.environ['PORT'],
                                database=os.environ['DBNAME'])
        return connection
    except KeyError as e:
        raise KeyError(f"Missing environment variable: {e}") from e
    except Exception as e:
        raise Exception(f"Error creating connection: {e}") from e

def close_db_connection(conn):
    """Closes the given database connection.

    Args:
        conn (object): Database connection instance to be closed.

    Returns:
        None
    """
    conn.close()

def get_recent_additions(conn, tablename: str, updatedate: str, time_now: str) -> dict:
    """Retrieves recent data from a table within a time range.

    Executes a SQL query to fetch data from `tablename` where `last_updated`
    is between `updatedate` and `time_now`.

    Args:
        conn: Database connection object with `run` and `columns` methods.
        tablename: Table name to query. **Caution: Vulnerable to SQL injection. Sanitize input.**
        updatedate: Start timestamp (inclusive). Database compatible format.
        time_now: End timestamp (inclusive). Database compatible format.

    Returns:
        dict: Dictionary with 'headers' (list of column names) and 'body' (query data).

    Raises:
        Exception: If database query or column retrieval fails.
    """
    try:
        data = conn.run(f'SELECT * FROM {identifier(tablename)} WHERE last_updated BETWEEN \'{updatedate}\' AND \'{time_now}\';')
        columns_info = conn.columns
        headers = [col["name"] for col in columns_info]
        return {'headers':headers, 'body':data}
    except Exception as e:
        raise Exception(f"Error fetching recent additions: {e}") from e
    
def get_last_upload_date(secretsclient):
    """Retrieves the date of the last ingestion which is stored inside a secret.

    Args:
        secretsclient: Boto3 client connecting to aws secret manager

    Returns:
        The value of the last upload date in the form of a time stamp 'YYYY-MM-DD 00:00:00'
            - defaults to the start of 2020 if no value found.
    
    Raises:
        Exception: If an error occurs while fetching the secret.
    """
    try:
        timestamp = secretsclient.get_secret_value(SecretId = 'lastupload')['SecretString']
        return timestamp
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            return '2020-01-01 00:00:00'
        else:
            raise Exception(f"Error fetching last upload: {e}")
    except Exception as e:

        raise Exception(f"Error fetching last upload: {e}")

def put_last_upload_date(time_object, secretclient):
    '''Updates the timestamp of the latest ingestion run.

    Takes in the timeobject from time.gmtime() or a list in the form [year,month,day,hour,minute,second] and secret client from boto3.client
        - if the secrets exsists it updates it otherwise creates a new secret called 'lastupload'
    
    Args:
        time_object (object or list): Time object representing the last 
            run timestamp.
        secretclient (object): Client for accessing the secrets manager.

    Returns:
        None

    Raises:
        Exception: If an error occurs while attempting update the secret.
    '''
    date = '-'.join([str(number).rjust(2,'0') for number in time_object[:3]])
    hours = ':'.join([str(number).rjust(2,'0') for number in time_object[3:6]])
    timestamp = f'{date} {hours}'

    try:
        secretclient.update_secret(SecretId='lastupload',SecretString=timestamp)
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            secretclient.create_secret(Name='lastupload',SecretString=timestamp)
        else:
            raise Exception(f"Error putting last upload: {e}")
    except Exception as e:
        raise Exception(f"Error putting last upload: {e}")