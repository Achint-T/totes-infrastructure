from pg8000.native import Connection
import os
from botocore.exceptions import ClientError
import time

def create_connection() -> Connection:
    """Creates a database connection using environment variables.

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

    To-do:
        SQL injection prevention
    """
    try:
        data = conn.run(f'SELECT * FROM {tablename} WHERE last_updated BETWEEN \'{updatedate}\' AND \'{time_now}\';')
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
        the value of the last upload date in the form of a time stamp 'YYYY-MM-DD 00:00:00' - defaults to the start of 2020 if no value found
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
    
def get_current_time(time_object):
    
    '''
    Takes in a time object from time.gmtime() and returns the timestamp (for sql and secret) and the filepath for the s3 bucket
    Returns in the form: {'secret':'2025-05-13 16:05:14', 'filepath':'2025/5/13/16/5'}
    ATTENTION: this returns a second slower to ensure that it's only pulling data from a second that has fully passed to prevent duplicates and missing rows
    '''

    timenow = list(time_object[:5]) + [time_object[5]-1]
    date = '-'.join([str(number).rjust(2,'0') for number in timenow[:3]])
    hours = ':'.join([str(number).rjust(2,'0') for number in timenow[3:]])
    timestamp = f'{date} {hours}'
    return {'secret':timestamp, 'filepath': '/'.join(map(str,timenow[:-1]))}

def put_last_upload_date(time_object, secretclient):
    
    '''
    Takes in the timeobject and secretlient and updates the secret
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