from pg8000.native import Connection
import os

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
    
# def get_last_upload_date(client, objects = client.list_objects_v2(Bucket='s3_ingestion_bucket')):
#     try:
#         date_info = max([obj['Key'] for obj in objects['Contents'] if re.match(r"^20\d\d/\d+/\d+/\d+/\d+/",obj['Key'])]).split('/')
#         return f'{date_info[0]}-{date_info[1].rjust(2,'0')}-{date_info[2].rjust(2,'0')} {date_info[3].rjust(2,'0')}:{date_info[4].rjust(2,'0')}:00'
#     except:
#         return "2020-01-01 00:00"