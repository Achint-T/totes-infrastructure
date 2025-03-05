import pandas as pd
import pg8000


def insert_dataframe_to_db(
    dataframe: pd.DataFrame, conn: pg8000.Connection, table_name: str
) -> bool:
    """Inserts data from a Pandas DataFrame into a PostgreSQL table.

    Args:
        dataframe (pd.DataFrame): DataFrame to insert.
        conn (pg8000.Connection): Database connection.
        table_name (str): Table name.

    Returns:
        bool: True if insertion successful.

    Raises:
        TypeError: If input types are incorrect.
        ValueError: If DataFrame or table name is empty.
        pg8000.Error: If database error occurs.
    """
    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError("dataframe must be a Pandas DataFrame")
    if not isinstance(conn, pg8000.Connection):
        raise TypeError("conn must be a pg8000 Connection")
    if not isinstance(table_name, str):
        raise TypeError("table_name must be a string")

    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")

    cols_str = ",".join(dataframe.columns)
    placeholders_str = ",".join(["%s"] * len(dataframe.columns))
    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})"

    try:
        conn.run(sql, dataframe.values.tolist())
        return True
    except pg8000.Error as db_error:
        raise pg8000.Error(str(db_error)) from db_error
