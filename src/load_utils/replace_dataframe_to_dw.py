import pandas as pd
import pg8000


def replace_dataframe_to_db(
    dataframe: pd.DataFrame, conn: pg8000.Connection, table_name: str
) -> bool:
    """Replaces table in DB with DataFrame data.

    Drops table if exists, then inserts DataFrame into a new table.

    Args:
        dataframe (pd.DataFrame): Data to insert.
        conn (pg8000.Connection): Database connection.
        table_name (str): Table name.

    Returns:
        bool: True if successful.

    Raises:
        TypeError: If input types are incorrect.
        ValueError: If DataFrame/table name is empty.
        pg8000.Error: If DB error occurs.
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

    try:
        conn.run(f"DROP TABLE IF EXISTS {table_name}")

        col_defs = []
        for col_name, dtype in dataframe.dtypes.items():
            if pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "FLOAT"
            elif pd.api.types.is_datetime64_any_dtype(dtype):
                col_type = "TIMESTAMP"
            else:
                col_type = "TEXT"
            col_defs.append(f"{col_name} {col_type}")
        create_table_sql = f"CREATE TABLE {table_name} ({','.join(col_defs)})"
        conn.run(create_table_sql)

        cols_str = ','.join(dataframe.columns)
        placeholders_str = ','.join(['%s'] * len(dataframe.columns))
        insert_sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})"
        conn.run(insert_sql, dataframe.values.tolist())

        return True
    except pg8000.Error as db_error:
        raise pg8000.Error(str(db_error)) from db_error