import pandas as pd
import pg8000


def write_dataframe_to_db(
    dataframe: pd.DataFrame, conn: pg8000.Connection, table_name: str, insert_mode: bool = True
) -> bool:
    """Writes data from a Pandas DataFrame into a PostgreSQL table, with options for insert or replace.

    If insert_mode is True (default), data is inserted into an existing table.
    If insert_mode is False, the table is dropped if it exists and a new table is created
    based on the DataFrame schema before inserting data (replace mode).

    Args:
        dataframe (pd.DataFrame): DataFrame to write to the database.
        conn (pg8000.Connection): Database connection.
        table_name (str): Table name.
        insert_mode (bool, optional): True for insert mode, False for replace mode. Defaults to True.

    Returns:
        bool: True if write operation successful.

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
    if not isinstance(insert_mode, bool):
        raise TypeError("insert_mode must be a boolean")

    if dataframe.empty:
        raise ValueError("DataFrame cannot be empty")
    if not table_name:
        raise ValueError("table_name cannot be empty")

    try:
        if not insert_mode:  # Replace mode when insert_mode is False
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

        cols_str = ",".join(dataframe.columns)
        placeholders_str = ",".join(["%s"] * len(dataframe.columns))
        sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})"
        conn.run(sql, dataframe.values.tolist())

        return True
    except pg8000.Error as db_error:
        raise pg8000.Error(str(db_error)) from db_error