import pandas as pd

def util_dim_design(df_design):
    """Performs transformation on input dataframe to convert it to suitable
       structure for data warehouse.
       
       Args:
       df_design: The dataframe created from the design table csv.
       
       Returns:
       df_dim_design - a dataframe ready to be converted to parquet file."""

    df_dim_design = pd.DataFrame()

    required_columns = [
        "design_id",
        "design_name",
        "file_location",
        "file_name",
    ]

    if df_design.empty:
        return "The source dataframe for dim_design is empty"
    col_missing = [col for col in required_columns if col not in df_design.columns]
    if col_missing:
        return f"Error: Missing columns {', '.join(col_missing)}"
    
    df_dim_design = df_design[required_columns].copy()
    
    print(df_dim_design)
    return df_dim_design