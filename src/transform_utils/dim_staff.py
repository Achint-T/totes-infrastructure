import pandas as pd

def util_dim_staff(df_staff, df_department):
    """Performs transformation on input dataframes to convert them to suitable
       structure for data warehouse.
       
       Args:
       df_staff: The dataframe created from the staff table csv.
       df_department: The dataframe created from the department csv.
       
       Returns:
       df_dim_staff - a dataframe ready to be converted to parquet file."""

    #instead of only returning messages (apart from for df_fact_sales_order where i couldn't get this to work)
    
    if df_staff.empty:
        raise ValueError("The source dataframe for df_staff is empty")
    if df_department.empty:
        raise ValueError("The source dataframe for df_department is empty")

    required_columns_staff = {"staff_id", "first_name", "last_name", "email_address", "department_id"}
    required_columns_department = {"department_id", "department_name", "location"}

    missing_staff = required_columns_staff - set(df_staff.columns)
    missing_department = required_columns_department - set(df_department.columns)

    
    if missing_staff:
        raise KeyError(f"Missing columns in df_staff: {', '.join(missing_staff)}")
    if missing_department:
        raise KeyError(f"Missing columns in df_department: {', '.join(missing_department)}")
    
    #creating df_dim_staff:
    df_merged = df_staff.merge(df_department, on='department_id', how='left')
    df_dim_staff = pd.DataFrame()
    
    df_dim_staff['staff_id'] = df_merged['staff_id'].astype(int)

    df_dim_staff['first_name'] = df_merged['first_name'].astype(str)
    df_dim_staff['last_name'] = df_merged['last_name'].astype(str)
    df_dim_staff['email_address'] = df_merged['email_address'].astype(str)

    df_dim_staff['department_name'] = df_merged['department_name'].astype(str)
    df_dim_staff['location'] = df_merged['location'].astype(str)
    
    return df_dim_staff

