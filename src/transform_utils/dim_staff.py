import pandas as pd

# Inputs: Pandas dataframe for staff
# Create new dataframe with data that has been transformed
# Return new dataframe 

def util_dim_staff(df_fact_sales_order, df_staff, df_department):
    #error handling for empty dataframes and missing columns. raising errors
    #instead of only returning messages (apart from for df_fact_sales_order where i couldn't get this to work)
    
    if df_fact_sales_order.empty:
        return "The source dataframe for df_fact_sales_order is empty"
    if df_staff.empty:
        raise ValueError("The source dataframe for df_staff is empty")
    if df_department.empty:
        raise ValueError("The source dataframe for df_department is empty")

    required_columns_fact_sales = {"staff_id"}
    required_columns_staff = {"first_name", "last_name", "email_address", "department_id"}
    required_columns_department = {"department_id", "department_name", "location"}

    missing_fact_sales = required_columns_fact_sales - set(df_fact_sales_order.columns)
    missing_staff = required_columns_staff - set(df_staff.columns)
    missing_department = required_columns_department - set(df_department.columns)

    if missing_fact_sales:
        raise KeyError(f"Missing columns in df_fact_sales_order: {', '.join(missing_fact_sales)}")
    if missing_staff:
        raise KeyError(f"Missing columns in df_staff: {', '.join(missing_staff)}")
    if missing_department:
        raise KeyError(f"Missing columns in df_department: {', '.join(missing_department)}")
    
    #creating df_dim_staff:
    df_merged = df_staff.merge(df_department, on='department_id', how='left')
    df_dim_staff = pd.DataFrame()
    
    df_dim_staff['staff_id'] = df_fact_sales_order['staff_id'].astype(int)

    df_dim_staff['first_name'] = df_staff['first_name'].astype(str)
    df_dim_staff['last_name'] = df_staff['last_name'].astype(str)
    df_dim_staff['email_address'] = df_staff['email_address'].astype(str)

    df_dim_staff['department_name'] = df_merged['department_name'].astype(str)
    df_dim_staff['location'] = df_merged['location'].astype(str)
    
    
    return df_dim_staff

