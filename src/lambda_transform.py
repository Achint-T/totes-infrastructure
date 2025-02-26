# get csv files from ingestion bucket - async
# logic to take only new/updated files

# returns pd dataframe. Each dataframe is assigned a variable name df_tablename. 

# run fact_sales_orders util on dataframe - outputs new dataframe
# run all dimension utils (taking from fact dataframe)
# write to parquet util function on all resulting dataframes - async

# general client error handling