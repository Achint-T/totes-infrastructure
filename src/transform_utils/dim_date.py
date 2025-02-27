import pandas as pd

def util_dim_date(start: str, end: str):
    """Creates a date range using pandas. Individual dates are then passed to
       a dataframe as individual rows.
       
       Args:
       start: The start date for the date range to be passed to the dataframe
              Example format: '2022-01-01'.
       end: The end date for the date range to be passed to the dataframe.
              Example format: '2022-01-01'
       
       Returns:
       A dataframe called df_dim_date ready to be converted to parquet.
       """

    date_range = pd.date_range(start=start, end=end)

    df_dim_date = pd.DataFrame({
        'date_id': date_range,
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'day_of_week': date_range.day_of_week + 1,
        'day_name': date_range.strftime('%A'),
        'month_name': date_range.strftime('%B'),
        'quarter': date_range.quarter
    })
    return df_dim_date