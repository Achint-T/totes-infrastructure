import csv

def data_to_csv(data, table_name):
    """Writes data dictionary to a CSV file.

    Extracts 'headers' and 'body' from the input data dictionary and writes
    them to a CSV file named '{table_name}.csv'.

    Args:
        data (dict): A dictionary containing 'headers' (list of column names)
                     and 'body' (list of lists representing rows of data).
        table_name (str): The name to use for the CSV file (without extension).

    Raises:
        TypeError: If `data` is not a dictionary or if 'headers' or 'body' keys are missing.
        ValueError: If 'headers' or 'body' are not lists.
        IOError: If there is an error writing to the CSV file.
    """
    if not isinstance(data, dict):
        raise TypeError("Input 'data' must be a dictionary.")
    if 'headers' not in data or 'body' not in data:
        raise ValueError("Input 'data' dictionary must contain 'headers' and 'body' keys.")
    if not isinstance(data['headers'], list) or not isinstance(data['body'], list):
        raise ValueError("'headers' and 'body' in 'data' must be lists.")

    body = data['body']
    headers = data['headers']
    try:
        with open(f'{table_name}.csv', 'w', newline='') as local_file:
            writer = csv.writer(local_file)
            writer.writerow(headers)
            writer.writerows(body)
    except IOError as e:
        raise IOError(f"Error writing to CSV file: {e}") from e