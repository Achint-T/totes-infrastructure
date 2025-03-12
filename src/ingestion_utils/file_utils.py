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
        with open(f'/tmp/{table_name}.csv', 'w', newline='') as local_file:
            writer = csv.writer(local_file)
            writer.writerow(headers)
            writer.writerows(body)
    except IOError as e:
        raise IOError(f"Error writing to CSV file: {e}") from e
    
def get_current_time(time_object):
    """Formats the given time object into a timestamp and filepath structure.

    Extracts and formats the date and time components from the given time object, 
    adjusting the seconds by subtracting one. The function returns a dictionary 
    containing timestamp and filepath information.

    Args:
        time_object (time.struct_time or list): A time object (e.g., from `time.gmtime()`) 
            or a list in the format [year, month, day, hour, minute, second].

    Returns:
        dict: A dictionary with keys 'secret' (formatted timestamp) and 'filepath' 
            (formatted date path).
    """
    timenow = list(time_object[:5]) + [time_object[5]-1]
    date = '-'.join([str(number).rjust(2,'0') for number in timenow[:3]])
    hours = ':'.join([str(number).rjust(2,'0') for number in timenow[3:]])
    timestamp = f'{date} {hours}'
    return {'secret':timestamp, 'filepath': '/'.join(map(str,timenow[:-1]))}