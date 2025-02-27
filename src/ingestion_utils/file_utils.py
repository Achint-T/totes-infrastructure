import csv

def data_to_csv(data,table_name):
    body = data['body']
    headers = data['headers']
    with open(f'{table_name}.csv', 'w') as local_file:
        writer = csv.writer(local_file)
        writer.writerow(headers)
        writer.writerows(body)