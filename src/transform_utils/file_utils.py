import pandas as pd
import boto3

def read_csv_from_s3(bucket, key):
    """Reads a CSV from S3 into a pandas DataFrame.
    
       Args:
       bucket: The S3 bucket to read csv files from.
       key: The key (name) of the csv file to be read.
       
       Returns:
       A pandas dataframe to be manipulated for the data transformation to
       warehouse star schema format."""
    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    data = pd.read_csv(response['Body'])
    return data