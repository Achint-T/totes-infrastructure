import pandas as pd
import boto3
import io
import botocore

def read_csv_from_s3(bucket, key):
    """Reads a CSV from S3 into a pandas DataFrame.
    
       Args:
       bucket: The S3 bucket to read csv files from.
       key: The key (name) of the csv file to be read.
       
       Returns:
       A pandas dataframe to be manipulated for the data transformation to
       warehouse star schema format."""
    
    s3_client = boto3.client('s3')
    try:
      response = s3_client.get_object(Bucket=bucket, Key=key)
      data = pd.read_csv(response['Body'])
      return data
    
    except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError):
        return "AWS credentials not found or incomplete."
    except botocore.exceptions.ClientError as e:
       if e.response['Error']['Code'] == 'NoSuchKey':
          return f"The file '{key}' does not exist in the bucket '{bucket}'."
       elif e.response['Error']['Code'] == 'AccessDenied':
            return f"Access denied to '{bucket}'. Check your S3 permissions."
    except Exception as e:
        return f"An error occurred: {str(e)}"

def write_parquet_to_s3(df, bucket, key):
    """Writes a pandas DataFrame to a given S3 bucket.
    
       Args:
       df: The dataframe to write to parquet file format.
       bucket: The S3 bucket to put the parquet file in.
       key: The desired key (name) of the parquet file.
       
       Returns:
       A string indicating the success of the upload.
       """
    
    try:
      buffer = io.BytesIO()
      df.to_parquet(buffer, engine='pyarrow', compression='snappy')
      buffer.seek(0)

      s3_client = boto3.client('s3')
      s3_client.upload_fileobj(buffer, bucket, key)

      print(f"File successfully uploaded to s3://{bucket}/{key}")
      return f"File successfully uploaded to s3://{bucket}/{key}"
    
    except (botocore.exceptions.NoCredentialsError, botocore.exceptions.PartialCredentialsError):
        return "AWS credentials not found or incomplete."
    except botocore.exceptions.BotoCoreError as e:
        return f"An AWS error occurred: {str(e)}"
    except Exception as e:
        return f"An error occurred: {str(e)}"