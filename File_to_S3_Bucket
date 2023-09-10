# Libraries import
import boto3
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

s3 = boto3.client('s3', aws_access_key_id = os.getenv('aws_access_key_id'), aws_secret_access_key = os.getenv('aws_secret_access_key'))

# Bucket name and file path
bucket_name = 'databricks-workspace-stack-1a438-metastore-bucket'
file_path = 'D:\Timi Centre\Career\Jobs\SH24\data_eng_interview_task_data.xlsx'
object_name = 'data_eng_interview_task_data.xlsx' 

# Upload the file
s3.upload_file(file_path, bucket_name, object_name)

print(f"File {file_path} uploaded to {bucket_name}/{object_name}")
