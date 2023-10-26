import os
import boto3
import shutil
import subprocess
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession


load_dotenv()

aws_key = os.environ['AWS_ACCESS_KEY']
aws_secret = os.environ['AWS_SECRET_ACCESS_KEY']
aws_token = os.environ['AWS_SESSION_TOKEN']

"""
A function to read file from S3.

Args:
bucket_name str Name of S3 bucket
object_key str Name of object file

Returns:
df File read in the form of dataframe
"""
def get_file_from_S3(bucket_name, object_key):
    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        aws_session_token=aws_token
        )
    s3 = session.client('s3')

    s3_object = s3.get_object(Bucket=bucket_name, Key=object_key)
    body = s3_object['Body']
    df = pd.read_csv(body)

    return df


"""
A function to upload file to S3.

Args:
bucket_name str Name of S3 bucket
src_path str Path to source file
dest_path str Path to destination file in S3
"""
def upload(bucket_name, src_path, dest_path):
    sys_cmd = "aws s3 cp {0} s3://{1}/{2} --recursive".format(src_path, bucket_name, dest_path)

    subprocess.call(sys_cmd, shell=True)

    shutil.rmtree(src_path)


"""
A function to download file to S3.

Args:
bucket_name str Name of S3 bucket
src_path str Path to source file
dest_path str Path to destination file in S3
"""
def download(bucket_name, src_path, dest_path):
    sys_cmd = "aws s3 cp s3://{0}/{1} {2} --recursive".format(bucket_name, src_path, dest_path)

    subprocess.call(sys_cmd, shell=True)


"""
A function to store the batch prediction in dynamodb.

Args:
table str Name of table in dynamodb
data JSON  data to store in dynamodb
"""
def store_prediction(table, data):

    session = boto3.Session(
        aws_access_key_id=aws_key,
        aws_secret_access_key=aws_secret,
        aws_session_token=aws_token,
        region_name="us-east-1"
        )
    dynamodb = session.client('dynamodb')
    dynamodb.put_item(TableName=table, Item=data)

"""
A function to get spark session.

Returns:
spark A sparksession
"""
def get_spark():
    spark = SparkSession.builder.master("local[1]").appName("fraud_detection.com").getOrCreate()
    return spark
