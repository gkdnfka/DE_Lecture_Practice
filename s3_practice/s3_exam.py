import os
import io
import glob
import logging

import boto3
from botocore.exceptions import ClientError

from utils import get_datalake_raw_layer_path, get_datalake_bucket_name


def create_bucket(bucket_name, region=None):
    try:
        if region is None:
            s3_client = boto3.client("s3")
            s3_client.create_bucket(Bucket=bucket_name)

        else:
            s3_client = boto3.client("s3", region_name=region)
            location = {"LocationConstraint": region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)

    except ClientError as e:
        logging.error(e, exc_info=True)
        return False

    return True


def list_buckets():
    s3 = boto3.client("s3")
    response = s3.list_buckets()

    for bucket in response['Buckets']:
        print(bucket["Name"])


def download_file(bucket, src, dst):
    s3 = boto3.client("s3")
    s3.download_file(bucket, src, dst)


def download_file_memory(bucket, src):
    s3 = boto3.client("s3")
    io_stream = io.BytesIO()
    s3.download_fileobj(bucket, src, io_stream)
    io_stream.seek(0) #cursor 리셋.
    return io_stream.read().decode("utf-8")


def upload_file(src, bucket, dst=None):
    if dst is None:
        dst = os.path.basename(src)

    s3_client = boto3.client("s3")

    try:
        s3_client.upload_file(src, bucket, dst)
    except ClientError as e:
        logging.error(e, exc_info=True)
        return False
    return True


def upload_file_memory(io_stream, bucket, dst):
    s3_client = boto3.client("s3")
    try:
        s3_client.upload_fileobj(io_stream, bucket, dst)

    except ClientError as e:
        logging.error(e, exc_info=True)
        return False

    return True


if __name__ == '__main__':
    #bucket = "aws-cli-test-wooram"
    #create_bucket("aws-sdk-test-wooram")
    #list_buckets()
    #download_file("aws-cli-test-wooram", "cli/data1.txt", "./data/data1.txt")
    #data = download_file_memory("aws-cli-test-wooram", "cli/data1.txt")
    #print(data)

    #upload_file("./data/data1.txt", bucket, "sdk/data1.txt")
    '''
    with open("./data/data1.txt", "rb") as f:
        upload_file_memory(f, bucket, "sdk/data1_stream.txt")
    '''

    bucket_name = get_datalake_bucket_name(
        layer="raw",
        company="de403",
        region="apnortheast2",
        account="073658113926",
        env="dev"
    )
    #create_bucket(bucket_name=bucket_name, region="ap-northeast-2")

    files = glob.glob("./data/*.json")
    dst = get_datalake_raw_layer_path(
        source="local",
        source_region="apnortheast2",
        table="bike-data",
        year=2023, month=8, day=2, hour=11
    )
    for f in files:
        upload_file(src=f,
                    bucket=bucket_name,
                    dst=f"{dst}/{os.path.basename(f)}"
                    )
    
