import boto3
from botocore.exceptions import ClientError

from configuration.settings import AWS_REGION, S3_BUCKET

s3 = boto3.client("s3", region_name=AWS_REGION)


def create_bucket():

    try:
        if AWS_REGION == "us-east-1":
            s3.create_bucket(Bucket=S3_BUCKET)
        else:
            s3.create_bucket(
                Bucket=S3_BUCKET,
                CreateBucketConfiguration={
                    "LocationConstraint": AWS_REGION
                },
            )

        print(f"Bucket '{S3_BUCKET}' created successfully.")

    except ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code == "BucketAlreadyOwnedByYou":
            print(f"Bucket '{S3_BUCKET}' already exists.")
        else:
            raise

    # Create logical folders
    prefixes = [
        "raw/",
        "processed/",
        "curated/",
        "reports/"
    ]

    for prefix in prefixes:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=prefix
        )

if __name__ == "__main__":
    create_bucket()