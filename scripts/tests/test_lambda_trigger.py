import json
from datetime import datetime
from io import BytesIO

import boto3

from configuration.settings import S3_BUCKET

s3 = boto3.client("s3")


def upload_test_file():

    test_record = {
        "event": "lambda_test",
        "timestamp": datetime.utcnow().isoformat(),
        "status": "SUCCESS"
    }

    object_key = (
        f"raw/test_lambda_"
        f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    )

    file_buffer = BytesIO()

    file_buffer.write(
        (json.dumps(test_record) + "\n").encode("utf-8")
    )

    file_buffer.seek(0)

    s3.upload_fileobj(
        file_buffer,
        S3_BUCKET,
        object_key
    )

    print("Test file uploaded successfully.")
    print(f"Bucket : {S3_BUCKET}")
    print(f"Object : {object_key}")
    print("\nIf the S3 Event Notification is configured correctly,")
    print("the Lambda function should be triggered immediately.")


if __name__ == "__main__":
    upload_test_file()