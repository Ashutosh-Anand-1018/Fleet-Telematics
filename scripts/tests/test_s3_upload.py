import json
from datetime import datetime
from pathlib import Path

import boto3

from configuration.settings import S3_BUCKET

s3 = boto3.client("s3")

project_root = Path(__file__).resolve().parents[2]

test_file = (
    project_root
    / "scripts"
    / "tests"
    / "vehicle_telematics_test.jsonl"
)

records = [
    {
        "tripID": 1,
        "deviceID": 101,
        "timeStamp": "2017-12-22 18:43:05",
        "accData": "10c0f8e00448fa18c80515d300000000",
        "gps_speed": 45.3,
        "battery": 12.5,
        "cTemp": 92.0,
        "dtc": 0,
        "eLoad": 34.2,
        "iat": 28.1,
        "imap": 101.3,
        "kpl": 17.8,
        "maf": 3.2,
        "rpm": 2200,
        "speed": 43,
        "tAdv": 8.1,
        "tPos": 22.4,
    },
    {
        "tripID": 2,
        "deviceID": 102,
        "timeStamp": "2017-12-22 18:43:06",
        "accData": "1138f8c804780a1ebdf718bcf919d106",
        "gps_speed": 52.1,
        "battery": 12.4,
        "cTemp": 95.0,
        "dtc": 0,
        "eLoad": 41.5,
        "iat": 30.4,
        "imap": 103.1,
        "kpl": 16.9,
        "maf": 3.7,
        "rpm": 2450,
        "speed": 50,
        "tAdv": 9.0,
        "tPos": 24.7,
    }
]

with open(test_file, "w", encoding="utf-8") as f:
    for record in records:
        f.write(json.dumps(record) + "\n")

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

object_key = f"raw/vehicle_telematics_{timestamp}.jsonl"

print(f"Uploading {test_file}...")

s3.upload_file(
    str(test_file),
    S3_BUCKET,
    object_key,
)

print("\nUpload successful.")
print(f"s3://{S3_BUCKET}/{object_key}")