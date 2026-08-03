import json
import os
from datetime import datetime

import boto3
from kafka import KafkaConsumer

from configuration.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    KAFKA_CONSUMER_GROUP,
    S3_BUCKET,
)

consumer = KafkaConsumer(
    KAFKA_TOPIC,  # type: ignore
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,  # type: ignore
    group_id=KAFKA_CONSUMER_GROUP,
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),  # type: ignore
)

s3 = boto3.client("s3")

os.makedirs("datasets/temp", exist_ok=True)

local_file = (
    f"datasets/temp/vehicle_telematics_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
)

print("Consuming messages from Kafka...\n")

record_count = 0

try:

    with open(local_file, "w", encoding="utf-8") as outfile:

        for message in consumer:

            record = message.value

            if record.get("__EOF__"):
                print("\nEOF received.")
                break

            outfile.write(json.dumps(record))
            outfile.write("\n")

            record_count += 1

            if record_count % 100000 == 0:
                print(f"Received {record_count:,} records")

finally:

    consumer.close()

print(f"\nTotal records received: {record_count:,}")

object_key = (
    f"raw/vehicle_telematics_"
    f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
)

print("\nUploading file to Amazon S3...")

s3.upload_file(
    local_file,
    S3_BUCKET,
    object_key,
)

print("\nUpload completed successfully.")
print(f"S3 Bucket : {S3_BUCKET}")
print(f"S3 Object : {object_key}")

os.remove(local_file)

print("Temporary file deleted.")