import csv
import json
from pathlib import Path

from kafka import KafkaProducer

from configuration.settings import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC,
    DATASET_FILE,
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, #type: ignore
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=20,
    batch_size=65536,
    compression_type="gzip",
)

project_root = Path(__file__).resolve().parent.parent
csv_file = project_root / DATASET_FILE #type: ignore

count = 0

print("Publishing records to Kafka...\n")

with open(csv_file, newline="", encoding="utf-8") as file:
    reader = csv.DictReader(file)
    for row in reader:
        producer.send(KAFKA_TOPIC, value=row) #type: ignore
        count += 1
        if count % 100000 == 0:
            print(f"Published {count:,} records")

producer.send(KAFKA_TOPIC,value={"__EOF__": True}) #type: ignore

producer.flush()
producer.close()

print("\nData sent to Kafka successfully.")
print(f"Total Records Sent : {count:,}")
print("EOF message sent.")