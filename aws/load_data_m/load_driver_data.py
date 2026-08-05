# Databricks notebook source
# MAGIC %md
# MAGIC ### Load DriverPerformance into RDS + DynamoDB
# MAGIC Reads the curated driver performance data from S3 once, writes it
# MAGIC to both destinations:
# MAGIC   - RDS `DriverPerformance` — full historical/analytical record
# MAGIC   - DynamoDB `DriverScore` — fast operational lookup table

# COMMAND ----------

# ============================================================
# SECTION 1 — Credentials (S3, RDS, DynamoDB all from secrets)
# ============================================================

import boto3
from decimal import Decimal
from pyspark.sql import functions as F

SECRET_SCOPE = "fleet-telematics"
AWS_REGION = "ap-south-1"
BUCKET = "capstone-fleet-telematics"

aws_access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-access-key")
aws_secret_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-secret-key")

spark.conf.set("fs.s3a.access.key", aws_access_key)
spark.conf.set("fs.s3a.secret.key", aws_secret_key)
spark.conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
spark.conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")

s3_client = boto3.client(
    "s3", aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key, region_name=AWS_REGION,
)

# RDS credentials
RDS_HOST = dbutils.secrets.get(scope=SECRET_SCOPE, key="mukund-rds-host")
RDS_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="mukund-rds-password")
RDS_USER = "admin"  # confirm against RDS Console > Connectivity & security
RDS_DATABASE = "fleet_telematics_eng2"
RDS_PORT = 3306

# DynamoDB — reuses the same AWS keys, no new secrets needed
dynamodb = boto3.resource(
    "dynamodb", aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key, region_name=AWS_REGION,
)
DYNAMO_TABLE_NAME = "DriverScore"

print("Credentials loaded for S3, RDS, and DynamoDB.")

# COMMAND ----------

# ============================================================
# SECTION 2 — Auto-detect the latest curated batch
# ============================================================

import re

response = s3_client.list_objects_v2(
    Bucket=BUCKET, Prefix="curated/uc2/driver_performance_", Delimiter="/"
)

candidates = []
for prefix_obj in response.get("CommonPrefixes", []):
    prefix = prefix_obj["Prefix"]
    match = re.search(r"(\d{8}_\d{6})", prefix)
    if match:
        candidates.append((match.group(1), prefix))

if not candidates:
    raise ValueError(
        "No curated/uc2/ driver performance data found. Run driver_behavior_analysis.py first."
    )

candidates.sort(key=lambda x: x[0], reverse=True)
DATESTAMP, curated_prefix = candidates[0]

print(f"Found {len(candidates)} curated batch(es). Using the latest:")
print(f"  Datestamp : {DATESTAMP}")

curated_path = f"s3a://{BUCKET}/{curated_prefix}"

# COMMAND ----------

# ============================================================
# SECTION 3 — Read the data once (shared by both writers below)
# ============================================================

driver_df = spark.read.parquet(curated_path)
driver_df = driver_df.withColumn("processed_datestamp", F.lit(DATESTAMP))

row_count = driver_df.count()
print(f"Rows to load: {row_count}")
driver_df.show(20, truncate=False)

# COMMAND ----------

# ============================================================
# SECTION 4 — Write to RDS via JDBC
# ============================================================

jdbc_url = f"jdbc:mysql://{RDS_HOST}:{RDS_PORT}/{RDS_DATABASE}"
connection_properties = {
    "user": RDS_USER,
    "password": RDS_PASSWORD,
    "driver": "com.mysql.cj.jdbc.Driver",
}

driver_df.write.jdbc(
    url=jdbc_url,
    table="DriverPerformance",
    mode="overwrite",
    properties=connection_properties | {"truncate": "true"},
)

print(f"RDS: wrote {row_count} rows to DriverPerformance.")

# COMMAND ----------

# ============================================================
# SECTION 5 — Write to DynamoDB
# ============================================================

def get_or_create_dynamo_table():
    existing_tables = [t.name for t in dynamodb.tables.all()]
    if DYNAMO_TABLE_NAME in existing_tables:
        return dynamodb.Table(DYNAMO_TABLE_NAME)

    print(f"Creating DynamoDB table '{DYNAMO_TABLE_NAME}'...")
    table = dynamodb.create_table(
        TableName=DYNAMO_TABLE_NAME,
        KeySchema=[{"AttributeName": "deviceID", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "deviceID", "AttributeType": "N"}],
        BillingMode="PAY_PER_REQUEST",
    )
    table.wait_until_exists()
    print(f"Table '{DYNAMO_TABLE_NAME}' created.")
    return table


def to_decimal(val):
    if val is None:
        return None
    return Decimal(str(round(float(val), 4)))


table = get_or_create_dynamo_table()

# Small dataset (max ~16 drivers) — safe to collect() into driver memory
# rather than needing a distributed write.
rows = driver_df.collect()

with table.batch_writer() as batch:
    for r in rows:
        item = {
            "deviceID": int(r["deviceID"]),
            "driver_safety_score": to_decimal(r["driver_safety_score"]),
            "fuel_efficiency_score": to_decimal(r["fuel_efficiency_score"]),
            "driver_category": str(r["driver_category"]),
            "driver_rank": int(r["driver_rank"]),
            "total_trips": int(r["total_trips"]),
            "aggressive_event_count": int(r["aggressive_event_count"]),
            "processed_datestamp": DATESTAMP,
        }
        item = {k: v for k, v in item.items() if v is not None}
        batch.put_item(Item=item)

print(f"DynamoDB: wrote {len(rows)} rows to {DYNAMO_TABLE_NAME}.")

# COMMAND ----------

# ============================================================
# SECTION 6 — Summary
# ============================================================

print(f"\n=== Load complete ===")
print(f"Datestamp      : {DATESTAMP}")
print(f"Rows loaded    : {row_count}")
print(f"RDS table      : {RDS_DATABASE}.DriverPerformance")
print(f"DynamoDB table : {DYNAMO_TABLE_NAME}")

dbutils.notebook.exit("LOADED_RDS_AND_DYNAMODB")
