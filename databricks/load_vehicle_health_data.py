# Databricks notebook source
# MAGIC %md
# MAGIC ### Load VehicleHealthReport into RDS + DynamoDB
# MAGIC Reads the curated vehicle health data from S3 once, writes it to
# MAGIC both destinations:
# MAGIC   - RDS `VehicleHealthReport` — full historical/analytical record
# MAGIC   - DynamoDB `VehicleAlerts` — fast operational lookup table

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

# RDS credentials — reuse the SAME instance/secrets as UC-2 (single shared
# RDS instance per your decision earlier), unless you want VehicleHealthReport
# on a separate instance too — adjust rds-host/rds-password key names if so.
RDS_HOST = dbutils.secrets.get(scope=SECRET_SCOPE, key="mukund-rds-host")
RDS_PASSWORD = dbutils.secrets.get(scope=SECRET_SCOPE, key="mukund-rds-password")
RDS_USER = "admin"  # confirm against RDS Console
RDS_DATABASE = "fleet_telematics_eng2"
RDS_PORT = 3306

dynamodb = boto3.resource(
    "dynamodb", aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key, region_name=AWS_REGION,
)
DYNAMO_TABLE_NAME = "VehicleAlerts"

print("Credentials loaded for S3, RDS, and DynamoDB.")

# COMMAND ----------

# ============================================================
# SECTION 2 — Auto-detect the latest curated batch
# ============================================================

import re

response = s3_client.list_objects_v2(
    Bucket=BUCKET, Prefix="curated/uc1/vehicle_health_", Delimiter="/"
)

candidates = []
for prefix_obj in response.get("CommonPrefixes", []):
    prefix = prefix_obj["Prefix"]
    match = re.search(r"(\d{8}_\d{6})", prefix)
    if match:
        candidates.append((match.group(1), prefix))

if not candidates:
    raise ValueError(
        "No curated/uc1/ vehicle health data found. Run vehicle_health_analysis.py first."
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

vehicle_df = spark.read.parquet(curated_path)
vehicle_df = vehicle_df.withColumn("processed_datestamp", F.lit(DATESTAMP))

row_count = vehicle_df.count()
print(f"Rows to load: {row_count}")
vehicle_df.show(20, truncate=False)

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

vehicle_df.write.jdbc(
    url=jdbc_url,
    table="VehicleHealthReport",
    mode="overwrite",
    properties=connection_properties | {"truncate": "true"},
)

print(f"RDS: wrote {row_count} rows to VehicleHealthReport.")

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
rows = vehicle_df.collect()

with table.batch_writer() as batch:
    for r in rows:
        item = {
            "deviceID": int(r["deviceID"]),
            "vehicle_health_score": to_decimal(r["vehicle_health_score"]),
            "is_high_risk": bool(r["is_high_risk"]),
            "maintenance_recommendation": str(r["maintenance_recommendation"]),
            "risk_rank": int(r["risk_rank"]),
            "total_alert_count": int(r["total_alert_count"]),
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
print(f"RDS table      : {RDS_DATABASE}.VehicleHealthReport")
print(f"DynamoDB table : {DYNAMO_TABLE_NAME}")

dbutils.notebook.exit("LOADED_RDS_AND_DYNAMODB_UC1")
