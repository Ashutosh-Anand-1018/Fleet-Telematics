# Databricks notebook source
# MAGIC %run /fleet-telematics/configure_s3_credentials

# COMMAND ----------

# Databricks notebook source
"""
Fleet Telematics — shared cleaning job (single-file version)
 
What this notebook does, top to bottom:
  1. Loads AWS credentials from Databricks secrets (cross-account S3 access,
     since this cluster isn't in the same AWS account as the S3 bucket)
  2. Checks S3 to see if this raw file has already been processed
     (idempotency) — skips if so
  3. Reads the raw JSONL from S3, cleans it (dedup, filter malformed
     rows, type-cast), and writes the result back to S3 processed/
"""
 
import re
 
import boto3
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

SECRET_SCOPE = "fleet-telematics"
AWS_REGION = "ap-south-1"
BUCKET = "capstone-fleet-telematics"
 
aws_access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-access-key")
aws_secret_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-secret-key")
 
# Spark config — use s3a:// paths (not s3://) everywhere below, since
# s3a:// is what respects these explicit keys.
spark.conf.set("fs.s3a.access.key", aws_access_key)
spark.conf.set("fs.s3a.secret.key", aws_secret_key)
spark.conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
spark.conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
 
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=AWS_REGION,
)
 
try:
    s3_client.head_bucket(Bucket=BUCKET)
    print(f"S3 credentials verified — successfully reached bucket: {BUCKET}")
except Exception as e:
    print(f"S3 credential check FAILED: {e}")
    raise
 
# COMMAND ----------

# COMMAND ----------

 
# ============================================================
# SECTION 2 — Parameters (from Step Functions, or set manually to test)
# ============================================================
 
dbutils.widgets.text("source_key", "")  # e.g. raw/vehicle_telematics_20260804_063344.jsonl
 
SOURCE_KEY = dbutils.widgets.get("source_key")
if not SOURCE_KEY:
    raise ValueError("source_key parameter is required (the raw S3 object key)")
 
match = re.search(r"(\d{8}_\d{6})", SOURCE_KEY)
if not match:
    raise ValueError(f"Could not extract datestamp from source_key: {SOURCE_KEY}")
DATESTAMP = match.group(1)
 
PROCESSED_PREFIX = f"processed/vehicle_telematics_processed_{DATESTAMP}"
 
print(f"Source    : s3a://{BUCKET}/{SOURCE_KEY}")
print(f"Datestamp : {DATESTAMP}")
print(f"Target    : s3a://{BUCKET}/{PROCESSED_PREFIX}/")
 
# COMMAND ---------

# COMMAND ----------

response = s3_client.list_objects_v2(Bucket="capstone-fleet-telematics", Prefix="raw/")
for obj in response.get("Contents", []):
    print(obj["Key"])

# COMMAND ----------

# ============================================================
# SECTION 3 — Idempotency check
# ============================================================
 
existing = s3_client.list_objects_v2(
    Bucket=BUCKET, Prefix=PROCESSED_PREFIX, MaxKeys=1
)
 
if existing.get("KeyCount", 0) > 0:
    print(f"\nSKIPPED: processed output already exists at "
          f"s3a://{BUCKET}/{PROCESSED_PREFIX}/ — not reprocessing.")
    dbutils.notebook.exit("SKIPPED_ALREADY_PROCESSED")
 
print("\nNo existing processed output found for this datestamp. Proceeding.")
 
# COMMAND ----------

# COMMAND ----------

RAW_SCHEMA = StructType([
    StructField("tripID", StringType(), True),
    StructField("deviceID", StringType(), True),
    StructField("timeStamp", StringType(), True),
    StructField("accData", StringType(), True),
    StructField("gps_speed", StringType(), True),
    StructField("battery", StringType(), True),
    StructField("cTemp", StringType(), True),
    StructField("dtc", StringType(), True),
    StructField("eLoad", StringType(), True),
    StructField("iat", StringType(), True),
    StructField("imap", StringType(), True),
    StructField("kpl", StringType(), True),
    StructField("maf", StringType(), True),
    StructField("rpm", StringType(), True),
    StructField("speed", StringType(), True),
    StructField("tAdv", StringType(), True),
    StructField("tPos", StringType(), True),
])
 
NUMERIC_DOUBLE_COLS = [
    "gps_speed", "battery", "cTemp", "dtc", "eLoad", "iat",
    "imap", "kpl", "maf", "rpm", "speed", "tAdv", "tPos",
]
 
raw_df = spark.read.schema(RAW_SCHEMA).json(f"s3a://{BUCKET}/{SOURCE_KEY}")
 
stats = {"input_rows": raw_df.count()}
 
# Drop exact full-row duplicates (verified: dupes are byte-identical
# across all columns, not distinct sub-second readings)
deduped = raw_df.dropDuplicates()
stats["after_dedup"] = deduped.count()
stats["exact_duplicates_removed"] = stats["input_rows"] - stats["after_dedup"]
 
# Filter malformed rows — e.g. embedded repeated CSV headers from
# concatenated source files (confirmed 33 occurrences in the full
# 3.1M-row dataset during local testing)
numeric_check = deduped.withColumn(
    "_tripID_valid", F.col("tripID").rlike(r"^\d+$")
).withColumn(
    "_deviceID_valid", F.col("deviceID").cast("double").isNotNull()
)
stats["malformed_rows_dropped"] = numeric_check.filter(
    (~F.col("_tripID_valid")) | (F.col("_deviceID_valid").isNull())
).count()
 
valid = numeric_check.filter(
    F.col("_tripID_valid") & F.col("_deviceID_valid")
).drop("_tripID_valid", "_deviceID_valid")
 
# Type casting — raw JSONL from Kafka has every field as a string
typed = valid \
    .withColumn("tripID", F.col("tripID").cast("int")) \
    .withColumn("deviceID", F.col("deviceID").cast("double").cast("int")) \
    .withColumn(
        "timeStamp",
        F.to_timestamp(F.col("timeStamp"), "yyyy-MM-dd HH:mm:ss")
    )
 
for c in NUMERIC_DOUBLE_COLS:
    typed = typed.withColumn(c, F.col(c).cast("double"))
 
# Drop rows where a cast produced a null in any critical column
critical_cols = ["tripID", "deviceID", "timeStamp"] + NUMERIC_DOUBLE_COLS
before_na_drop = typed.count()
cleaned_df = typed.dropna(subset=critical_cols)
stats["dropped_for_null_after_cast"] = before_na_drop - cleaned_df.count()
stats["output_rows"] = cleaned_df.count()
 
print("\n=== Cleaning stats ===")
for k, v in stats.items():
    print(f"{k}: {v:,}")
 
# COMMAND ----------

# COMMAND ----------

output_path = f"s3a://{BUCKET}/{PROCESSED_PREFIX}/"
cleaned_df.write.mode("overwrite").parquet(output_path)
 
print(f"\nWrote {stats['output_rows']:,} cleaned rows to {output_path}")
dbutils.notebook.exit("PROCESSED")

# COMMAND ----------