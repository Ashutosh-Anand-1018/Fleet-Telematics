# Databricks notebook source
# MAGIC %md
# MAGIC ### UC-2: Driver Behavior & Fuel Efficiency Analytics
# MAGIC Reads cleaned data from S3 processed/, detects driver behavior events
# MAGIC (aggressive driving, hard braking, high-RPM driving, poor fuel
# MAGIC efficiency, rapid acceleration), and produces per-trip and
# MAGIC per-driver(device) scores for the dashboard.
# MAGIC
# MAGIC Thresholds below are evidence-based, derived from profiling the real
# MAGIC 3.1M-row dataset (see project docs for the percentile analysis):
# MAGIC   - High RPM       : rpm > 2000        (~p95 of rpm distribution)
# MAGIC   - Rapid accel     : Δspeed > 5 km/h/s (~p99 of positive deltas)
# MAGIC   - Hard braking    : Δspeed < -6 km/h/s (~p01 of deltas)
# MAGIC   - Poor efficiency : kpl < 1.2 while moving (~p25 of kpl when speed>0)

# COMMAND ----------

# ============================================================
# SECTION 1 — S3 credentials (same cross-account setup as shared_cleaning)
# ============================================================

import boto3
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=AWS_REGION,
)

print("S3 credentials configured for UC-2 notebook.")

# COMMAND ----------

# ============================================================
# SECTION 2 — Auto-detect the latest processed/ batch
# ============================================================

# Lists everything under processed/, extracts the datestamp from each
# folder name, and picks the most recent one automatically — no manual
# widget entry needed. Matches the pattern real Step Functions
# orchestration would use, just done client-side here for testing.

import re

response = s3_client.list_objects_v2(
    Bucket=BUCKET, Prefix="processed/vehicle_telematics_processed_", Delimiter="/"
)

candidates = []
for prefix_obj in response.get("CommonPrefixes", []):
    prefix = prefix_obj["Prefix"]
    match = re.search(r"(\d{8}_\d{6})", prefix)
    if match:
        candidates.append((match.group(1), prefix))

if not candidates:
    raise ValueError(
        "No processed/ batches found in S3. Run shared_cleaning.py first."
    )

candidates.sort(key=lambda x: x[0], reverse=True)  # datestamp format sorts lexicographically = chronologically
DATESTAMP, PROCESSED_PREFIX = candidates[0]
PROCESSED_PREFIX = PROCESSED_PREFIX.rstrip("/")

print(f"Found {len(candidates)} processed batch(es). Using the latest:")
print(f"  Datestamp : {DATESTAMP}")
print(f"  Prefix    : {PROCESSED_PREFIX}")

input_path = f"s3a://{BUCKET}/{PROCESSED_PREFIX}/"

# COMMAND ----------

# ============================================================
# SECTION 3 — Load cleaned data + compute derived features
# ============================================================

df = spark.read.parquet(input_path)

# Row-to-row speed delta within each trip, ordered by time.
# This is THE key derived feature for hard braking / rapid acceleration
# detection — not part of the shared cleaning step since it's specific
# to UC-2's business logic, not general data quality.
trip_window = Window.partitionBy("tripID", "deviceID").orderBy("timeStamp")

df = df.withColumn("prev_speed", F.lag("speed").over(trip_window))
df = df.withColumn("speed_delta", F.col("speed") - F.col("prev_speed"))

# COMMAND ----------

# ============================================================
# SECTION 4 — Per-row event detection
# ============================================================

HIGH_RPM_THRESHOLD = 2000
RAPID_ACCEL_THRESHOLD = 5      # km/h increase in 1 second
HARD_BRAKE_THRESHOLD = -6      # km/h decrease in 1 second
POOR_KPL_THRESHOLD = 1.2

df = df \
    .withColumn("is_high_rpm", F.col("rpm") > HIGH_RPM_THRESHOLD) \
    .withColumn("is_rapid_accel", F.col("speed_delta") > RAPID_ACCEL_THRESHOLD) \
    .withColumn("is_hard_brake", F.col("speed_delta") < HARD_BRAKE_THRESHOLD) \
    .withColumn(
        "is_poor_efficiency",
        (F.col("kpl") < POOR_KPL_THRESHOLD) & (F.col("speed") > 0)
    )

# Aggressive driving: 2+ risk signals firing on the same row
risk_signal_count = (
    F.col("is_high_rpm").cast("int") +
    F.col("is_rapid_accel").cast("int") +
    F.col("is_hard_brake").cast("int")
)
df = df.withColumn("is_aggressive_event", risk_signal_count >= 2)

event_counts = df.select(
    F.sum(F.col("is_high_rpm").cast("int")).alias("high_rpm_events"),
    F.sum(F.col("is_rapid_accel").cast("int")).alias("rapid_accel_events"),
    F.sum(F.col("is_hard_brake").cast("int")).alias("hard_brake_events"),
    F.sum(F.col("is_poor_efficiency").cast("int")).alias("poor_efficiency_rows"),
    F.sum(F.col("is_aggressive_event").cast("int")).alias("aggressive_events"),
    F.count("*").alias("total_rows"),
).collect()[0]

print("=== Event counts across dataset ===")
for k in event_counts.asDict():
    print(f"{k}: {event_counts[k]:,}")

# COMMAND ----------

# ============================================================
# SECTION 5 — Per-driver (deviceID) aggregation and scoring
# ============================================================

driver_agg = df.groupBy("deviceID").agg(
    F.count("*").alias("total_readings"),
    F.countDistinct("tripID").alias("total_trips"),
    F.sum(F.col("is_high_rpm").cast("int")).alias("high_rpm_count"),
    F.sum(F.col("is_rapid_accel").cast("int")).alias("rapid_accel_count"),
    F.sum(F.col("is_hard_brake").cast("int")).alias("hard_brake_count"),
    F.sum(F.col("is_aggressive_event").cast("int")).alias("aggressive_event_count"),
    F.avg(F.when(F.col("speed") > 0, F.col("kpl"))).alias("avg_kpl_while_moving"),
    F.avg("speed").alias("avg_speed"),
    F.avg("rpm").alias("avg_rpm"),
)

# Driver Safety Score: 100 minus a penalty per risky event per 100 readings
# (normalized so drivers with more data aren't unfairly penalized just for
# having more readings)
driver_agg = driver_agg.withColumn(
    "events_per_100_readings",
    (F.col("aggressive_event_count") / F.col("total_readings")) * 100
)
driver_agg = driver_agg.withColumn(
    "driver_safety_score",
    F.greatest(
        F.lit(0.0),
        F.lit(100.0) - (F.col("events_per_100_readings") * 5)
    )
)

# Fuel Efficiency Score: scaled 0-100 based on avg kpl relative to the
# dataset's p90 kpl (~12.14 from profiling) as a "good" reference point
KPL_REFERENCE = 12.14
driver_agg = driver_agg.withColumn(
    "fuel_efficiency_score",
    F.least(
        F.lit(100.0),
        (F.col("avg_kpl_while_moving") / F.lit(KPL_REFERENCE)) * 100
    )
)

# Driver Category
driver_agg = driver_agg.withColumn(
    "driver_category",
    F.when(F.col("driver_safety_score") >= 80, "Eco Driver")
     .when(F.col("driver_safety_score") >= 50, "Normal Driver")
     .otherwise("Aggressive Driver")
)

# Driver Ranking (by safety score, descending)
rank_window = Window.orderBy(F.desc("driver_safety_score"))
driver_agg = driver_agg.withColumn("driver_rank", F.row_number().over(rank_window))

print("\n=== Driver Performance Summary ===")
driver_agg.select(
    "deviceID", "driver_rank", "driver_category",
    "driver_safety_score", "fuel_efficiency_score",
    "total_trips", "aggressive_event_count"
).orderBy("driver_rank").show(20, truncate=False)

# COMMAND ----------

# ============================================================
# SECTION 6 — Write outputs to S3 (both curated/uc2/ and reports/driver_behavior/)
# ============================================================

# curated/uc2/ — business-ready curated dataset for this use case
curated_path = f"s3a://{BUCKET}/curated/uc2/driver_performance_{DATESTAMP}/"
driver_agg.write.mode("overwrite").parquet(curated_path)
print(f"Wrote curated output to {curated_path}")

# reports/driver_behavior/ — PDF-mandated output path for UC-2
report_path = f"s3a://{BUCKET}/reports/driver_behavior/driver_performance_{DATESTAMP}/"
driver_agg.write.mode("overwrite").parquet(report_path)
print(f"Wrote report output to {report_path}")

dbutils.notebook.exit("UC2_PROCESSED")
