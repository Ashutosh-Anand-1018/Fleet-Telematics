# Databricks notebook source
# MAGIC %md
# MAGIC ### UC-1: Vehicle Health Monitoring & Predictive Maintenance
# MAGIC Reads cleaned data from S3 processed/, detects vehicle health events
# MAGIC (high RPM, engine overheating, excessive engine load, low battery
# MAGIC voltage, frequent DTC events), and produces per-vehicle health
# MAGIC scores for the dashboard.
# MAGIC
# MAGIC Thresholds below are evidence-based, derived from profiling the real
# MAGIC 3.1M-row dataset:
# MAGIC   - High RPM        : rpm > 2000       (~p95 of rpm distribution)
# MAGIC   - Overheating      : cTemp > 91, excluding cTemp=0 (engine off)  (~p90-p95)
# MAGIC   - Excessive load   : eLoad > 81, excluding eLoad=0 (idle)         (~p95)
# MAGIC   - Low battery      : battery < 12.0V, excluding battery=0 (sensor inactive)
# MAGIC                        (standard 12V automotive convention, also matches
# MAGIC                        bottom ~0.3% of real non-zero readings)
# MAGIC   - Frequent DTC     : dtc > 0 — NOTE: across the full 3.1M-row dataset,
# MAGIC                        DTC codes fire in only 71 rows (0.002%). The
# MAGIC                        detection logic is correct, but this dataset barely
# MAGIC                        exercises it — worth flagging honestly rather than
# MAGIC                        overstating how "frequent" these events actually are
# MAGIC                        in this specific data.

# COMMAND ----------

# ============================================================
# SECTION 1 — S3 credentials (cross-account setup)
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
    "s3", aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key, region_name=AWS_REGION,
)

print("S3 credentials configured for UC-1 notebook.")

# COMMAND ----------

# ============================================================
# SECTION 2 — Auto-detect the latest processed/ batch
# ============================================================

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
    raise ValueError("No processed/ batches found in S3. Run shared_cleaning.py first.")

candidates.sort(key=lambda x: x[0], reverse=True)
DATESTAMP, PROCESSED_PREFIX = candidates[0]
PROCESSED_PREFIX = PROCESSED_PREFIX.rstrip("/")

print(f"Found {len(candidates)} processed batch(es). Using the latest:")
print(f"  Datestamp : {DATESTAMP}")
print(f"  Prefix    : {PROCESSED_PREFIX}")

input_path = f"s3a://{BUCKET}/{PROCESSED_PREFIX}/"

# COMMAND ----------

# ============================================================
# SECTION 3 — Load cleaned data
# ============================================================

df = spark.read.parquet(input_path)
print(f"Rows loaded: {df.count():,}")

# COMMAND ----------

# ============================================================
# SECTION 4 — Per-row event detection
# ============================================================

HIGH_RPM_THRESHOLD = 2000
OVERHEAT_THRESHOLD = 91
EXCESSIVE_LOAD_THRESHOLD = 81
LOW_BATTERY_THRESHOLD = 12.0

df = df \
    .withColumn("is_high_rpm", F.col("rpm") > HIGH_RPM_THRESHOLD) \
    .withColumn(
        "is_overheating",
        (F.col("cTemp") > OVERHEAT_THRESHOLD) & (F.col("cTemp") > 0)
    ) \
    .withColumn(
        "is_excessive_load",
        (F.col("eLoad") > EXCESSIVE_LOAD_THRESHOLD) & (F.col("eLoad") > 0)
    ) \
    .withColumn(
        "is_low_battery",
        (F.col("battery") < LOW_BATTERY_THRESHOLD) & (F.col("battery") > 0)
    ) \
    .withColumn("is_dtc_event", F.col("dtc") > 0)

# Any of the 5 signals firing counts as a "health alert" row
alert_signal_count = (
    F.col("is_high_rpm").cast("int") +
    F.col("is_overheating").cast("int") +
    F.col("is_excessive_load").cast("int") +
    F.col("is_low_battery").cast("int") +
    F.col("is_dtc_event").cast("int")
)
df = df.withColumn("has_health_alert", alert_signal_count >= 1)

event_counts = df.select(
    F.coalesce(F.sum(F.col("is_high_rpm").cast("int")), F.lit(0)).alias("high_rpm_events"),
    F.coalesce(F.sum(F.col("is_overheating").cast("int")), F.lit(0)).alias("overheating_events"),
    F.coalesce(F.sum(F.col("is_excessive_load").cast("int")), F.lit(0)).alias("excessive_load_events"),
    F.coalesce(F.sum(F.col("is_low_battery").cast("int")), F.lit(0)).alias("low_battery_events"),
    F.coalesce(F.sum(F.col("is_dtc_event").cast("int")), F.lit(0)).alias("dtc_events"),
    F.coalesce(F.sum(F.col("has_health_alert").cast("int")), F.lit(0)).alias("total_alert_rows"),
    F.count("*").alias("total_rows"),
).collect()[0]

print("=== Event counts across dataset ===")
for k in event_counts.asDict():
    v = event_counts[k]
    print(f"{k}: {v:,}" if v is not None else f"{k}: {v}")

# COMMAND ----------

# ============================================================
# SECTION 5 — Per-vehicle (deviceID) aggregation and scoring
# ============================================================

vehicle_agg = df.groupBy("deviceID").agg(
    F.count("*").alias("total_readings"),
    F.countDistinct("tripID").alias("total_trips"),
    F.coalesce(F.sum(F.col("is_high_rpm").cast("int")), F.lit(0)).alias("high_rpm_count"),
    F.coalesce(F.sum(F.col("is_overheating").cast("int")), F.lit(0)).alias("overheating_count"),
    F.coalesce(F.sum(F.col("is_excessive_load").cast("int")), F.lit(0)).alias("excessive_load_count"),
    F.coalesce(F.sum(F.col("is_low_battery").cast("int")), F.lit(0)).alias("low_battery_count"),
    F.coalesce(F.sum(F.col("is_dtc_event").cast("int")), F.lit(0)).alias("dtc_count"),
    F.coalesce(F.sum(F.col("has_health_alert").cast("int")), F.lit(0)).alias("total_alert_count"),
    F.avg("cTemp").alias("avg_cTemp"),
    F.avg("eLoad").alias("avg_eLoad"),
)

# Vehicle Health Score: 100 minus a penalty per alert event per 100
# readings (normalized so vehicles with more data aren't unfairly
# penalized just for having more readings)
vehicle_agg = vehicle_agg.withColumn(
    "alerts_per_100_readings",
    (F.col("total_alert_count") / F.col("total_readings")) * 100
)
vehicle_agg = vehicle_agg.withColumn(
    "vehicle_health_score",
    F.greatest(
        F.lit(0.0),
        F.lit(100.0) - (F.col("alerts_per_100_readings") * 3)
    )
)

# High Risk flag
vehicle_agg = vehicle_agg.withColumn(
    "is_high_risk",
    F.col("vehicle_health_score") < 60
)

# Maintenance Recommendation — based on which issue is most frequent
# for that vehicle (simple rule-based approach, not ML-based prioritization)
vehicle_agg = vehicle_agg.withColumn(
    "maintenance_recommendation",
    F.when(F.col("dtc_count") > 0, "Diagnostic scan required — active trouble codes detected")
     .when(F.col("low_battery_count") > F.col("total_readings") * 0.01, "Battery inspection/replacement recommended")
     .when(F.col("overheating_count") > F.col("total_readings") * 0.05, "Cooling system inspection recommended")
     .when(F.col("excessive_load_count") > F.col("total_readings") * 0.05, "Engine load / transmission check recommended")
     .when(F.col("high_rpm_count") > F.col("total_readings") * 0.05, "Driver coaching — frequent high-RPM operation")
     .otherwise("No immediate maintenance action indicated")
)

# Vehicle Ranking (by health score, ascending — worst first, since
# that's what a fleet manager actually wants to see first)
rank_window = Window.orderBy(F.asc("vehicle_health_score"))
vehicle_agg = vehicle_agg.withColumn("risk_rank", F.row_number().over(rank_window))

print("\n=== Vehicle Health Summary ===")
vehicle_agg.select(
    "deviceID", "risk_rank", "is_high_risk", "vehicle_health_score",
    "total_alert_count", "maintenance_recommendation"
).orderBy("risk_rank").show(20, truncate=False)

# COMMAND ----------

# ============================================================
# SECTION 6 — Write outputs to S3 (curated/uc1/ and reports/vehicle_health/)
# ============================================================

curated_path = f"s3a://{BUCKET}/curated/uc1/vehicle_health_{DATESTAMP}/"
vehicle_agg.write.mode("overwrite").parquet(curated_path)
print(f"Wrote curated output to {curated_path}")

report_path = f"s3a://{BUCKET}/reports/vehicle_health/vehicle_health_{DATESTAMP}/"
vehicle_agg.write.mode("overwrite").parquet(report_path)
print(f"Wrote report output to {report_path}")

dbutils.notebook.exit("UC1_PROCESSED")
