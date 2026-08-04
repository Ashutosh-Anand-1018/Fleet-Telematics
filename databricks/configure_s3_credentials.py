# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC ### S3 credentials setup (cross-account access)
# MAGIC This cluster isn't in the same AWS account as the S3 bucket, so we
# MAGIC authenticate explicitly using an IAM user's keys instead of an
# MAGIC instance profile. Keys are pulled from a Databricks secret scope,
# MAGIC never hardcoded.
 
# COMMAND ----------
 
import boto3
 
SECRET_SCOPE = "fleet-telematics"
AWS_REGION = "ap-south-1"
BUCKET = "capstone-fleet-telematics"
 
# COMMAND ----------

# COMMAND ----------

aws_access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-access-key")
aws_secret_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-secret-key")
 
print("Credentials loaded from secret scope.")

# COMMAND ----------

# Configure Spark's s3a:// filesystem.
# Use s3a:// (not s3://) in every read/write path from now on —
# s3a:// is what respects these explicit keys. Databricks' native
# s3:// scheme expects an instance profile, which doesn't apply here.
spark.conf.set("fs.s3a.access.key", aws_access_key)
spark.conf.set("fs.s3a.secret.key", aws_secret_key)
spark.conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
spark.conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
 
print("Spark s3a:// filesystem configured.")
 
# COMMAND ----------

# COMMAND ----------

# Configure boto3 for direct S3 API calls (e.g. idempotency checks,
# listing objects) — separate from Spark's filesystem config above.
s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=AWS_REGION,
)
 
print("boto3 S3 client configured.")

# COMMAND ----------

# COMMAND ----------

# Verify everything actually works before moving on to any real job.
try:
    s3_client.head_bucket(Bucket=BUCKET)
    print(f"S3 credentials verified — successfully reached bucket: {BUCKET}")
except Exception as e:
    print(f"S3 credential check FAILED: {e}")
    raise
 
# COMMAND ----------
 
# Quick sanity check — list the top-level folders to confirm structure
response = s3_client.list_objects_v2(Bucket=BUCKET, Delimiter="/")
prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
print("Top-level folders found:")
for p in prefixes:
    print(f"  {p}")