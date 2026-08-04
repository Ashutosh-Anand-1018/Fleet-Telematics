
 
import boto3
 
SECRET_SCOPE = "fleet-telematics"
AWS_REGION = "ap-south-1"
BUCKET = "capstone-fleet-telematics"
 
# COMMAND ----------

# COMMAND ----------

aws_access_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-access-key")
aws_secret_key = dbutils.secrets.get(scope=SECRET_SCOPE, key="aws-secret-key")
 
print("Credentials loaded from secret scope.")


spark.conf.set("fs.s3a.access.key", aws_access_key)
spark.conf.set("fs.s3a.secret.key", aws_secret_key)
spark.conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
spark.conf.set("fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
 
print("Spark s3a:// filesystem configured.")
 
# COMMAND ----------

# COMMAND ----------


s3_client = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=AWS_REGION,
)
 
print("boto3 S3 client configured.")

# COMMAND ----------

# COMMAND ----------

try:
    s3_client.head_bucket(Bucket=BUCKET)
    print(f"S3 credentials verified — successfully reached bucket: {BUCKET}")
except Exception as e:
    print(f"S3 credential check FAILED: {e}")
    raise
 
# COMMAND ----------
 

response = s3_client.list_objects_v2(Bucket=BUCKET, Delimiter="/")
prefixes = [p["Prefix"] for p in response.get("CommonPrefixes", [])]
print("Top-level folders found:")
for p in prefixes:
    print(f"  {p}")