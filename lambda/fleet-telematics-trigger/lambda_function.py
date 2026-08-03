import json


def lambda_handler(event, context):
    """
    Future Responsibility:
    - Start an AWS Step Functions execution.
    """
    print("Fleet Telematics Lambda Triggered")

    print("\nReceived Event:\n")
    print(json.dumps(event, indent=4))

    try:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        object_key = record["s3"]["object"]["key"]

        print("\nS3 Event Details")
        print("---------------------------")
        print(f"Bucket : {bucket_name}")
        print(f"Object : {object_key}")

    except (KeyError, IndexError) as e:
        print(f"Unable to parse S3 Event : {e}")

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Lambda executed successfully."
            }
        )
    }