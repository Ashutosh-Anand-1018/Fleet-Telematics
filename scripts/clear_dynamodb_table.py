"""
Deletes all items from a DynamoDB table while keeping the table itself
(structure, key schema, settings) intact.

Useful between test runs without needing to delete/recreate the table.

Run:
    python scripts/clear_dynamodb_table.py --table DriverScore
    python scripts/clear_dynamodb_table.py --table VehicleAlerts
"""

import argparse

import boto3

AWS_REGION = "ap-south-1"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", required=True, help="e.g. DriverScore or VehicleAlerts")
    args = parser.parse_args()

    session = boto3.Session(region_name=AWS_REGION)
    dynamodb = session.resource("dynamodb")
    table = dynamodb.Table(args.table)

    response = table.scan()
    items = response.get("Items", [])

    if not items:
        print(f"Table '{args.table}' is already empty. Nothing to delete.")
        return

    # Dynamically read the partition key name from the table itself, so
    # this works regardless of whether the key is deviceID or something
    # else, and regardless of String vs Number type.
    key_schema = table.key_schema
    key_names = [k["AttributeName"] for k in key_schema]

    with table.batch_writer() as batch:
        for item in items:
            key = {k: item[k] for k in key_names}
            batch.delete_item(Key=key)

    print(f"Deleted {len(items)} items from '{args.table}'. Table structure kept intact.")


if __name__ == "__main__":
    main()
