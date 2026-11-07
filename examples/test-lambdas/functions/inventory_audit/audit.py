"""DynamoDB stream consumer: writes an audit row per change. A sku starting with BAD- fails (partial batch)."""

import os

import boto3

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def handler(event, context):
    failures = []
    for record in event["Records"]:
        ddb = record["dynamodb"]
        sku = ddb["Keys"]["sku"]["S"]
        try:
            if sku.startswith("BAD-"):
                raise ValueError(f"cannot audit {sku}")
            new = ddb.get("NewImage") or {}
            RESULTS.put_item(Item={"fn": "inventory-audit", "id": f"{sku}#{ddb['SequenceNumber']}", "sku": sku,
                                   "change": record["eventName"], "stock": new.get("stock", {}).get("N", "-")})
        except Exception as e:  # noqa: BLE001
            print(f"record {ddb['SequenceNumber']} failed: {e}")
            failures.append({"itemIdentifier": ddb["SequenceNumber"]})
    return {"batchItemFailures": failures}
