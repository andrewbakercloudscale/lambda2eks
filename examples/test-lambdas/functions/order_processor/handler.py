"""SQS consumer with ReportBatchItemFailures. An order with qty <= 0 fails and is retried, then dead-lettered."""

import json
import os

import boto3
from botocore.exceptions import ClientError

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def process(order):
    if int(order.get("qty", 0)) <= 0:
        raise ValueError(f"order {order.get('order_id')} has no quantity")
    total = round(int(order["qty"]) * float(order["unit_price"]), 2)
    try:
        # Keyed on order_id and conditional, so a redelivered message is not counted twice.
        RESULTS.put_item(
            Item={"fn": "order-processor", "id": order["order_id"], "total": str(total)},
            ConditionExpression="attribute_not_exists(id)",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise


def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            process(json.loads(record["body"]))
        except Exception as e:  # noqa: BLE001 - one bad message must not fail the batch
            print(f"message {record['messageId']} failed: {e}")
            failures.append({"itemIdentifier": record["messageId"]})
    return {"batchItemFailures": failures}
