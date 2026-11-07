"""EventBridge event-pattern rule (console sign-ins). lambda2eks reports this trigger instead of migrating it."""

import os

import boto3

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def handler(event, context):
    detail = event.get("detail", {})
    RESULTS.put_item(Item={"fn": "audit-shipper", "id": event.get("id", context.aws_request_id),
                           "user": detail.get("userIdentity", {}).get("arn", "unknown"),
                           "result": detail.get("responseElements", {}).get("ConsoleLogin", "unknown")})
    return {"shipped": 1}
