"""EventBridge schedule: receives the rule's constant input and records the run."""

import os
import time

import boto3

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def lambda_handler(event, context):
    if event.get("task") != "expire-todos":
        raise ValueError(f"unexpected input {event!r}")
    run = {"fn": "nightly-cleanup", "id": context.aws_request_id, "task": event["task"],
           "older_than_days": int(event["older_than_days"]), "at": int(time.time())}
    RESULTS.put_item(Item=run)
    return run
