"""SNS subscriber with a filter policy (severity high|critical). Records each alert it receives."""

import json
import os

import boto3

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def handler(event, context):
    for record in event["Records"]:
        sns = record["Sns"]
        alert = json.loads(sns["Message"])
        severity = sns["MessageAttributes"]["severity"]["Value"]
        RESULTS.put_item(Item={"fn": "alert-router", "id": sns["MessageId"], "alert": alert["id"],
                               "severity": severity, "topic": sns["TopicArn"]})
    return {"routed": len(event["Records"])}
