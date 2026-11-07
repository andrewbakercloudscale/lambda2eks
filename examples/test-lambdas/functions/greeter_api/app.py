"""HTTP API (payload 2.0): GET /greet/{name} answers, POST /greet records a signature."""

import json
import os
import time
import uuid

import boto3

RESULTS = boto3.resource("dynamodb").Table(os.environ["RESULTS_TABLE"])


def _response(status, body):
    return {"statusCode": status, "headers": {"content-type": "application/json"}, "body": json.dumps(body)}


def handler(event, context):
    method = event["requestContext"]["http"]["method"]
    if method == "GET":
        name = event["pathParameters"]["name"]
        lang = (event.get("queryStringParameters") or {}).get("lang", "en")
        greeting = {"en": "Hello", "fr": "Bonjour", "af": "Hallo"}.get(lang, "Hello")
        return _response(200, {"message": f"{greeting}, {name}!", "request_id": context.aws_request_id})
    body = json.loads(event.get("body") or "{}")
    if not body.get("name"):
        return _response(400, {"error": "name is required"})
    item = {"fn": "greeter-api", "id": body.get("id") or str(uuid.uuid4()), "name": body["name"], "at": int(time.time())}
    RESULTS.put_item(Item=item)
    return _response(201, item)
