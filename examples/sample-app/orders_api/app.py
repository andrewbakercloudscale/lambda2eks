import json
import os

import boto3

TABLE = boto3.resource("dynamodb").Table(os.environ["TABLE_NAME"]) if os.environ.get("USE_DYNAMO") else None
_cache = {}


def handler(event, context):
    order_id = (event.get("pathParameters") or {}).get("id")
    if order_id is None:
        return {"statusCode": 200, "body": json.dumps({"orders": list(_cache.values())})}
    if TABLE is not None:
        item = TABLE.get_item(Key={"id": order_id}).get("Item")
    else:
        item = _cache.setdefault(order_id, {"id": order_id, "status": "new"})
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"order": item, "remaining_ms": context.get_remaining_time_in_millis()}),
    }
