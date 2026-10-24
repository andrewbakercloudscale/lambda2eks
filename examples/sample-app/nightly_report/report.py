import json


def lambda_handler(event, context):
    print(json.dumps({"generating": event.get("report"), "request": context.aws_request_id}))
    return {"ok": True}
