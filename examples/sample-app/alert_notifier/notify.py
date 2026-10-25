import json
import os
import urllib.request


def handler(event, context):
    for record in event["Records"]:
        alert = json.loads(record["Sns"]["Message"])
        text = f"[{alert['severity'].upper()}] {alert['title']}"
        req = urllib.request.Request(os.environ["SLACK_WEBHOOK_URL"], data=json.dumps({"text": text}).encode(),
                                     headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5).close()
