import json
import os
import urllib.request

INDEX_URL = os.environ["SEARCH_INDEX_URL"]


def handler(event, context):
    failures = []
    for record in event["Records"]:
        try:
            keys = record["dynamodb"]["Keys"]
            image = record["dynamodb"].get("NewImage")
            body = json.dumps({"sku": keys["sku"]["S"], "deleted": record["eventName"] == "REMOVE", "item": image}).encode()
            req = urllib.request.Request(f"{INDEX_URL}/{keys['sku']['S']}", data=body, method="PUT",
                                         headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=5).close()
        except Exception as e:  # noqa: BLE001
            print(f"failed {record['dynamodb']['SequenceNumber']}: {e}")
            failures.append({"itemIdentifier": record["dynamodb"]["SequenceNumber"]})
    return {"batchItemFailures": failures}
