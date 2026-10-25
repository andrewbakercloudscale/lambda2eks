"""The Python shim's bridge unwrapping and stream consumer, against in-memory fakes of SQS, Kinesis and DynamoDB."""

import base64
import json
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "lambda2eks" / "shims"))
import shim  # noqa: E402


class ConditionalCheckFailedException(Exception):
    pass


class FakeDdb:
    """Just enough of DynamoDB for the lease conditions the shim uses."""

    def __init__(self):
        self.items = {}

    def scan(self, TableName, ConsistentRead):
        return {"Items": [dict(v, shard={"S": k}) for k, v in self.items.items()]}

    def put_item(self, TableName, Item):
        self.items[Item["shard"]["S"]] = {k: v for k, v in Item.items() if k != "shard"}

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames):
        item = self.items.setdefault(Key["shard"]["S"], {})
        names, vals = ExpressionAttributeNames, ExpressionAttributeValues
        get = lambda n: item.get(names[n])  # noqa: E731
        me = vals[":me"]
        if ConditionExpression == "#o = :me":
            ok = get("#o") == me
        elif ConditionExpression == "attribute_not_exists(#o) OR #o = :me OR #e < :now":
            ok = get("#o") is None or get("#o") == me or float(get("#e")["N"]) < float(vals[":now"]["N"])
        else:
            raise AssertionError(ConditionExpression)
        if not ok:
            raise ConditionalCheckFailedException()
        for name, value in re.findall(r"(#\w+) = (:\w+)", UpdateExpression.removeprefix("SET ")):
            item[names[name]] = vals[value]
        return {}


class FakeKinesis:
    def __init__(self, shards):
        # shard id -> {"parent": id|None, "records": [...], "closed": bool}
        self.shards = shards

    def list_shards(self, StreamName):
        return {"Shards": [{"ShardId": k, **({"ParentShardId": v["parent"]} if v["parent"] else {})} for k, v in self.shards.items()]}

    def get_shard_iterator(self, StreamName, ShardId, ShardIteratorType, StartingSequenceNumber=None, Timestamp=None):
        recs = self.shards[ShardId]["records"]
        if ShardIteratorType == "AFTER_SEQUENCE_NUMBER":
            pos = next(i for i, r in enumerate(recs) if r["SequenceNumber"] == StartingSequenceNumber) + 1
        elif ShardIteratorType == "AT_TIMESTAMP":
            pos = next((i for i, r in enumerate(recs) if r["ApproximateArrivalTimestamp"].timestamp() >= Timestamp), len(recs))
        else:
            pos = 0 if ShardIteratorType == "TRIM_HORIZON" else len(recs)
        return {"ShardIterator": f"{ShardId}|{pos}"}

    def get_records(self, ShardIterator, Limit):
        shard_id, pos = ShardIterator.split("|")
        pos, sh = int(pos), self.shards[shard_id]
        out = sh["records"][pos:pos + Limit]
        nxt = pos + len(out)
        done = sh["closed"] and nxt >= len(sh["records"])
        return {"Records": out, "NextShardIterator": None if done else f"{shard_id}|{nxt}"}


def krec(seq, data, ts=1_700_000_000):
    return {"SequenceNumber": seq, "PartitionKey": "pk", "Data": data.encode(),
            "ApproximateArrivalTimestamp": datetime.fromtimestamp(ts, timezone.utc)}


def consumer(shards, handler, owner="a", ddb=None, **kw):
    ddb = ddb or FakeDdb()
    source = shim.KinesisSource(FakeKinesis(shards), "clicks", None)
    leases = shim.Leases(ddb, "leases", "clicks-consumer", owner)
    return shim.StreamConsumer(source, leases, shim.Invoker(handler, 1), batch_size=2, position="TRIM_HORIZON",
                               idle_wait=0.01, sleep=lambda s: None, **kw), ddb


def run_until(c, done, timeout=5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        c.balance()
        if done():
            return
        time.sleep(0.02)
    raise AssertionError("consumer did not finish")


def test_kinesis_parent_before_child_and_checkpoints():
    seen = []

    def handler(event, ctx):
        seen.extend((r["eventID"].split(":")[0], base64.b64decode(r["kinesis"]["data"]).decode()) for r in event["Records"])

    shards = {"shard-0": {"parent": None, "records": [krec("1", "a"), krec("2", "b"), krec("3", "c")], "closed": True},
              "shard-1": {"parent": "shard-0", "records": [krec("4", "d")], "closed": True}}
    c, ddb = consumer(shards, handler)
    run_until(c, lambda: all(ddb.items.get(f"clicks-consumer#{s}", {}).get("finished", {}).get("BOOL") for s in shards))
    assert seen == [("shard-0", "a"), ("shard-0", "b"), ("shard-0", "c"), ("shard-1", "d")]
    assert ddb.items["clicks-consumer#shard-0"]["checkpoint"] == {"S": "3"}
    assert ddb.items["clicks-consumer#shard-1"]["checkpoint"] == {"S": "4"}


def test_lambda_shaped_kinesis_record():
    source = shim.KinesisSource(None, "clicks", None)
    r = source.record(krec("7", "hi"), "shard-0")
    assert r["eventSource"] == "aws:kinesis" and r["eventID"] == "shard-0:7"
    assert r["eventSourceARN"].endswith(":stream/clicks") and r["kinesis"]["data"] == "aGk="
    assert r["kinesis"]["approximateArrivalTimestamp"] == 1_700_000_000


def test_partial_batch_failure_resumes_from_failed_record():
    calls = []

    def handler(event, ctx):
        seqs = [r["kinesis"]["sequenceNumber"] for r in event["Records"]]
        calls.append(seqs)
        if len(calls) == 1:
            return {"batchItemFailures": [{"itemIdentifier": "2"}]}
        return {"batchItemFailures": []}

    checkpoints = []
    c, _ = consumer({}, handler, partial=True)
    assert c.deliver([c.source.record(krec(s, s), "s") for s in ("1", "2")], checkpoints.append)
    assert calls == [["1", "2"], ["2"]] and checkpoints == ["1"]


def test_retries_run_out_then_records_are_skipped():
    calls = []

    def handler(event, ctx):
        calls.append(len(event["Records"]))
        raise RuntimeError("down")

    c, _ = consumer({}, handler, max_retry=1)
    assert c.deliver([c.source.record(krec(s, s), "s") for s in ("1", "2")], lambda s: None)
    assert calls == [2, 2]


def test_bisect_on_error_isolates_the_poison_record():
    calls = []

    def handler(event, ctx):
        data = [base64.b64decode(r["kinesis"]["data"]).decode() for r in event["Records"]]
        calls.append(data)
        if "poison" in data:
            raise ValueError("poison")

    c, _ = consumer({}, handler, max_retry=1, bisect=True)
    recs = [c.source.record(krec(str(i), d), "s") for i, d in enumerate(["a", "poison", "b", "c"])]
    assert c.deliver(recs, lambda s: None)
    assert calls == [["a", "poison", "b", "c"], ["a", "poison"], ["a"], ["poison"], ["poison"], ["b", "c"]]


def test_new_replica_gets_a_fair_share():
    shards = {f"shard-{i}": {"parent": None, "records": [], "closed": False} for i in range(4)}
    a, ddb = consumer(shards, lambda e, c: None, owner="a")
    b, _ = consumer(shards, lambda e, c: None, owner="b", ddb=ddb)
    try:
        assert len(a.balance()) == 4
        assert b.balance() == []          # everything is leased; b announces itself
        assert len(a.balance()) == 2      # a sees two members and gives two shards back
        assert len(b.balance()) == 2
        assert set(a.workers).isdisjoint(b.workers)
    finally:
        a.stopping.set()
        b.stopping.set()


def test_start_timestamp_skips_older_records():
    seen = []
    shards = {"shard-0": {"parent": None, "records": [krec("1", "old", 100), krec("2", "new", 300)], "closed": True}}
    c, ddb = consumer(shards, lambda e, ctx: seen.extend(r["kinesis"]["sequenceNumber"] for r in e["Records"]), start_ts=200)
    run_until(c, lambda: ddb.items.get("clicks-consumer#shard-0", {}).get("finished"))
    assert seen == ["2"]


def test_dynamodb_record_shape():
    source = shim.DynamoDBSource(None, "arn:aws:dynamodb:eu-west-1:1:table/stock/stream/2026")
    raw = {"eventID": "e1", "eventName": "INSERT", "eventVersion": "1.1", "eventSource": "aws:dynamodb", "awsRegion": "eu-west-1",
           "dynamodb": {"ApproximateCreationDateTime": datetime.fromtimestamp(5, timezone.utc), "Keys": {"id": {"S": "1"}},
                        "SequenceNumber": "9", "SizeBytes": 10, "StreamViewType": "NEW_IMAGE"}}
    r = source.record(raw, "s")
    assert r["dynamodb"]["ApproximateCreationDateTime"] == 5 and r["eventSourceARN"].endswith("/stream/2026")
    assert source.seq(r) == "9" and source.created(raw) == 5


# ------------------------------------------------------------------ S3 / SNS bridge queues

def test_unwrap_s3_and_test_event():
    body = json.dumps({"Records": [{"eventSource": "aws:s3", "s3": {"object": {"key": "a.csv"}}}]})
    assert shim.unwrap_message("s3", body)["Records"][0]["s3"]["object"]["key"] == "a.csv"
    assert shim.unwrap_message("s3", json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent"})) is None


def test_unwrap_sns_envelope():
    body = json.dumps({"Type": "Notification", "MessageId": "m", "TopicArn": "arn:aws:sns:eu-west-1:1:alerts", "Subject": None,
                       "Message": "hello", "Timestamp": "2026-01-01T00:00:00.000Z", "SignatureVersion": "1", "Signature": "x",
                       "SigningCertURL": "https://c", "UnsubscribeURL": "https://u",
                       "MessageAttributes": {"level": {"Type": "String", "Value": "high"}}})
    rec = shim.unwrap_message("sns", body, "arn:aws:sns:eu-west-1:1:alerts:sub")["Records"][0]
    assert rec["EventSource"] == "aws:sns" and rec["EventSubscriptionArn"].endswith(":sub")
    assert rec["Sns"]["Message"] == "hello" and rec["Sns"]["SigningCertUrl"] == "https://c"
    assert rec["Sns"]["MessageAttributes"] == {"level": {"Type": "String", "Value": "high"}}


@pytest.mark.parametrize("bodies, deleted", [
    (["ok", "fail", "{not json"], ["r0"]),
    ([json.dumps({"Service": "Amazon S3", "Event": "s3:TestEvent"})], ["r0"]),
])
def test_bridged_messages_are_invoked_one_at_a_time(bodies, deleted):
    events = []

    def handler(event, ctx):
        events.append(event)
        if event["Records"][0]["key"] == "fail":
            raise RuntimeError("x")

    class FakeSQS:
        def __init__(self):
            self.deleted = []

        def delete_message_batch(self, QueueUrl, Entries):
            self.deleted += [e["ReceiptHandle"] for e in Entries]
            return {}

    def body(b):
        return b if b.startswith(("{", "[")) else json.dumps({"Records": [{"key": b}]})

    records = [shim.sqs_record({"MessageId": f"m{i}", "ReceiptHandle": f"r{i}", "Body": body(b)}, "arn") for i, b in enumerate(bodies)]
    sqs = FakeSQS()
    shim.process_bridged(sqs, "url", shim.Invoker(handler, 1), records, "s3")
    assert sqs.deleted == deleted
    assert all(len(e["Records"]) == 1 for e in events)
