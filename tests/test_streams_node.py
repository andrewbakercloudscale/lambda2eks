"""The Node shim's bridge unwrapping and stream consumer, against in-memory fakes run in a Node script."""

import json
import os
import subprocess
from pathlib import Path

import pytest

SHIM = (Path(__file__).resolve().parent.parent / "lambda2eks" / "shims" / "shim.mjs").as_uri()

pytestmark = pytest.mark.skipif(not os.popen("which node").read(), reason="node missing")

SCRIPT = """
import { Invoker, Leases, KinesisSource, StreamConsumer, unwrapMessage, processBridged } from SHIM;

function fakeDdb() {
  const items = new Map();
  const call = async (op, input) => {
    if (op === 'Scan') return { Items: [...items].map(([k, v]) => ({ ...v, shard: { S: k } })) };
    if (op === 'PutItem') { const { shard, ...rest } = input.Item; items.set(shard.S, rest); return {}; }
    const key = input.Key.shard.S;
    const item = items.get(key) || {};
    const get = (n) => item[input.ExpressionAttributeNames[n]];
    const v = input.ExpressionAttributeValues;
    const ok = input.ConditionExpression === '#o = :me' ? get('#o')?.S === v[':me'].S
      : !get('#o') || get('#o').S === v[':me'].S || Number(get('#e').N) < Number(v[':now'].N);
    if (!ok) { const e = new Error('cond'); e.name = 'ConditionalCheckFailedException'; throw e; }
    for (const [, n, val] of input.UpdateExpression.matchAll(/(#\\w+) = (:\\w+)/g)) item[input.ExpressionAttributeNames[n]] = v[val];
    items.set(key, item);
    return {};
  };
  return { call, items };
}

function fakeKinesis(shards) {
  return async (op, input) => {
    if (op === 'ListShards') return { Shards: Object.entries(shards).map(([id, s]) => ({ ShardId: id, ParentShardId: s.parent })) };
    if (op === 'GetShardIterator') {
      const recs = shards[input.ShardId].records;
      const pos = input.ShardIteratorType === 'AFTER_SEQUENCE_NUMBER'
        ? recs.findIndex((r) => r.SequenceNumber === input.StartingSequenceNumber) + 1
        : input.ShardIteratorType === 'TRIM_HORIZON' ? 0 : recs.length;
      return { ShardIterator: `${input.ShardId}|${pos}` };
    }
    const [id, p] = input.ShardIterator.split('|');
    const pos = Number(p), s = shards[id];
    const out = s.records.slice(pos, pos + input.Limit);
    const next = pos + out.length;
    return { Records: out, NextShardIterator: s.closed && next >= s.records.length ? null : `${id}|${next}` };
  };
}

const rec = (seq, data) => ({ SequenceNumber: seq, PartitionKey: 'pk', Data: new TextEncoder().encode(data), ApproximateArrivalTimestamp: new Date(1e12) });
const make = (shards, handler, owner, ddb, opts = {}) => new StreamConsumer(
  new KinesisSource(fakeKinesis(shards), 'clicks'), new Leases(ddb.call, 'leases', 'c', owner), new Invoker(handler, 1),
  { batchSize: 2, position: 'TRIM_HORIZON', idleWait: 0.01, sleep: async () => {}, ...opts });

const out = {};

// parent shard before child, checkpoints, finished flags
{
  const seen = [];
  const shards = { 'shard-1': { parent: 'shard-0', records: [rec('4', 'd')], closed: true },
                   'shard-0': { parent: null, records: [rec('1', 'a'), rec('2', 'b'), rec('3', 'c')], closed: true } };
  const ddb = fakeDdb();
  const c = make(shards, async (e) => { seen.push(...e.Records.map((r) => r.eventID)); }, 'a', ddb);
  for (let i = 0; i < 200 && !['shard-0', 'shard-1'].every((s) => ddb.items.get(`c#${s}`)?.finished?.BOOL); i++) {
    await c.balance();
    await new Promise((r) => setTimeout(r, 5));
  }
  out.order = seen;
  out.checkpoints = ['shard-0', 'shard-1'].map((s) => ddb.items.get(`c#${s}`).checkpoint.S);
  out.data = Buffer.from(c.source.record(rec('9', 'hi'), 's').kinesis.data, 'base64').toString();
}

// partial batch failure resumes from the failed record
{
  const calls = [];
  const c = make({}, async (e) => {
    calls.push(e.Records.map((r) => r.kinesis.sequenceNumber));
    return { batchItemFailures: calls.length === 1 ? [{ itemIdentifier: '2' }] : [] };
  }, 'a', fakeDdb(), { partial: true });
  const cps = [];
  await c.deliver([rec('1', 'x'), rec('2', 'y')].map((r) => c.source.record(r, 's')), async (s) => cps.push(s));
  out.partial = { calls, cps };
}

// bisect isolates a poison record; retries run out
{
  const calls = [];
  const c = make({}, async (e) => {
    const d = e.Records.map((r) => Buffer.from(r.kinesis.data, 'base64').toString());
    calls.push(d);
    if (d.includes('poison')) throw new Error('poison');
  }, 'a', fakeDdb(), { maxRetry: 1, bisect: true });
  await c.deliver(['a', 'poison', 'b', 'c'].map((d, i) => c.source.record(rec(String(i), d), 's')), async () => {});
  out.bisect = calls;
}

// a new replica gets a fair share
{
  const shards = Object.fromEntries([0, 1, 2, 3].map((i) => [`shard-${i}`, { parent: null, records: [], closed: false }]));
  const ddb = fakeDdb();
  const a = make(shards, async () => {}, 'a', ddb);
  const b = make(shards, async () => {}, 'b', ddb);
  out.share = [(await a.balance()).length, (await b.balance()).length, (await a.balance()).length, (await b.balance()).length];
  a.stopping = b.stopping = true;
}

// S3 / SNS bridge
{
  out.s3test = unwrapMessage('s3', JSON.stringify({ Service: 'Amazon S3', Event: 's3:TestEvent' }));
  const sns = unwrapMessage('sns', JSON.stringify({ Type: 'Notification', MessageId: 'm', TopicArn: 'arn:t', Message: 'hello',
    SigningCertURL: 'https://c', MessageAttributes: { level: { Type: 'String', Value: 'high' } } }));
  out.sns = sns.Records[0];
  const deleted = [];
  const sqs = async (op, input) => { deleted.push(...input.Entries.map((e) => e.ReceiptHandle)); return {}; };
  const body = (k) => JSON.stringify({ Records: [{ key: k }] });
  const records = [body('ok'), body('fail'), '{not json'].map((b, i) => ({ messageId: `m${i}`, receiptHandle: `r${i}`, body: b }));
  await processBridged(sqs, 'url', new Invoker(async (e) => { if (e.Records[0].key === 'fail') throw new Error('x'); }, 1), records, 's3');
  out.bridged = deleted;
}

console.log(JSON.stringify(out));
process.exit(0);
"""


def test_node_stream_consumer_and_bridge():
    script = SCRIPT.replace("SHIM", json.dumps(SHIM))
    proc = subprocess.run(["node", "--input-type=module", "-e", script], capture_output=True, text=True, timeout=60,
                          env={**os.environ, "L2E_TIMEOUT_S": "5"})
    assert proc.returncode == 0, proc.stderr + proc.stdout
    out = json.loads(proc.stdout.strip().splitlines()[-1])
    assert out["order"] == ["shard-0:1", "shard-0:2", "shard-0:3", "shard-1:4"]
    assert out["checkpoints"] == ["3", "4"] and out["data"] == "hi"
    assert out["partial"] == {"calls": [["1", "2"], ["2"]], "cps": ["1"]}
    assert out["bisect"] == [["a", "poison", "b", "c"], ["a", "poison"], ["a"], ["poison"], ["poison"], ["b", "c"]]
    assert out["share"] == [4, 0, 2, 2]
    assert out["s3test"] is None
    assert out["sns"]["EventSource"] == "aws:sns" and out["sns"]["Sns"]["Message"] == "hello"
    assert out["sns"]["Sns"]["SigningCertUrl"] == "https://c" and out["sns"]["EventSubscriptionArn"] == "arn:t:lambda2eks"
    assert out["bridged"] == ["r0"]
