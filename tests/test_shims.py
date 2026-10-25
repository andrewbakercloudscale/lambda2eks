"""Run both shims for real: an HTTP server in a subprocess, plus SQS and schedule modes."""

import base64
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

SHIMS = Path(__file__).resolve().parent.parent / "lambda2eks" / "shims"

PY_HANDLER = '''
import time
def handler(event, context):
    if event.get("rawPath") == "/slow" or event.get("path") == "/slow":
        time.sleep(3)
    if event.get("boom") or event.get("rawPath") == "/boom" or event.get("path") == "/boom":
        raise ValueError("boom")
    return {"statusCode": 201, "headers": {"X-Test": "yes"}, "cookies": ["a=1"],
            "body": __import__("json").dumps({"event": event, "remaining": context.get_remaining_time_in_millis(),
                                               "fn": context.function_name})}
def raw(event, context):
    if event.get("boom"):
        raise ValueError("boom")
    return {"echo": event, "fn": context.function_name}
def records(event, context):
    bad = [r["messageId"] for r in event["Records"] if r["body"] == "bad"]
    return {"batchItemFailures": [{"itemIdentifier": m} for m in bad]}
'''

NODE_HANDLER = '''
export const handler = async (event, context) => {
  const p = event.rawPath || event.path;
  if (p === '/slow') await new Promise((r) => setTimeout(r, 3000));
  if (p === '/boom' || event.boom) throw new Error('boom');
  return { statusCode: 201, headers: { 'X-Test': 'yes' }, cookies: ['a=1'],
           body: JSON.stringify({ event, remaining: context.getRemainingTimeInMillis(), fn: context.functionName }) };
};
export const raw = (event, context, callback) => { event.boom ? callback(new Error('boom')) : callback(null, { echo: event, fn: context.functionName }); };
'''


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(lang, tmp_path, handler, fmt, routes):
    if lang == "python":
        (tmp_path / "app.py").write_text(PY_HANDLER)
        cmd = [sys.executable, str(SHIMS / "shim.py")]
    else:
        (tmp_path / "app.mjs").write_text(NODE_HANDLER)
        cmd = ["node", str(SHIMS / "shim.mjs")]
    port = free_port()
    env = {**os.environ, "LAMBDA_TASK_ROOT": str(tmp_path), "L2E_MODE": "http", "L2E_HANDLER": handler,
           "L2E_PORT": str(port), "L2E_HTTP_EVENT_FORMAT": fmt, "L2E_HTTP_ROUTES": json.dumps(routes),
           "L2E_TIMEOUT_S": "1", "AWS_LAMBDA_FUNCTION_NAME": "orders-api", "L2E_CONCURRENCY": "2"}
    proc = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    base = f"http://127.0.0.1:{port}"
    for _ in range(100):
        try:
            urllib.request.urlopen(base + "/_lambda2eks/healthz", timeout=1)
            return proc, base
        except OSError:
            if proc.poll() is not None:
                raise RuntimeError(proc.stdout.read().decode())
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("shim did not start")


def call(base, path, method="GET", body=None, headers=None):
    req = urllib.request.Request(base + path, data=body, method=method, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, dict(r.headers), r.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


LANGS = ["python", pytest.param("node", marks=pytest.mark.skipif(not os.popen("which node").read(), reason="node missing"))]


@pytest.fixture(params=LANGS)
def lang(request):
    return request.param


ROUTES = [{"method": "GET", "path": "/orders/{id}"}, {"method": "POST", "path": "/files/{key+}"},
          {"method": "ANY", "path": "/slow"}, {"method": "ANY", "path": "/boom"}]


def test_http_v2(lang, tmp_path):
    proc, base = start(lang, tmp_path, "app.handler", "2.0", ROUTES)
    try:
        status, headers, body = call(base, "/orders/a%20b?x=1&x=2", headers={"Cookie": "s=1; t=2"})
        assert status == 201 and headers["X-Test"] == "yes" and headers["Set-Cookie"] == "a=1"
        out = json.loads(body)
        ev = out["event"]
        assert ev["version"] == "2.0" and ev["routeKey"] == "GET /orders/{id}"
        assert ev["pathParameters"] == {"id": "a b"}
        assert ev["queryStringParameters"] == {"x": "1,2"} and ev["rawQueryString"] == "x=1&x=2"
        assert ev["cookies"] == ["s=1", "t=2"]
        assert out["fn"] == "orders-api" and 0 < out["remaining"] <= 1000

        status, _, body = call(base, "/files/a/b/c.png", "POST", b"\x89PNG", {"Content-Type": "image/png"})
        ev = json.loads(body)["event"]
        assert ev["pathParameters"] == {"key": "a/b/c.png"}
        assert ev["isBase64Encoded"] is True and base64.b64decode(ev["body"]) == b"\x89PNG"

        assert call(base, "/orders/1", "DELETE")[0] == 404
        assert call(base, "/nope")[0] == 404
        assert call(base, "/boom")[0] == 502
        assert call(base, "/slow")[0] == 504
    finally:
        proc.terminate()
        proc.wait(5)


def test_http_v1(lang, tmp_path):
    proc, base = start(lang, tmp_path, "app.handler", "1.0", ROUTES)
    try:
        status, _, body = call(base, "/orders/42?x=1&x=2", "GET", headers={"X-A": "1"})
        ev = json.loads(body)["event"]
        assert status == 201
        assert ev["resource"] == "/orders/{id}" and ev["path"] == "/orders/42" and ev["httpMethod"] == "GET"
        assert ev["pathParameters"] == {"id": "42"}
        assert ev["queryStringParameters"] == {"x": "2"} and ev["multiValueQueryStringParameters"] == {"x": ["1", "2"]}
        assert ev["headers"]["X-A"] == "1" and ev["body"] is None
    finally:
        proc.terminate()
        proc.wait(5)


def test_invoke_api(lang, tmp_path):
    proc, base = start(lang, tmp_path, "app.raw", "2.0", [])
    path = "/2015-03-31/functions/function/invocations"
    try:
        status, headers, body = call(base, path, "POST", json.dumps({"a": 1}).encode())
        assert status == 200 and json.loads(body) == {"echo": {"a": 1}, "fn": "orders-api"}
        assert "X-Amz-Function-Error" not in headers
        status, _, body = call(base, path.replace("/function/", "/orders-api/"), "POST", b"{}")
        assert status == 200 and json.loads(body)["echo"] == {}
        status, headers, body = call(base, path, "POST", json.dumps({"boom": True}).encode())
        assert status == 200 and headers["X-Amz-Function-Error"] == "Unhandled"
        assert json.loads(body)["errorMessage"] == "boom"
    finally:
        proc.terminate()
        proc.wait(5)


def test_python_sqs_partial_failures(tmp_path, monkeypatch):
    (tmp_path / "app.py").write_text(PY_HANDLER)
    monkeypatch.setenv("LAMBDA_TASK_ROOT", str(tmp_path))
    sys.path.insert(0, str(SHIMS))
    import shim  # noqa: PLC0415
    monkeypatch.setattr(shim, "TASK_ROOT", str(tmp_path))  # another test may have imported it already

    class FakeSQS:
        def __init__(self):
            self.deleted = []

        def delete_message_batch(self, QueueUrl, Entries):
            self.deleted += [e["ReceiptHandle"] for e in Entries]
            return {}

    msgs = [{"MessageId": f"m{i}", "ReceiptHandle": f"r{i}", "Body": body, "MD5OfBody": "",
             "MessageAttributes": {"k": {"DataType": "Binary", "BinaryValue": b"\x00"}}}
            for i, body in enumerate(["ok", "bad", "ok"])]
    records = [shim.sqs_record(m, "arn:aws:sqs:eu-west-1:1:q") for m in msgs]
    assert records[0]["messageAttributes"]["k"]["binaryValue"] == "AA=="
    invoker = shim.Invoker(shim.load_handler("app.records"), 1)
    sqs = FakeSQS()
    shim.process_batch(sqs, "url", invoker, records, partial=True)
    assert sqs.deleted == ["r0", "r2"]
    # A malformed failure response fails the whole batch, as Lambda does.
    assert shim.failed_ids({"batchItemFailures": [{"itemIdentifier": "zzz"}]}, records) == {"m0", "m1", "m2"}
    sqs2 = FakeSQS()
    shim.process_batch(sqs2, "url", invoker, records, partial=False)
    assert sqs2.deleted == ["r0", "r1", "r2"]


@pytest.mark.parametrize("handler, code", [("report.ok", 0), ("report.bad", 1)])
def test_python_schedule_exit_code(tmp_path, handler, code):
    (tmp_path / "report.py").write_text(
        "def ok(event, context):\n    assert event == {'report': 'daily'}\n    return 1\n"
        "def bad(event, context):\n    raise RuntimeError('x')\n")
    env = {**os.environ, "LAMBDA_TASK_ROOT": str(tmp_path), "L2E_MODE": "schedule", "L2E_HANDLER": handler,
           "L2E_SCHEDULE_INPUT": '{"report": "daily"}'}
    assert subprocess.run([sys.executable, str(SHIMS / "shim.py")], env=env, capture_output=True, timeout=20).returncode == code


@pytest.mark.skipif(not os.popen("which node").read(), reason="node missing")
def test_node_sqs_helpers():
    script = f'''
import {{ failedIds, sqsRecord }} from {json.dumps((SHIMS / "shim.mjs").as_uri())};
const recs = [sqsRecord({{MessageId: 'a', ReceiptHandle: 'r', Body: 'x', MessageAttributes: {{k: {{DataType: 'Binary', BinaryValue: new Uint8Array([0])}}}}}}, 'arn'),
              sqsRecord({{MessageId: 'b', ReceiptHandle: 's', Body: 'y'}}, 'arn')];
console.log(JSON.stringify({{
  bin: recs[0].messageAttributes.k.binaryValue,
  one: [...failedIds({{batchItemFailures: [{{itemIdentifier: 'b'}}]}}, recs)],
  bad: [...failedIds({{batchItemFailures: [{{itemIdentifier: 'zz'}}]}}, recs)].sort(),
  none: [...failedIds(null, recs)],
}}));
'''
    out = json.loads(subprocess.run(["node", "--input-type=module", "-e", script], capture_output=True, text=True, check=True).stdout)
    assert out == {"bin": "AA==", "one": ["b"], "bad": ["a", "b"], "none": []}
