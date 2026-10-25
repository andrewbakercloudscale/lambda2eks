"""lambda2eks runtime shim for Python handlers.

Replaces the Lambda runtime inside a Lambda base image and calls the unchanged handler.
Modes (L2E_MODE): http (API Gateway events + Invoke API), sqs (queue worker, optionally unwrapping
S3 or SNS events from a bridge queue), stream (Kinesis / DynamoDB Streams consumer), schedule (one run).
Standard library only, except boto3 in sqs and stream modes (the Lambda Python base image ships it).
"""

import base64
import concurrent.futures
import importlib
import json
import logging
import math
import os
import re
import signal
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qsl, unquote, urlsplit

log = logging.getLogger("lambda2eks")
logging.basicConfig(level=os.environ.get("L2E_LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")

TASK_ROOT = os.environ.get("LAMBDA_TASK_ROOT", "/var/task")
FUNCTION_NAME = os.environ.get("AWS_LAMBDA_FUNCTION_NAME", "function")
MEMORY = os.environ.get("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128")
TIMEOUT_S = float(os.environ.get("L2E_TIMEOUT_S", "3"))
REGION = os.environ.get("AWS_REGION", "us-east-1")
ACCOUNT = os.environ.get("L2E_ACCOUNT_ID", "000000000000")
FUNCTION_ARN = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION_NAME}"
HEALTH_PATH = "/_lambda2eks/healthz"
# Any function name, so SDK clients pointed at this endpoint keep their FunctionName.
INVOKE_PATH = re.compile(r"^/2015-03-31/functions/[^/]+/invocations$")
TEXT_TYPES = re.compile(r"^(text/|application/(json|xml|javascript|x-www-form-urlencoded|[a-z.+-]*\+(json|xml)))", re.I)


class LambdaContext:
    def __init__(self, request_id=None):
        self.function_name = FUNCTION_NAME
        self.function_version = os.environ.get("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST")
        self.invoked_function_arn = FUNCTION_ARN
        self.memory_limit_in_mb = MEMORY
        self.aws_request_id = request_id or str(uuid.uuid4())
        self.log_group_name = f"/aws/lambda/{FUNCTION_NAME}"
        self.log_stream_name = os.environ.get("HOSTNAME", "local")
        self.identity = None
        self.client_context = None
        self._deadline = time.monotonic() + TIMEOUT_S

    def get_remaining_time_in_millis(self):
        return max(0, int((self._deadline - time.monotonic()) * 1000))


def load_handler(spec):
    module_path, _, func_name = spec.rpartition(".")
    if not module_path:
        raise ValueError(f"handler {spec!r} must look like module.function")
    sys.path.insert(0, TASK_ROOT)
    module = importlib.import_module(module_path.replace("/", "."))
    return getattr(module, func_name)


class Invoker:
    """Runs the handler with Lambda's limits: a timeout and a cap on concurrent invocations."""

    def __init__(self, handler, concurrency):
        self.handler = handler
        self.slots = threading.BoundedSemaphore(concurrency)
        self.pool = concurrent.futures.ThreadPoolExecutor(max_workers=concurrency * 2 + 2)

    def invoke(self, event, request_id=None):
        """Return (ok, result_or_error). A timed-out handler keeps running; Python cannot stop a thread."""
        with self.slots:
            ctx = LambdaContext(request_id)
            future = self.pool.submit(self.handler, event, ctx)
            try:
                return True, future.result(timeout=TIMEOUT_S)
            except concurrent.futures.TimeoutError:
                log.error("request %s timed out after %ss", ctx.aws_request_id, TIMEOUT_S)
                return False, {"errorType": "TimeoutError", "errorMessage": f"Task timed out after {TIMEOUT_S:.2f} seconds"}
            except Exception as e:  # noqa: BLE001 - the handler's error is the invocation result
                log.exception("request %s failed", ctx.aws_request_id)
                return False, {"errorType": type(e).__name__, "errorMessage": str(e),
                               "stackTrace": traceback.format_exception(type(e), e, e.__traceback__)}


# ------------------------------------------------------------------ HTTP mode

def compile_routes(routes):
    """API Gateway route templates -> (method, template, regex, param names)."""
    out = []
    for r in routes or []:
        path = r.get("path")
        if not path:
            continue
        names, pattern = [], ""
        for seg in path.strip("/").split("/"):
            m = re.fullmatch(r"\{([^}]+?)(\+?)\}", seg)
            if m:
                names.append(m.group(1))
                pattern += "/(.+)" if m.group(2) else "/([^/]+)"
            elif seg:
                pattern += "/" + re.escape(seg)
        out.append((r.get("method", "ANY").upper(), path, re.compile("^" + (pattern or "/") + "/?$"), names))
    return out


def match_route(routes, method, path):
    if not routes:
        return "$default", None
    for rmethod, template, regex, names in routes:
        m = regex.match(path)
        if m and rmethod in ("ANY", method):
            return template, {n: unquote(v) for n, v in zip(names, m.groups())} or None
    return None, None


def build_event(fmt, method, raw_path, query, headers_list, body, source_ip, template, path_params):
    now = datetime.now(timezone.utc)
    request_id = str(uuid.uuid4())
    ctype = next((v for k, v in headers_list if k.lower() == "content-type"), "")
    is_b64 = bool(body) and not TEXT_TYPES.match(ctype)
    body_out = (base64.b64encode(body).decode() if is_b64 else body.decode("utf-8", "replace")) if body else None
    pairs = parse_qsl(query, keep_blank_values=True)
    user_agent = next((v for k, v in headers_list if k.lower() == "user-agent"), "")

    if fmt == "2.0":
        headers, cookies = {}, []
        for k, v in headers_list:
            if k.lower() == "cookie":
                cookies.extend(c.strip() for c in v.split(";") if c.strip())
                continue
            headers[k.lower()] = f"{headers[k.lower()]},{v}" if k.lower() in headers else v
        qs = {}
        for k, v in pairs:
            qs[k] = f"{qs[k]},{v}" if k in qs else v
        route_key = "$default" if template == "$default" else f"{method} {template}"
        event = {
            "version": "2.0", "routeKey": route_key, "rawPath": raw_path, "rawQueryString": query,
            "headers": headers, "requestContext": {
                "accountId": ACCOUNT, "apiId": "lambda2eks", "domainName": headers.get("host", ""),
                "domainPrefix": headers.get("host", "").split(".")[0],
                "http": {"method": method, "path": raw_path, "protocol": "HTTP/1.1", "sourceIp": source_ip, "userAgent": user_agent},
                "requestId": request_id, "routeKey": route_key, "stage": "$default",
                "time": now.strftime("%d/%b/%Y:%H:%M:%S +0000"), "timeEpoch": int(now.timestamp() * 1000),
            },
            "isBase64Encoded": is_b64,
        }
        if cookies:
            event["cookies"] = cookies
        if qs:
            event["queryStringParameters"] = qs
        if path_params:
            event["pathParameters"] = path_params
        if body_out is not None:
            event["body"] = body_out
        return event, request_id

    mv_headers, mv_qs = {}, {}
    for k, v in headers_list:
        mv_headers.setdefault(k, []).append(v)
    for k, v in pairs:
        mv_qs.setdefault(k, []).append(v)
    resource = raw_path if template == "$default" else template
    return {
        "resource": resource, "path": raw_path, "httpMethod": method,
        "headers": {k: v[-1] for k, v in mv_headers.items()} or None, "multiValueHeaders": mv_headers or None,
        "queryStringParameters": {k: v[-1] for k, v in mv_qs.items()} or None,
        "multiValueQueryStringParameters": mv_qs or None,
        "pathParameters": path_params, "stageVariables": None,
        "requestContext": {
            "resourcePath": resource, "httpMethod": method, "path": raw_path, "stage": "prod",
            "requestId": request_id, "requestTimeEpoch": int(now.timestamp() * 1000), "accountId": ACCOUNT,
            "apiId": "lambda2eks", "protocol": "HTTP/1.1", "identity": {"sourceIp": source_ip, "userAgent": user_agent},
        },
        "body": body_out, "isBase64Encoded": is_b64,
    }, request_id


def convert_response(fmt, result):
    """Handler result -> (status, [(header, value)], body bytes), following API Gateway's rules."""
    if fmt == "2.0" and not (isinstance(result, dict) and "statusCode" in result):
        body = result if isinstance(result, str) else json.dumps(result)
        return 200, [("Content-Type", "application/json")], body.encode()
    if not isinstance(result, dict):
        return 502, [("Content-Type", "application/json")], b'{"message": "Internal server error"}'
    headers = [(k, str(v)) for k, v in (result.get("headers") or {}).items()]
    for k, values in (result.get("multiValueHeaders") or {}).items():
        headers.extend((k, str(v)) for v in values)
    headers.extend(("Set-Cookie", c) for c in result.get("cookies") or [])
    body = result.get("body") or ""
    if not isinstance(body, str):
        body = json.dumps(body)
    data = base64.b64decode(body) if result.get("isBase64Encoded") else body.encode()
    return int(result.get("statusCode", 200)), headers, data


def serve_http(invoker):
    fmt = os.environ.get("L2E_HTTP_EVENT_FORMAT", "1.0")
    routes = compile_routes(json.loads(os.environ.get("L2E_HTTP_ROUTES", "[]")))
    port = int(os.environ.get("L2E_PORT", "8080"))

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, format, *args):  # noqa: A002 - BaseHTTPRequestHandler signature
            log.debug("%s %s", self.address_string(), format % args)

        def _send(self, status, headers, body):
            self.send_response(status)
            for k, v in headers:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _handle(self):
            url = urlsplit(self.path)
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            if url.path == HEALTH_PATH:
                return self._send(200, [("Content-Type", "text/plain")], b"ok")
            if INVOKE_PATH.match(url.path) and self.command == "POST":
                event = json.loads(body or b"{}")
                ok, result = invoker.invoke(event)
                headers = [("Content-Type", "application/json")] + ([] if ok else [("X-Amz-Function-Error", "Unhandled")])
                return self._send(200, headers, json.dumps(result).encode())
            template, params = match_route(routes, self.command, url.path)
            if template is None:
                return self._send(404, [("Content-Type", "application/json")], b'{"message": "Not Found"}')
            event, request_id = build_event(fmt, self.command, url.path, url.query, list(self.headers.items()),
                                            body, self.client_address[0], template, params)
            ok, result = invoker.invoke(event, request_id)
            if not ok:
                status = 504 if result.get("errorType") == "TimeoutError" else 502
                msg = b'{"message": "Endpoint request timed out"}' if status == 504 else b'{"message": "Internal server error"}'
                return self._send(status, [("Content-Type", "application/json")], msg)
            self._send(*convert_response(fmt, result))

        do_GET = do_POST = do_PUT = do_PATCH = do_DELETE = do_HEAD = do_OPTIONS = _handle

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104 - container listens on all interfaces
    server.daemon_threads = True

    def stop(*_):
        log.info("SIGTERM: finishing in-flight requests")
        threading.Thread(target=server.shutdown, daemon=True).start()

    signal.signal(signal.SIGTERM, stop)
    log.info("http mode on :%d, payload format %s, %d route(s)", port, fmt, len(routes))
    server.serve_forever()


# ------------------------------------------------------------------ SQS mode

def sqs_record(msg, queue_arn):
    attrs = {}
    for name, a in (msg.get("MessageAttributes") or {}).items():
        binary = a.get("BinaryValue")
        attrs[name] = {
            "stringValue": a.get("StringValue"),
            "binaryValue": base64.b64encode(binary).decode() if binary is not None else None,
            "stringListValues": a.get("StringListValues", []),
            "binaryListValues": [base64.b64encode(b).decode() for b in a.get("BinaryListValues", [])],
            "dataType": a.get("DataType"),
        }
    return {
        "messageId": msg["MessageId"], "receiptHandle": msg["ReceiptHandle"], "body": msg["Body"],
        "attributes": msg.get("Attributes", {}), "messageAttributes": attrs, "md5OfBody": msg.get("MD5OfBody"),
        "eventSource": "aws:sqs", "eventSourceARN": queue_arn, "awsRegion": REGION,
    }


def failed_ids(result, records):
    """Message ids to leave on the queue, from a ReportBatchItemFailures response."""
    if not isinstance(result, dict) or "batchItemFailures" not in result:
        return set()
    ids = {r["messageId"] for r in records}
    out = set()
    for item in result.get("batchItemFailures") or []:
        mid = item.get("itemIdentifier") if isinstance(item, dict) else None
        if not mid or mid not in ids:
            return ids  # Lambda treats a malformed response as a failure of the whole batch
        out.add(mid)
    return out


def unwrap_message(kind, body, subscription_arn=""):
    """Body of a bridge-queue message -> the event Lambda would have received, or None to drop it."""
    msg = json.loads(body)
    if kind == "s3":
        if msg.get("Event") == "s3:TestEvent":
            return None  # S3 sends this once when the notification is configured
        return {"Records": msg["Records"]}
    attrs = {k: {"Type": v.get("Type"), "Value": v.get("Value")} for k, v in (msg.get("MessageAttributes") or {}).items()}
    return {"Records": [{
        "EventSource": "aws:sns", "EventVersion": "1.0",
        "EventSubscriptionArn": subscription_arn or f"{msg.get('TopicArn')}:lambda2eks",
        "Sns": {
            "Type": msg.get("Type"), "MessageId": msg.get("MessageId"), "TopicArn": msg.get("TopicArn"),
            "Subject": msg.get("Subject"), "Message": msg.get("Message"), "Timestamp": msg.get("Timestamp"),
            "SignatureVersion": msg.get("SignatureVersion"), "Signature": msg.get("Signature"),
            "SigningCertUrl": msg.get("SigningCertURL"), "UnsubscribeUrl": msg.get("UnsubscribeURL"),
            "MessageAttributes": attrs,
        },
    }]}


def delete_records(sqs, queue_url, done):
    for i in range(0, len(done), 10):
        chunk = done[i:i + 10]
        res = sqs.delete_message_batch(QueueUrl=queue_url, Entries=[
            {"Id": str(n), "ReceiptHandle": r["receiptHandle"]} for n, r in enumerate(chunk)])
        for f in res.get("Failed") or []:
            log.error("delete failed for %s: %s", chunk[int(f["Id"])]["messageId"], f.get("Message"))


def process_batch(sqs, queue_url, invoker, records, partial):
    ok, result = invoker.invoke({"Records": records})
    if not ok:
        log.error("batch of %d failed; messages return to the queue after the visibility timeout", len(records))
        return
    keep = failed_ids(result, records) if partial else set()
    delete_records(sqs, queue_url, [r for r in records if r["messageId"] not in keep])


def process_bridged(sqs, queue_url, invoker, records, kind, subscription_arn=""):
    """S3 and SNS invoke Lambda one event at a time, so each message is its own invocation."""
    done = []
    for r in records:
        try:
            event = unwrap_message(kind, r["body"], subscription_arn)
        except (ValueError, KeyError, AttributeError) as e:
            log.error("message %s is not a %s event (%s); it stays on the queue for the redrive policy", r["messageId"], kind, e)
            continue
        if event is None:
            done.append(r)
            continue
        ok, _ = invoker.invoke(event)
        if ok:
            done.append(r)
        else:
            log.error("message %s failed; it returns to the queue after the visibility timeout", r["messageId"])
    delete_records(sqs, queue_url, done)


def run_sqs(invoker, client=None):
    queue_url = os.environ["L2E_SQS_QUEUE_URL"]
    queue_arn = os.environ.get("L2E_SQS_QUEUE_ARN", "")
    batch = int(os.environ.get("L2E_SQS_BATCH_SIZE", "10"))
    partial = os.environ.get("L2E_SQS_REPORT_BATCH_ITEM_FAILURES", "false") == "true"
    unwrap = os.environ.get("L2E_SQS_UNWRAP", "")
    subscription_arn = os.environ.get("L2E_SNS_SUBSCRIPTION_ARN", "")
    if client is None:
        import boto3
        client = boto3.client("sqs", region_name=REGION)
    sqs = client
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: (log.info("SIGTERM: finishing the current batch"), stopping.set()))
    log.info("sqs mode on %s, batch %d, partial failures %s%s", queue_url, batch, partial, f", unwrapping {unwrap}" if unwrap else "")

    def poll():
        while not stopping.is_set():
            try:
                resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=min(batch, 10), WaitTimeSeconds=20,
                                           AttributeNames=["All"], MessageAttributeNames=["All"])
            except Exception:  # noqa: BLE001 - keep polling through throttling and network errors
                log.exception("receive_message failed; retrying in 5s")
                stopping.wait(5)
                continue
            records = [sqs_record(m, queue_arn) for m in resp.get("Messages") or []]
            if records and unwrap:
                process_bridged(sqs, queue_url, invoker, records, unwrap, subscription_arn)
            elif records:
                process_batch(sqs, queue_url, invoker, records, partial)

    # One poller per concurrency slot, as Lambda runs one batch per environment.
    pollers = [threading.Thread(target=poll, daemon=True) for _ in range(int(os.environ.get("L2E_CONCURRENCY", "1")))]
    for t in pollers:
        t.start()
    for t in pollers:
        t.join()
    return 0


# ------------------------------------------------------------------ stream mode
#
# Kinesis and DynamoDB Streams have no queue to share, so replicas divide the shards between them
# with leases in a DynamoDB table, the way the Kinesis Client Library does. Each lease holds the
# shard's checkpoint (the last sequence number handled). A replica takes its fair share of shards,
# renews its leases, and gives extra ones back when more replicas join. A child shard starts only
# after its parent is finished, so records for a key stay in order across reshards.

LEASE_TTL_S = 30
RENEW_EVERY_S = 10


class LeaseLost(Exception):
    pass


class Leases:
    """Shard leases in a DynamoDB table keyed by "shard" (string)."""

    def __init__(self, ddb, table, consumer, owner):
        self.ddb, self.table, self.consumer, self.owner = ddb, table, consumer, owner

    def _key(self, shard_id):
        return {"shard": {"S": f"{self.consumer}#{shard_id}"}}

    MEMBER = "~member~"  # heartbeat rows, so a replica that holds no shard yet still counts towards the fair share

    def heartbeat(self, now):
        self.ddb.put_item(TableName=self.table, Item={**self._key(self.MEMBER + self.owner), "owner": {"S": self.owner},
                                                      "expires": {"N": str(now + LEASE_TTL_S)}})

    def snapshot(self):
        """shard id (or ~member~owner) -> {"owner", "expires", "checkpoint", "finished"} for this consumer."""
        out, kwargs = {}, {"TableName": self.table, "ConsistentRead": True}
        while True:
            page = self.ddb.scan(**kwargs)
            for item in page.get("Items", []):
                consumer, _, shard = item["shard"]["S"].partition("#")
                if consumer == self.consumer:
                    out[shard] = {"owner": item.get("owner", {}).get("S"), "expires": float(item.get("expires", {}).get("N", "0")),
                                  "checkpoint": item.get("checkpoint", {}).get("S"), "finished": item.get("finished", {}).get("BOOL", False)}
            if "LastEvaluatedKey" not in page:
                return out
            kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    def _update(self, shard_id, expr, condition, values, names=None):
        try:
            self.ddb.update_item(TableName=self.table, Key=self._key(shard_id), UpdateExpression=expr,
                                 ConditionExpression=condition, ExpressionAttributeValues=values,
                                 **({"ExpressionAttributeNames": names} if names else {}))
            return True
        except Exception as e:  # noqa: BLE001 - only a failed condition means the lease moved
            if type(e).__name__ == "ConditionalCheckFailedException" or "ConditionalCheckFailed" in str(e):
                return False
            raise

    def acquire(self, shard_id, now):
        return self._update(shard_id, "SET #o = :me, #e = :exp",
                            "attribute_not_exists(#o) OR #o = :me OR #e < :now",
                            {":me": {"S": self.owner}, ":exp": {"N": str(now + LEASE_TTL_S)}, ":now": {"N": str(now)}},
                            {"#o": "owner", "#e": "expires"})

    def renew(self, shard_id, now):
        return self._update(shard_id, "SET #e = :exp", "#o = :me",
                            {":me": {"S": self.owner}, ":exp": {"N": str(now + LEASE_TTL_S)}}, {"#o": "owner", "#e": "expires"})

    def checkpoint(self, shard_id, seq, finished=False):
        ok = self._update(shard_id, "SET #c = :seq, #f = :fin", "#o = :me",
                          {":me": {"S": self.owner}, ":seq": {"S": seq or ""}, ":fin": {"BOOL": finished}},
                          {"#o": "owner", "#c": "checkpoint", "#f": "finished"})
        if not ok:
            raise LeaseLost(shard_id)

    def release(self, shard_id):
        self._update(shard_id, "SET #e = :zero", "#o = :me", {":me": {"S": self.owner}, ":zero": {"N": "0"}},
                     {"#o": "owner", "#e": "expires"})


def _epoch(value):
    return value.timestamp() if hasattr(value, "timestamp") else float(value)


class KinesisSource:
    kind = "kinesis"

    def __init__(self, client, stream_name, stream_arn):
        self.client, self.name = client, stream_name
        self.arn = stream_arn or f"arn:aws:kinesis:{REGION}:{ACCOUNT}:stream/{stream_name}"

    def shards(self):
        out, kwargs = [], {"StreamName": self.name}
        while True:
            page = self.client.list_shards(**kwargs)
            out += [{"id": sh["ShardId"], "parent": sh.get("ParentShardId")} for sh in page["Shards"]]
            if not page.get("NextToken"):
                return out
            kwargs = {"NextToken": page["NextToken"]}

    def iterator(self, shard_id, checkpoint, position, start_ts):
        kw = {"StreamName": self.name, "ShardId": shard_id}
        if checkpoint:
            kw.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER", StartingSequenceNumber=checkpoint)
        elif start_ts is not None:
            kw.update(ShardIteratorType="AT_TIMESTAMP", Timestamp=start_ts)
        else:
            kw["ShardIteratorType"] = position
        return self.client.get_shard_iterator(**kw)["ShardIterator"]

    def read(self, it, limit):
        resp = self.client.get_records(ShardIterator=it, Limit=limit)
        return resp["Records"], resp.get("NextShardIterator")

    def record(self, r, shard_id):
        data = r["Data"]
        return {
            "kinesis": {"kinesisSchemaVersion": "1.0", "partitionKey": r["PartitionKey"], "sequenceNumber": r["SequenceNumber"],
                        "data": base64.b64encode(data if isinstance(data, bytes) else data.encode()).decode(),
                        "approximateArrivalTimestamp": _epoch(r["ApproximateArrivalTimestamp"])},
            "eventSource": "aws:kinesis", "eventVersion": "1.0", "eventID": f"{shard_id}:{r['SequenceNumber']}",
            "eventName": "aws:kinesis:record", "invokeIdentityArn": os.environ.get("L2E_ROLE_ARN", ""),
            "awsRegion": REGION, "eventSourceARN": self.arn,
        }

    @staticmethod
    def seq(lambda_record):
        return lambda_record["kinesis"]["sequenceNumber"]

    @staticmethod
    def created(raw):
        return _epoch(raw["ApproximateArrivalTimestamp"])


class DynamoDBSource:
    kind = "dynamodb"

    def __init__(self, client, stream_arn):
        self.client, self.arn = client, stream_arn

    def shards(self):
        out, kwargs = [], {"StreamArn": self.arn}
        while True:
            desc = self.client.describe_stream(**kwargs)["StreamDescription"]
            out += [{"id": sh["ShardId"], "parent": sh.get("ParentShardId")} for sh in desc["Shards"]]
            if not desc.get("LastEvaluatedShardId"):
                return out
            kwargs["ExclusiveStartShardId"] = desc["LastEvaluatedShardId"]

    def iterator(self, shard_id, checkpoint, position, start_ts):
        kw = {"StreamArn": self.arn, "ShardId": shard_id}
        if checkpoint:
            kw.update(ShardIteratorType="AFTER_SEQUENCE_NUMBER", SequenceNumber=checkpoint)
        else:  # DynamoDB Streams has no AT_TIMESTAMP; older records are skipped by the consumer instead
            kw["ShardIteratorType"] = "TRIM_HORIZON" if start_ts is not None else position
        return self.client.get_shard_iterator(**kw)["ShardIterator"]

    def read(self, it, limit):
        resp = self.client.get_records(ShardIterator=it, Limit=limit)
        return resp["Records"], resp.get("NextShardIterator")

    def record(self, r, shard_id):
        out = {k: v for k, v in r.items() if k != "userIdentity"}
        out["dynamodb"] = dict(r["dynamodb"])
        if "ApproximateCreationDateTime" in out["dynamodb"]:
            out["dynamodb"]["ApproximateCreationDateTime"] = _epoch(out["dynamodb"]["ApproximateCreationDateTime"])
        if r.get("userIdentity"):
            out["userIdentity"] = r["userIdentity"]
        out["eventSourceARN"] = self.arn
        return out

    @staticmethod
    def seq(lambda_record):
        return lambda_record["dynamodb"]["SequenceNumber"]

    @staticmethod
    def created(raw):
        return _epoch(raw["dynamodb"].get("ApproximateCreationDateTime", 0))


class StreamConsumer:
    def __init__(self, source, leases, invoker, batch_size=100, max_retry=-1, bisect=False, partial=False,
                 position="LATEST", start_ts=None, idle_wait=1.0, sleep=None):
        self.source, self.leases, self.invoker = source, leases, invoker
        self.batch_size, self.max_retry, self.bisect, self.partial = batch_size, max_retry, bisect, partial
        self.position, self.start_ts, self.idle_wait = position, start_ts, idle_wait
        self.stopping = threading.Event()
        self.sleep = sleep or self.stopping.wait
        self.workers = {}  # shard id -> (thread, stop event)

    # -- one shard
    def deliver(self, records, checkpoint, attempt=0):
        """Invoke until the records are handled or their retries run out, checkpointing progress."""
        seq = self.source.seq
        while records and not self.stopping.is_set():
            ok, result = self.invoker.invoke({"Records": records})
            failed_at = None
            if ok and self.partial:
                failed_at = self._first_failure(result, records)
            if ok and failed_at is None:
                return True
            if ok and failed_at > 0:
                checkpoint(seq(records[failed_at - 1]))
                records = records[failed_at:]
            if self.bisect and len(records) > 1:  # as on Lambda, a split does not count as a retry
                mid = len(records) // 2
                return self.deliver(records[:mid], checkpoint, attempt) and self.deliver(records[mid:], checkpoint, attempt)
            attempt += 1
            if 0 <= self.max_retry < attempt:
                log.error("skipping %d record(s) from %s after %d attempt(s)", len(records), seq(records[0]), attempt)
                return True
            self.sleep(min(2 ** attempt, 30))
        return not records

    def _first_failure(self, result, records):
        if not isinstance(result, dict) or "batchItemFailures" not in result:
            return None
        items = result.get("batchItemFailures") or []
        if not items:
            return None
        index = {self.source.seq(r): i for i, r in enumerate(records)}
        hits = [index.get(item.get("itemIdentifier")) if isinstance(item, dict) else None for item in items]
        return 0 if None in hits else min(hits)  # a malformed response retries the whole batch, as Lambda does

    def consume(self, shard_id, checkpoint_seq, stop):
        leases, source = self.leases, self.source
        progress = {"seq": checkpoint_seq}

        def checkpoint(s):
            leases.checkpoint(shard_id, s)
            progress["seq"] = s

        it = source.iterator(shard_id, checkpoint_seq, self.position, self.start_ts)
        while not (stop.is_set() or self.stopping.is_set()):
            try:
                raw, it = source.read(it, self.batch_size)
            except Exception as e:  # noqa: BLE001
                if type(e).__name__ == "ExpiredIteratorException":
                    it = source.iterator(shard_id, progress["seq"], self.position, self.start_ts)
                    continue
                raise
            if self.start_ts is not None:
                raw = [r for r in raw if source.created(r) >= self.start_ts]
            records = [source.record(r, shard_id) for r in raw]
            if records and self.deliver(records, checkpoint):
                checkpoint(source.seq(records[-1]))
            elif records:
                return  # stopping mid-batch: the next owner starts again from the checkpoint
            if it is None:
                leases.checkpoint(shard_id, progress["seq"], finished=True)
                log.info("shard %s finished", shard_id)
                return
            if not records:
                self.sleep(self.idle_wait)

    def _run_worker(self, shard_id, checkpoint_seq, stop):
        try:
            self.consume(shard_id, checkpoint_seq, stop)
        except LeaseLost:
            log.warning("lease on %s was taken by another replica", shard_id)
        except Exception:  # noqa: BLE001 - the coordinator restarts the shard on its next pass
            log.exception("shard %s failed", shard_id)
            self.leases.release(shard_id)

    # -- the coordinator
    def balance(self, now=None):
        """One pass: renew, rebalance and acquire leases. Returns the shards this replica holds."""
        now = time.time() if now is None else now
        for shard_id, (thread, stop) in list(self.workers.items()):
            if not thread.is_alive() or stop.is_set():
                self.workers.pop(shard_id)
            elif not self.leases.renew(shard_id, now):
                stop.set()
                self.workers.pop(shard_id)
        self.leases.heartbeat(now)
        shards = self.source.shards()
        ids = {s["id"] for s in shards}
        snap = self.leases.snapshot()
        owners = {v["owner"] for k, v in snap.items() if k.startswith(Leases.MEMBER) and v["expires"] > now} | {self.leases.owner}
        snap = {k: v for k, v in snap.items() if not k.startswith(Leases.MEMBER)}
        finished = {k for k, v in snap.items() if v["finished"]}
        eligible = [s["id"] for s in shards if s["id"] not in finished
                    and (not s["parent"] or s["parent"] not in ids or s["parent"] in finished)]
        fair = math.ceil(len(eligible) / len(owners)) if eligible else 0
        mine = [k for k in self.workers if k in eligible]
        for shard_id in mine[fair:]:  # give extra shards back so a new replica can take them
            thread, stop = self.workers.pop(shard_id)
            stop.set()
            thread.join(timeout=60)
            self.leases.release(shard_id)
        for shard_id in eligible:
            if len(self.workers) >= fair:
                break
            lease = snap.get(shard_id, {})
            if shard_id in self.workers or (lease.get("owner") and lease["owner"] != self.leases.owner and lease["expires"] > now):
                continue
            if self.leases.acquire(shard_id, now):
                stop = threading.Event()
                thread = threading.Thread(target=self._run_worker, args=(shard_id, lease.get("checkpoint") or None, stop), daemon=True)
                self.workers[shard_id] = (thread, stop)
                thread.start()
        return sorted(self.workers)

    def run(self):
        while not self.stopping.is_set():
            try:
                held = self.balance()
                log.debug("holding %d shard(s): %s", len(held), held)
            except Exception:  # noqa: BLE001 - keep coordinating through throttling and network errors
                log.exception("lease coordination failed; retrying")
            self.stopping.wait(RENEW_EVERY_S)
        for shard_id, (thread, stop) in list(self.workers.items()):
            stop.set()
            thread.join(timeout=TIMEOUT_S + 30)
            self.leases.release(shard_id)


def _start_timestamp(raw):
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        return datetime.fromisoformat(raw.replace("Z", "+00:00")).timestamp()


def run_stream(invoker):
    import boto3
    kind = os.environ["L2E_STREAM_KIND"]
    if kind == "kinesis":
        source = KinesisSource(boto3.client("kinesis", region_name=REGION), os.environ["L2E_STREAM_NAME"], os.environ.get("L2E_STREAM_ARN"))
    else:
        arn = os.environ.get("L2E_STREAM_ARN") or boto3.client("dynamodb", region_name=REGION).describe_table(
            TableName=os.environ["L2E_DDB_TABLE"])["Table"].get("LatestStreamArn")
        if not arn:
            raise SystemExit(f"table {os.environ.get('L2E_DDB_TABLE')} has no stream enabled")
        source = DynamoDBSource(boto3.client("dynamodbstreams", region_name=REGION), arn)
    owner = f"{os.environ.get('HOSTNAME', 'local')}-{uuid.uuid4().hex[:8]}"
    leases = Leases(boto3.client("dynamodb", region_name=REGION), os.environ["L2E_LEASE_TABLE"],
                    os.environ.get("L2E_CONSUMER_NAME", FUNCTION_NAME), owner)
    consumer = StreamConsumer(
        source, leases, invoker,
        batch_size=int(os.environ.get("L2E_STREAM_BATCH_SIZE", "100")),
        max_retry=int(os.environ.get("L2E_STREAM_MAX_RETRY", "-1")),
        bisect=os.environ.get("L2E_STREAM_BISECT", "false") == "true",
        partial=os.environ.get("L2E_STREAM_REPORT_BATCH_ITEM_FAILURES", "false") == "true",
        position=os.environ.get("L2E_STREAM_STARTING_POSITION", "LATEST"),
        start_ts=_start_timestamp(os.environ.get("L2E_STREAM_START_TIMESTAMP")),
    )
    signal.signal(signal.SIGTERM, lambda *_: (log.info("SIGTERM: checkpointing and releasing leases"), consumer.stopping.set()))
    log.info("stream mode on %s %s as %s", kind, source.arn, owner)
    consumer.run()
    return 0


# ------------------------------------------------------------------ schedule mode

def run_schedule(invoker):
    raw = os.environ.get("L2E_SCHEDULE_INPUT")
    if raw:
        event = json.loads(raw)
    else:
        event = {
            "version": "0", "id": str(uuid.uuid4()), "detail-type": "Scheduled Event", "source": "aws.events",
            "account": ACCOUNT, "time": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"), "region": REGION,
            "resources": [r for r in [os.environ.get("L2E_SCHEDULE_RULE_ARN")] if r], "detail": {},
        }
    ok, result = invoker.invoke(event)
    log.info("scheduled run %s: %s", "succeeded" if ok else "failed", json.dumps(result, default=str)[:2000])
    return 0 if ok else 1


def main():
    mode = os.environ.get("L2E_MODE", "http")
    invoker = Invoker(load_handler(os.environ["L2E_HANDLER"]), int(os.environ.get("L2E_CONCURRENCY", "1")))
    if mode == "http":
        serve_http(invoker)
        return 0
    if mode == "sqs":
        return run_sqs(invoker)
    if mode == "stream":
        return run_stream(invoker)
    if mode == "schedule":
        code = run_schedule(invoker)
        os._exit(code)  # do not wait for a timed-out handler thread
    raise SystemExit(f"unknown L2E_MODE {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
