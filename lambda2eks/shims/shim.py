"""lambda2eks runtime shim for Python handlers.

Replaces the Lambda runtime inside a Lambda base image and calls the unchanged handler.
Modes (L2E_MODE): http (API Gateway events + Invoke API), sqs (queue worker), schedule (one run).
Standard library only, except boto3 in sqs mode (the Lambda Python base image ships it).
"""

import base64
import concurrent.futures
import importlib
import json
import logging
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
INVOKE_PATH = "/2015-03-31/functions/function/invocations"
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
            if url.path == INVOKE_PATH and self.command == "POST":
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


def process_batch(sqs, queue_url, invoker, records, partial):
    ok, result = invoker.invoke({"Records": records})
    if not ok:
        log.error("batch of %d failed; messages return to the queue after the visibility timeout", len(records))
        return
    keep = failed_ids(result, records) if partial else set()
    done = [r for r in records if r["messageId"] not in keep]
    for i in range(0, len(done), 10):
        chunk = done[i:i + 10]
        res = sqs.delete_message_batch(QueueUrl=queue_url, Entries=[
            {"Id": str(n), "ReceiptHandle": r["receiptHandle"]} for n, r in enumerate(chunk)])
        for f in res.get("Failed") or []:
            log.error("delete failed for %s: %s", chunk[int(f["Id"])]["messageId"], f.get("Message"))


def run_sqs(invoker, client=None):
    queue_url = os.environ["L2E_SQS_QUEUE_URL"]
    queue_arn = os.environ.get("L2E_SQS_QUEUE_ARN", "")
    batch = int(os.environ.get("L2E_SQS_BATCH_SIZE", "10"))
    partial = os.environ.get("L2E_SQS_REPORT_BATCH_ITEM_FAILURES", "false") == "true"
    if client is None:
        import boto3
        client = boto3.client("sqs", region_name=REGION)
    sqs = client
    stopping = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: (log.info("SIGTERM: finishing the current batch"), stopping.set()))
    log.info("sqs mode on %s, batch %d, partial failures %s", queue_url, batch, partial)

    def poll():
        while not stopping.is_set():
            try:
                resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=min(batch, 10), WaitTimeSeconds=20,
                                           AttributeNames=["All"], MessageAttributeNames=["All"])
            except Exception:  # noqa: BLE001 - keep polling through throttling and network errors
                log.exception("receive_message failed; retrying in 5s")
                stopping.wait(5)
                continue
            msgs = resp.get("Messages") or []
            if msgs:
                process_batch(sqs, queue_url, invoker, [sqs_record(m, queue_arn) for m in msgs], partial)

    # One poller per concurrency slot, as Lambda runs one batch per environment.
    pollers = [threading.Thread(target=poll, daemon=True) for _ in range(int(os.environ.get("L2E_CONCURRENCY", "1")))]
    for t in pollers:
        t.start()
    for t in pollers:
        t.join()
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
    if mode == "schedule":
        code = run_schedule(invoker)
        os._exit(code)  # do not wait for a timed-out handler thread
    raise SystemExit(f"unknown L2E_MODE {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
