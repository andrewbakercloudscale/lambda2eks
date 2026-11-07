#!/usr/bin/env python3
"""Migrate the test Lambdas onto Rancher Desktop's Kubernetes and prove that each one works.

Rancher Desktop plays EKS and LocalStack plays the AWS account. The manifests deployed are the ones
lambda2eks generates, with a small kustomize overlay per function for what a laptop lacks (see
OVERLAY below): no ECR, no ALB controller, no Pod Identity.

    run.py up        install LocalStack and KEDA on the rancher-desktop cluster
    run.py seed      create the existing "AWS estate" in LocalStack: tables, queue, bucket, topic, stream
    run.py migrate   lambda2eks -> images -> bridge queues and lease tables -> S3/SNS cut-over -> kubectl apply
    run.py verify    send real events through every trigger and check what each function recorded
    run.py all       up, seed, migrate, verify
    run.py down      delete the migrated workloads (--all also removes LocalStack and KEDA)

Run it with the repo's virtualenv: .venv/bin/python examples/test-lambdas/rancher/run.py all
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path

import boto3
import yaml
from botocore.exceptions import ClientError

HERE = Path(__file__).resolve().parent
EXAMPLE = HERE.parent
TEMPLATE = EXAMPLE / "template.yaml"
OUT = EXAMPLE / "out"
OVERLAYS = OUT / "_rancher"

CONTEXT = "rancher-desktop"
NS = "lambda-migration"
REGION, ACCOUNT = "us-east-1", "000000000000"
REGISTRY = "lambda2eks.local"
KEDA_CHART_VERSION = "2.21.0"
ENDPOINT_IN_CLUSTER = "http://localstack.localstack.svc.cluster.local:4566"
ENDPOINT_LOCAL = "http://localhost:4566"
INGRESS = "http://127.0.0.1"

RESULTS_TABLE = "l2e-test-results"
TODO_TABLE = "l2e-test-todos"
INVENTORY_TABLE = "l2e-test-inventory"
ORDERS_QUEUE = "l2e-test-orders"
UPLOAD_BUCKET = "l2e-test-uploads"
ALERTS_TOPIC = "l2e-test-alerts"
CLICKS_STREAM = "l2e-test-clicks"


def log(msg: str) -> None:
    print(f"==> {msg}", file=sys.stderr, flush=True)


def run(*cmd: str, check: bool = True, capture: bool = False, **kw) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture, **kw)


def kubectl(*args: str, **kw) -> subprocess.CompletedProcess:
    # Always the rancher-desktop context, so nothing here can touch another cluster.
    return run("kubectl", "--context", CONTEXT, *args, **kw)


def aws(service: str):
    return boto3.client(service, endpoint_url=ENDPOINT_LOCAL, region_name=REGION,
                        aws_access_key_id="test", aws_secret_access_key="test")


def wait_until(what: str, probe, timeout: float, every: float = 2.0):
    deadline = time.time() + timeout
    while True:
        try:
            value = probe()
        except Exception:  # noqa: BLE001 - keep polling until the deadline
            value = None
        if value:
            return value
        if time.time() > deadline:
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")
        time.sleep(every)


# ------------------------------------------------------------------ up

def up(_: argparse.Namespace) -> None:
    if CONTEXT not in kubectl("config", "get-contexts", "-o", "name", capture=True).stdout.split():
        raise SystemExit(f"no kubectl context {CONTEXT!r}: enable Kubernetes in Rancher Desktop (rdctl set --kubernetes-enabled=true)")
    log("LocalStack (stands in for the AWS account)")
    kubectl("apply", "-f", str(HERE / "platform.yaml"))
    log(f"KEDA {KEDA_CHART_VERSION} (scales the queue and stream workers)")
    run("helm", "repo", "add", "kedacore", "https://kedacore.github.io/charts", "--force-update", capture=True)
    run("helm", "--kube-context", CONTEXT, "upgrade", "--install", "keda", "kedacore/keda", "--version", KEDA_CHART_VERSION,
        "--namespace", "keda", "--create-namespace", "--wait", "--timeout", "5m")
    kubectl("-n", "localstack", "rollout", "status", "deploy/localstack", "--timeout=300s")
    wait_until("LocalStack on localhost:4566",
               lambda: json.load(urllib.request.urlopen(f"{ENDPOINT_LOCAL}/_localstack/health", timeout=3)), 120)
    log("platform ready")


# ------------------------------------------------------------------ seed

def _ignore_exists(call, *codes: str):
    try:
        return call()
    except ClientError as e:
        if e.response["Error"]["Code"] not in codes:
            raise
        return None


def seed(_: argparse.Namespace) -> None:
    """The resources the Lambdas already use in AWS. lambda2eks does not create these; it migrates onto them."""
    ddb, sqs, s3, sns, kinesis = aws("dynamodb"), aws("sqs"), aws("s3"), aws("sns"), aws("kinesis")
    exists = ("ResourceInUseException",)
    log("DynamoDB tables")
    _ignore_exists(lambda: ddb.create_table(
        TableName=RESULTS_TABLE, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "fn", "AttributeType": "S"}, {"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "fn", "KeyType": "HASH"}, {"AttributeName": "id", "KeyType": "RANGE"}]), *exists)
    _ignore_exists(lambda: ddb.create_table(
        TableName=TODO_TABLE, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}]), *exists)
    _ignore_exists(lambda: ddb.create_table(
        TableName=INVENTORY_TABLE, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": "sku", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "sku", "KeyType": "HASH"}],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}), *exists)
    log("SQS orders queue with a dead-letter queue (2 receives)")
    dlq = sqs.create_queue(QueueName=f"{ORDERS_QUEUE}-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    # Visibility = the function timeout, so a failed order comes back quickly enough to watch it dead-letter.
    sqs.create_queue(QueueName=ORDERS_QUEUE, Attributes={
        "VisibilityTimeout": "30",
        "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})})
    log("S3 bucket, SNS topic, Kinesis stream")
    _ignore_exists(lambda: s3.create_bucket(Bucket=UPLOAD_BUCKET), "BucketAlreadyOwnedByYou", "BucketAlreadyExists")
    sns.create_topic(Name=ALERTS_TOPIC)
    _ignore_exists(lambda: kinesis.create_stream(StreamName=CLICKS_STREAM, ShardCount=1), *exists)
    wait_until("Kinesis stream ACTIVE", lambda: kinesis.describe_stream_summary(StreamName=CLICKS_STREAM)
               ["StreamDescriptionSummary"]["StreamStatus"] == "ACTIVE", 60)
    log("AWS estate seeded")


# ------------------------------------------------------------------ migrate

def plans():
    sys.path.insert(0, str(EXAMPLE.parent.parent))
    from lambda2eks.loaders.template import load_template
    from lambda2eks.plan import baseline_plan
    return [p for p in (baseline_plan(s, REGISTRY) for s in load_template(TEMPLATE)) if p.supported]


def _container_ops(base: str) -> list[dict]:
    env = [{"name": "AWS_ENDPOINT_URL", "value": ENDPOINT_IN_CLUSTER},
           {"name": "AWS_ACCESS_KEY_ID", "value": "test"}, {"name": "AWS_SECRET_ACCESS_KEY", "value": "test"}]
    return [{"op": "add", "path": f"{base}/env/-", "value": e} for e in env] + \
           [{"op": "add", "path": f"{base}/imagePullPolicy", "value": "Never"}]


# OVERLAY: the only differences between what runs here and what lambda2eks generated for EKS.
def overlay(plan) -> dict:
    """kustomization.yaml on top of out/<fn>/k8s: LocalStack endpoint and keys, local images, Traefik, KEDA keys."""
    base = OUT / plan.k8s_name / "k8s"
    docs = [d for f in sorted(base.glob("*.yaml")) if f.name != "kustomization.yaml"
            for d in yaml.safe_load_all(f.read_text()) if d]
    patches = []

    def patch(doc: dict, ops: list[dict]) -> None:
        patches.append({"target": {"kind": doc["kind"], "name": doc["metadata"]["name"]}, "patch": yaml.safe_dump(ops)})

    for d in docs:
        kind = d["kind"]
        if kind == "Deployment":
            ops = _container_ops("/spec/template/spec/containers/0")
            # The JS SDK sends SQS calls to the queue URL's host, not AWS_ENDPOINT_URL, so the AWS-style
            # URL would go to the real sqs.<region>.amazonaws.com. Give it LocalStack's form of the URL.
            for i, e in enumerate(d["spec"]["template"]["spec"]["containers"][0]["env"]):
                if e["name"] == "L2E_SQS_QUEUE_URL":
                    ops.append({"op": "replace", "path": f"/spec/template/spec/containers/0/env/{i}/value",
                                "value": f"{ENDPOINT_IN_CLUSTER}/{ACCOUNT}/{e['value'].rsplit('/', 1)[-1]}"})
            patch(d, ops)
        elif kind == "CronJob":
            patch(d, _container_ops("/spec/jobTemplate/spec/template/spec/containers/0"))
        elif kind == "Ingress":
            patch(d, [{"op": "replace", "path": "/spec/ingressClassName", "value": "traefik"}])
        elif kind == "TriggerAuthentication":
            patch(d, [{"op": "remove", "path": "/spec/podIdentity"},
                      {"op": "add", "path": "/spec/secretTargetRef", "value": [
                          {"parameter": "awsAccessKeyID", "name": "localstack-credentials", "key": "AWS_ACCESS_KEY_ID"},
                          {"parameter": "awsSecretAccessKey", "name": "localstack-credentials", "key": "AWS_SECRET_ACCESS_KEY"}]}])
        elif kind == "ScaledObject":
            patch(d, [{"op": "add", "path": "/spec/triggers/0/metadata/awsEndpoint", "value": ENDPOINT_IN_CLUSTER}])
        elif kind == "Secret":
            # On EKS these come from Secrets Manager; here, the value the Lambda had in its configuration.
            patch(d, [{"op": "replace", "path": f"/stringData/{k}", "value": plan.spec.environment[k]}
                      for k in d.get("stringData", {})])
    return {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
            "resources": [os.path.relpath(base, OVERLAYS / plan.k8s_name)], "patches": patches}


def deploy_infra(plan, cfn) -> None:
    """infra/resources.yaml (bridge queues, SNS subscription, lease table) as a CloudFormation stack in LocalStack."""
    path = OUT / plan.k8s_name / "infra" / "resources.yaml"
    if not path.is_file():
        return
    body = path.read_text()
    template = yaml.safe_load(body)
    # <Id>Subscribe=true is the SNS cut-over step: there is no Lambda subscription here to overlap with.
    params = [{"ParameterKey": k, "ParameterValue": "true"} for k in template.get("Parameters", {}) if k.endswith("Subscribe")]
    name = f"l2e-{plan.k8s_name}"
    try:
        cfn.create_stack(StackName=name, TemplateBody=body, Parameters=params)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
        try:
            cfn.update_stack(StackName=name, TemplateBody=body, Parameters=params)
        except ClientError as e2:
            if "No updates" not in str(e2):
                raise
    status = wait_until(f"stack {name}", lambda: (s := cfn.describe_stacks(StackName=name)["Stacks"][0]["StackStatus"])
                        and not s.endswith("IN_PROGRESS") and s, 120)
    if status not in ("CREATE_COMPLETE", "UPDATE_COMPLETE"):
        raise SystemExit(f"stack {name} ended in {status}")
    log(f"  {name}: {status}")


def s3_cutover(plan) -> None:
    """Point the bucket's notifications at the bridge queue (the S3 step of cutover/CUTOVER.md)."""
    from lambda2eks.generate import Target, s3_queue_configs
    s3 = aws("s3")
    for bucket, configs in s3_queue_configs(plan, Target(plan, NS, REGION, "pod-identity", live=True)).items():
        current = s3.get_bucket_notification_configuration(Bucket=bucket)
        current.pop("ResponseMetadata", None)
        ids = {c["Id"] for c in configs}
        current["QueueConfigurations"] = [c for c in current.get("QueueConfigurations", []) if c.get("Id") not in ids] + configs
        s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=current)
        log(f"  s3://{bucket} notifications -> {', '.join(c['QueueArn'].split(':')[-1] for c in configs)}")


def migrate(args: argparse.Namespace) -> None:
    cmd = [sys.executable, "-m", "lambda2eks.cli", "from-template", str(TEMPLATE), "--out", str(OUT),
           "--registry", REGISTRY, "--region", REGION, "--namespace", NS,
           # --live: workers and CronJobs start active. Nothing runs on the Lambda side here, so there is no
           # overlap to protect against; on a real migration leave it off and follow cutover/CUTOVER.md.
           "--live"]
    if not args.ai:
        cmd.append("--no-ai")
    log("lambda2eks " + " ".join(cmd[3:]))
    run(*cmd, cwd=EXAMPLE.parent.parent)

    cfn = aws("cloudformation")
    for plan in plans():
        name = plan.k8s_name
        log(f"{plan.spec.name}: image {plan.image}")
        run("docker", "build", "-q", "--platform", f"linux/{plan.spec.arch}", "-t", plan.image, str(OUT / name), capture=True)
        deploy_infra(plan, cfn)
        s3_cutover(plan)
        overlay_dir = OVERLAYS / name
        overlay_dir.mkdir(parents=True, exist_ok=True)
        (overlay_dir / "kustomization.yaml").write_text(yaml.safe_dump(overlay(plan), sort_keys=False))
        selector = f"app.kubernetes.io/name={name}"
        existed = kubectl("-n", NS, "get", "deploy", "-l", selector, "-o", "name", capture=True).stdout.split()
        kubectl("apply", "-k", str(overlay_dir), capture=True)
        if existed:  # a re-run: pick up the rebuilt image, which has the same tag
            kubectl("-n", NS, "rollout", "restart", *existed, capture=True)
    log("waiting for the deployments")
    for d in kubectl("-n", NS, "get", "deploy", "-l", "app.kubernetes.io/managed-by=lambda2eks", "-o", "name",
                     capture=True).stdout.split():
        kubectl("-n", NS, "rollout", "status", d, "--timeout=240s")
    kubectl("-n", NS, "get", "deploy,cronjob,scaledobject,ingress")


# ------------------------------------------------------------------ verify

def results(fn: str) -> dict[str, dict]:
    items, kwargs = {}, {"TableName": RESULTS_TABLE, "KeyConditionExpression": "fn = :f",
                         "ExpressionAttributeValues": {":f": {"S": fn}}, "ConsistentRead": True}
    while True:
        page = aws("dynamodb").query(**kwargs)
        for it in page["Items"]:
            items[it["id"]["S"]] = {k: next(iter(v.values())) for k, v in it.items()}
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def wait_results(fn: str, ids: set[str], timeout: float = 120) -> dict[str, dict]:
    return wait_until(f"{fn} to record {sorted(ids)}", lambda: (r := results(fn)) and ids <= r.keys() and r, timeout, 3)


def http(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(url, method=method, data=json.dumps(body).encode() if body is not None else None,
                                 headers={"content-type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read() or b"{}")


@contextmanager
def port_forward(service: str, port: int):
    proc = subprocess.Popen(["kubectl", "--context", CONTEXT, "-n", NS, "port-forward", f"svc/{service}", f"{port}:80"],
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        wait_until(f"port-forward to {service}", lambda: socket.create_connection(("127.0.0.1", port), 1).close() or True, 30, 0.5)
        yield f"http://127.0.0.1:{port}"
    finally:
        proc.terminate()


def workload(fn: str, kind: str) -> str:
    plan = next(p for p in plans() if p.spec.name == fn)
    return next(w.name for w in plan.workloads if w.kind == kind)


def check_greeter(rid: str) -> str:
    status, body = http("GET", f"{INGRESS}/greet/Ada?lang=fr")
    assert status == 200 and body["message"] == "Bonjour, Ada!", f"GET /greet/Ada -> {status} {body}"
    status, body = http("POST", f"{INGRESS}/greet", {"id": rid, "name": "Ada"})
    assert status == 201, f"POST /greet -> {status} {body}"
    status, _ = http("POST", f"{INGRESS}/greet", {})
    assert status == 400, f"POST /greet without a name -> {status}, wanted 400"
    wait_results("greeter-api", {rid}, 10)
    return "GET path+query 200, POST 201 recorded, bad POST 400 (payload 2.0 via Traefik)"


def check_todo(rid: str) -> str:
    status, body = http("POST", f"{INGRESS}/todos", {"id": rid, "title": "migrate to EKS"})
    assert status == 201, f"POST /todos -> {status} {body}"
    status, body = http("GET", f"{INGRESS}/todos/{rid}")
    assert status == 200 and body["title"] == "migrate to EKS", f"GET /todos/{rid} -> {status} {body}"
    status, body = http("GET", f"{INGRESS}/todos")
    assert status == 200 and any(t["id"] == rid for t in body["todos"]), f"GET /todos -> {status}"
    status, _ = http("GET", f"{INGRESS}/todos/missing-{rid}")
    assert status == 404, f"GET missing todo -> {status}, wanted 404"
    return "POST 201, GET by id, list, 404 (payload 1.0, DynamoDB via SDK v3)"


def check_orders(rid: str) -> str:
    sqs = aws("sqs")
    url = sqs.get_queue_url(QueueName=ORDERS_QUEUE)["QueueUrl"]
    dlq = sqs.get_queue_url(QueueName=f"{ORDERS_QUEUE}-dlq")["QueueUrl"]
    good = {f"{rid}-{i}" for i in range(3)}
    bad = f"{rid}-bad"
    for oid in sorted(good):
        sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"order_id": oid, "qty": 2, "unit_price": 9.99}))
    sqs.send_message(QueueUrl=url, MessageBody=json.dumps({"order_id": bad, "qty": 0, "unit_price": 1}))
    got = wait_results("order-processor", good, 180)
    assert all(got[o]["total"] == "19.98" for o in good), f"totals {[got[o]['total'] for o in good]}"

    def dead_lettered():
        msgs = sqs.receive_message(QueueUrl=dlq, MaxNumberOfMessages=10, WaitTimeSeconds=1).get("Messages", [])
        hit = [m for m in msgs if json.loads(m["Body"]).get("order_id") == bad]
        for m in hit:
            sqs.delete_message(QueueUrl=dlq, ReceiptHandle=m["ReceiptHandle"])
        return bool(hit)
    wait_until(f"order {bad} in the DLQ", dead_lettered, 150, 5)
    assert bad not in results("order-processor"), "the failed order was recorded"
    return "KEDA scaled from 0; 3 orders recorded; qty=0 reported as a batch item failure and dead-lettered"


def check_uploads(rid: str) -> str:
    s3 = aws("s3")
    s3.put_object(Bucket=UPLOAD_BUCKET, Key=f"incoming/{rid}.txt", Body=b"hello eks")
    s3.put_object(Bucket=UPLOAD_BUCKET, Key=f"ignored/{rid}.txt", Body=b"outside the prefix")
    got = wait_results("upload-indexer", {f"incoming/{rid}.txt"}, 180)[f"incoming/{rid}.txt"]
    assert got["size"] == "9" and got["event"].startswith("ObjectCreated"), got
    time.sleep(5)
    assert f"ignored/{rid}.txt" not in results("upload-indexer"), "prefix filter not applied"
    return "S3 -> bridge queue -> rebuilt S3 event; prefix filter kept"


def check_alerts(rid: str) -> str:
    sns = aws("sns")
    arn = f"arn:aws:sns:{REGION}:{ACCOUNT}:{ALERTS_TOPIC}"

    def publish(sev):
        return sns.publish(TopicArn=arn, Message=json.dumps({"id": f"{rid}-{sev}"}),
                           MessageAttributes={"severity": {"DataType": "String", "StringValue": sev}})["MessageId"]
    high, low = publish("high"), publish("low")
    got = wait_results("alert-router", {high}, 180)[high]
    assert got["severity"] == "high" and got["alert"] == f"{rid}-high", got
    time.sleep(5)
    assert low not in results("alert-router"), "severity=low got through the filter policy"
    return "SNS -> bridge queue -> rebuilt SNS event; filter policy dropped severity=low"


def check_inventory(rid: str) -> str:
    ddb = aws("dynamodb")
    sku = f"SKU-{rid}"
    ddb.put_item(TableName=INVENTORY_TABLE, Item={"sku": {"S": sku}, "stock": {"N": "5"}})
    ddb.update_item(TableName=INVENTORY_TABLE, Key={"sku": {"S": sku}}, UpdateExpression="SET stock = :s",
                    ExpressionAttributeValues={":s": {"N": "3"}})
    ddb.put_item(TableName=INVENTORY_TABLE, Item={"sku": {"S": f"BAD-{rid}"}, "stock": {"N": "1"}})

    def changes():
        rows = [r for r in results("inventory-audit").values() if r["sku"] == sku]
        return rows if len(rows) >= 2 else None
    rows = wait_until(f"audit rows for {sku}", changes, 180, 3)
    assert sorted((r["change"], r["stock"]) for r in rows) == [("INSERT", "5"), ("MODIFY", "3")], rows
    assert not any(r["sku"] == f"BAD-{rid}" for r in results("inventory-audit").values())
    return "INSERT + MODIFY audited; BAD- record failed alone (partial batch)"


def check_clicks(rid: str) -> str:
    kinesis, ddb = aws("kinesis"), aws("dynamodb")
    consumer = workload("clickstream-counter", "kinesis")
    table = "l2e-clickstream-counter-leases"  # Target.lease_table()

    def lease_held():
        items = ddb.scan(TableName=table, ConsistentRead=True)["Items"]
        return any(i["shard"]["S"].startswith(f"{consumer}#shardId") and "owner" in i
                   and float(i["expires"]["N"]) > time.time() for i in items)
    # StartingPosition LATEST: records written before the consumer holds its shard are not for it.
    wait_until("the Kinesis consumer to hold its shard lease", lease_held, 120, 3)
    time.sleep(5)
    ids = {f"{rid}-{i}" for i in range(5)}
    for cid in sorted(ids):
        kinesis.put_record(StreamName=CLICKS_STREAM, PartitionKey=cid,
                           Data=json.dumps({"click_id": cid, "page": "/pricing"}).encode())
    wait_results("clickstream-counter", ids, 120)
    return "5 records via the shard-lease consumer (Kinesis, LATEST)"


def check_cleanup(rid: str) -> str:
    cron = workload("nightly-cleanup", "schedule")
    start = time.time() - 1
    job = f"verify-{rid}"
    kubectl("-n", NS, "create", "job", job, f"--from=cronjob/{cron}", capture=True)
    kubectl("-n", NS, "wait", f"job/{job}", "--for=condition=complete", "--timeout=180s", capture=True)
    runs = [r for r in results("nightly-cleanup").values() if int(r["at"]) >= start]
    assert runs and runs[0]["task"] == "expire-todos" and runs[0]["older_than_days"] == "30", runs
    kubectl("-n", NS, "delete", "job", job, "--wait=false", capture=True, check=False)
    return "Job from the CronJob ran with the rule's constant input"


def check_price(rid: str) -> str:
    with port_forward(workload("price-calculator", "invoke"), 18081) as base:
        status, body = http("POST", f"{base}/2015-03-31/functions/price-calculator/invocations",
                            {"quote_id": rid, "items": [{"qty": 2, "unit_price": 10}, {"qty": 1, "unit_price": 5}]})
    assert status == 200 and body["total"] == 28.75, f"invoke -> {status} {body}"
    wait_results("price-calculator", {rid}, 10)
    return "Invoke API path; secret env from the Secret; 25.00 + 15% tax = 28.75"


def check_audit(rid: str) -> str:
    report = (OUT / "audit-shipper" / "MIGRATION.md").read_text()
    assert "Trigger not migrated: SAM event Signins (EventBridgeRule)" in report, "MIGRATION.md does not report the EventBridge rule"
    event = {"id": rid, "source": "aws.signin", "detail-type": "AWS Console Sign In via CloudTrail",
             "detail": {"userIdentity": {"arn": "arn:aws:iam::000000000000:user/ada"},
                        "responseElements": {"ConsoleLogin": "Success"}}}
    with port_forward(workload("audit-shipper", "invoke"), 18082) as base:
        status, body = http("POST", f"{base}/2015-03-31/functions/audit-shipper/invocations", event)
    assert status == 200 and body == {"shipped": 1}, f"invoke -> {status} {body}"
    assert wait_results("audit-shipper", {rid}, 10)[rid]["result"] == "Success"
    return "trigger reported in MIGRATION.md, not migrated; function answers on the Invoke API"


CHECKS = [
    ("greeter-api", "HTTP API 2.0", check_greeter),
    ("todo-rest", "REST API 1.0", check_todo),
    ("order-processor", "SQS", check_orders),
    ("upload-indexer", "S3", check_uploads),
    ("alert-router", "SNS", check_alerts),
    ("inventory-audit", "DynamoDB stream", check_inventory),
    ("clickstream-counter", "Kinesis", check_clicks),
    ("nightly-cleanup", "Schedule", check_cleanup),
    ("price-calculator", "Invoke (no trigger)", check_price),
    ("audit-shipper", "EventBridge rule", check_audit),
]


def verify(args: argparse.Namespace) -> int:
    from concurrent.futures import ThreadPoolExecutor
    rid = uuid.uuid4().hex[:8]
    log(f"verifying (run {rid})")
    selected = [c for c in CHECKS if not args.only or c[0] in args.only]

    def one(check):
        fn, trigger, f = check
        t0 = time.time()
        try:
            return fn, trigger, True, f(rid), time.time() - t0
        except Exception as e:  # noqa: BLE001 - report every check, not just the first failure
            return fn, trigger, False, f"{type(e).__name__}: {e}", time.time() - t0
    with ThreadPoolExecutor(max_workers=len(selected)) as pool:
        rows = list(pool.map(one, selected))
    print()
    for fn, trigger, ok, detail, secs in rows:
        print(f"{'PASS' if ok else 'FAIL'}  {fn:20} {trigger:20} {secs:5.0f}s  {detail}")
    failed = sum(not r[2] for r in rows)
    print(f"\n{len(rows) - failed}/{len(rows)} passed")
    return 1 if failed else 0


# ------------------------------------------------------------------ down

def down(args: argparse.Namespace) -> None:
    kubectl("-n", NS, "delete", "deploy,svc,ingress,hpa,pdb,cronjob,job,scaledobject,triggerauthentication,configmap,secret,sa",
            "-l", "app.kubernetes.io/managed-by=lambda2eks", "--ignore-not-found")
    if args.all:
        run("helm", "--kube-context", CONTEXT, "uninstall", "keda", "--namespace", "keda", check=False)
        kubectl("delete", "-f", str(HERE / "platform.yaml"), "--ignore-not-found")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("up")
    sub.add_parser("seed")
    for name in ("migrate", "all"):
        s = sub.add_parser(name)
        s.add_argument("--ai", action="store_true", help="run lambda2eks's AI review (needs ANTHROPIC_API_KEY or TOGETHER_API_KEY)")
    for s in (sub.choices["all"], sub.add_parser("verify")):
        s.add_argument("--only", action="append", metavar="FUNCTION", help="verify only this function; repeatable")
    sub.add_parser("down").add_argument("--all", action="store_true", help="also remove LocalStack, KEDA and the namespace")
    args = p.parse_args()
    if args.cmd == "all":
        up(args), seed(args), migrate(args)
        return verify(args)
    return {"up": up, "seed": seed, "migrate": migrate, "verify": verify, "down": down}[args.cmd](args) or 0


if __name__ == "__main__":
    sys.exit(main())
