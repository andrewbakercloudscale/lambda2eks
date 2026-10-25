"""Bridge and stream workloads, the AWS resources they need, the cut-over runbook and the Helm chart."""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from lambda2eks import cli
from lambda2eks.cutover import route53_script
from lambda2eks.generate import PAUSED, Target, infra_stack, manifests, s3_queue_configs, worker_policy
from lambda2eks.plan import baseline_plan

from .conftest import SAMPLE


def by_kind(docs):
    return {d["kind"]: d for d in docs}


def env_of(deploy):
    return {e["name"]: e["value"] for e in deploy["spec"]["template"]["spec"]["containers"][0]["env"]}


def test_bridge_worker_starts_paused(sample_specs):
    plan = baseline_plan(sample_specs["order-worker"], "r")
    docs = by_kind(manifests(plan, "ns", "eu-west-1", "pod-identity")["order-worker-s3-replace-me-bucket.yaml"])
    env = env_of(docs["Deployment"])
    assert env["L2E_MODE"] == "sqs" and env["L2E_SQS_UNWRAP"] == "s3"
    assert env["L2E_SQS_QUEUE_URL"] == "https://sqs.eu-west-1.amazonaws.com/123456789012/l2e-order-worker-s3-replace-me-bucket"
    assert docs["Deployment"]["spec"]["replicas"] == 0
    assert docs["ScaledObject"]["metadata"]["annotations"] == {PAUSED: "0"}
    assert docs["ScaledObject"]["spec"]["triggers"][0]["metadata"]["queueLength"] == "1"

    live = by_kind(manifests(plan, "ns", "eu-west-1", "pod-identity", live=True)["order-worker-s3-replace-me-bucket.yaml"])
    assert "annotations" not in live["ScaledObject"]["metadata"] and live["Deployment"]["spec"]["replicas"] == 1


def test_stream_consumer_manifests(sample_specs):
    plan = baseline_plan(sample_specs["stock-sync"], "r")
    docs = by_kind(manifests(plan, "ns", "eu-west-1", "irsa")["stock-sync-dynamodb-stock.yaml"])
    env = env_of(docs["Deployment"])
    assert env["L2E_MODE"] == "stream" and env["L2E_STREAM_KIND"] == "dynamodb" and env["L2E_DDB_TABLE"] == "stock"
    # TRIM_HORIZON on the Lambda would replay a day of records; the cut-over sets a start time instead.
    assert env["L2E_STREAM_STARTING_POSITION"] == "LATEST"
    assert env["L2E_STREAM_MAX_RETRY"] == "3" and env["L2E_STREAM_BISECT"] == "true"
    assert env["L2E_LEASE_TABLE"] == "l2e-stock-sync-leases" and env["L2E_CONSUMER_NAME"] == "stock-sync-dynamodb-stock"
    so = docs["ScaledObject"]["spec"]
    assert so["minReplicaCount"] == 1 and so["triggers"][0]["type"] == "aws-dynamodb-streams"
    assert so["triggers"][0]["metadata"]["tableName"] == "stock"

    kin = by_kind(manifests(baseline_plan(sample_specs["click-ingest"], "r"), "ns", "eu-west-1", "pod-identity",
                            live=True)["click-ingest-kinesis-clicks.yaml"])
    assert kin["ScaledObject"]["spec"]["triggers"][0]["type"] == "aws-kinesis-stream"
    assert env_of(kin["Deployment"])["L2E_STREAM_NAME"] == "clicks"


def test_cronjob_suspended_until_cutover(sample_specs):
    plan = baseline_plan(sample_specs["nightly-report"], "r")
    assert manifests(plan, "ns", "eu-west-1", "pod-identity")["nightly-report-cron.yaml"][0]["spec"]["suspend"] is True
    assert manifests(plan, "ns", "eu-west-1", "pod-identity", live=True)["nightly-report-cron.yaml"][0]["spec"]["suspend"] is False


def test_infra_and_worker_policy(sample_specs):
    plan = baseline_plan(sample_specs["alert-notifier"], "r")
    t = Target(plan, "ns", "eu-west-1", "pod-identity")
    stack = infra_stack(plan, t)
    res = stack["Resources"]
    queue = res["AlertNotifierSnsAlertsQueue"]["Properties"]
    assert queue["RedrivePolicy"]["maxReceiveCount"] == 3 and queue["VisibilityTimeout"] == 60
    sub = res["AlertNotifierSnsAlertsSubscription"]
    assert sub["Condition"] == "AlertNotifierSnsAlertsSubscribed" and sub["Properties"]["FilterPolicy"] == {"severity": ["high", "critical"]}
    assert stack["Parameters"]["AlertNotifierSnsAlertsSubscribe"]["Default"] == "false"
    policy = worker_policy(plan, t)
    assert policy["Statement"][0]["Resource"] == ["arn:aws:sqs:eu-west-1:123456789012:l2e-alert-notifier-sns-alerts"]

    stock = baseline_plan(sample_specs["stock-sync"], "r")
    ts = Target(stock, "ns", "eu-west-1", "pod-identity")
    assert infra_stack(stock, ts)["Resources"]["LeaseTable"]["Properties"]["TableName"] == "l2e-stock-sync-leases"
    assert {s["Sid"] for s in worker_policy(stock, ts)["Statement"]} == {"StreamLeases", "StockSyncDynamodbStock"}

    api = baseline_plan(sample_specs["orders-api"], "r")
    assert infra_stack(api, Target(api, "ns", "eu-west-1", "irsa")) is None

    worker = baseline_plan(sample_specs["order-worker"], "r")
    [cfg] = s3_queue_configs(worker, Target(worker, "ns", "eu-west-1", "irsa"))["REPLACE_ME_BUCKET"]
    assert cfg["Events"] == ["s3:ObjectCreated:*"] and cfg["QueueArn"].endswith(":l2e-order-worker-s3-replace-me-bucket")


@pytest.fixture(scope="module")
def generated(tmp_path_factory):
    out = tmp_path_factory.mktemp("gen") / "out"
    assert cli.main(["from-template", str(SAMPLE), "--out", str(out), "--no-ai", "--format", "both",
                     "--region", "eu-west-1", "--certificate-arn", "arn:aws:acm:eu-west-1:123456789012:certificate/abc"]) == 0
    return out


def test_runbooks(generated):
    api = (generated / "orders-api" / "cutover" / "CUTOVER.md").read_text()
    assert "./cutover/route53-weights.sh set 5" in api and "(done)." in api
    assert os.access(generated / "orders-api" / "cutover" / "route53-weights.sh", os.X_OK)
    ing = by_kind(yaml.safe_load_all((generated / "orders-api" / "k8s" / "orders-api-http.yaml").read_text()))["Ingress"]
    assert ing["metadata"]["annotations"]["alb.ingress.kubernetes.io/ssl-redirect"] == "443"

    worker = (generated / "order-worker" / "cutover" / "CUTOVER.md").read_text()
    assert "aws lambda update-event-source-mapping --uuid \"$UUID\" --no-enabled" in worker
    assert "put-bucket-notification-configuration" in worker and "s3-REPLACE_ME_BUCKET-before.json" in worker
    assert not (generated / "order-worker" / "cutover" / "route53-weights.sh").exists()
    assert (generated / "order-worker" / "infra" / "s3-REPLACE_ME_BUCKET-queue-config.json").is_file()

    sns = (generated / "alert-notifier" / "cutover" / "CUTOVER.md").read_text()
    assert "AlertNotifierSnsAlertsSubscribe=true" in sns and "aws sns unsubscribe" in sns
    stream = (generated / "stock-sync" / "cutover" / "CUTOVER.md").read_text()
    assert "L2E_STREAM_START_TIMESTAMP=$START" in stream and "IteratorAge" in stream
    cron = (generated / "nightly-report" / "cutover" / "CUTOVER.md").read_text()
    assert "aws events disable-rule" in cron and '"suspend":false' in cron

    report = (generated / "alert-notifier" / "MIGRATION.md").read_text()
    assert "SNS subscriptions stay off until the cut-over" in report and "Applying moves no events" in report
    # WEBHOOK names now count as secrets, so the URL never reaches a ConfigMap.
    assert "hooks.slack" not in (generated / "alert-notifier" / "k8s" / "configmap.yaml").read_text()


def all_docs(path):
    docs = []
    for f in sorted(path.rglob("*.yaml")):
        if f.name != "kustomization.yaml":
            docs += [d for d in yaml.safe_load_all(f.read_text()) if d]
    return docs


def key(d):
    return d["kind"], d["metadata"]["name"]


@pytest.mark.skipif(not shutil.which("helm"), reason="helm is not installed")
def test_helm_chart_defaults_render_the_kustomize_objects(generated):
    for fn in ("orders-api", "order-worker", "nightly-report", "stock-sync", "click-ingest", "alert-notifier"):
        lint = subprocess.run(["helm", "lint", str(generated / fn / "helm")], capture_output=True, text=True)
        assert lint.returncode == 0, lint.stdout + lint.stderr
        rendered = subprocess.run(["helm", "template", fn, str(generated / fn / "helm")], capture_output=True, text=True)
        assert rendered.returncode == 0, rendered.stderr
        helm_docs = sorted((d for d in yaml.safe_load_all(rendered.stdout) if d), key=key)
        assert helm_docs == sorted(all_docs(generated / fn / "k8s"), key=key), fn

    # Values reach the objects: unpause a worker and change its ceiling.
    rendered = subprocess.run(["helm", "template", "w", str(generated / "order-worker" / "helm"),
                               "--set", "workloads.order-worker-sqs-orders.paused=false",
                               "--set", "workloads.order-worker-sqs-orders.maxReplicas=7"], capture_output=True, text=True, check=True)
    objs = {key(d): d for d in yaml.safe_load_all(rendered.stdout) if d}
    so = objs[("ScaledObject", "order-worker-sqs-orders")]
    assert so["metadata"]["annotations"] == {} and so["spec"]["maxReplicaCount"] == 7
    assert objs[("Deployment", "order-worker-sqs-orders")]["spec"]["replicas"] == 1


CRD_SCHEMAS = "https://raw.githubusercontent.com/datreeio/CRDs-catalog/main/{{.Group}}/{{.ResourceKind}}_{{.ResourceAPIVersion}}.json"


@pytest.mark.skipif(not shutil.which("kubeconform"), reason="kubeconform is not installed (brew install kubeconform)")
def test_kubeconform_strict(generated):
    """Every generated object validates against the Kubernetes and KEDA schemas. A missing schema is a failure."""
    cache = Path(__file__).resolve().parent / ".kubeconform-cache"
    cache.mkdir(exist_ok=True)
    files = [str(f) for f in sorted(generated.rglob("k8s/*.yaml")) if f.name != "kustomization.yaml"]
    proc = subprocess.run(["kubeconform", "-strict", "-summary", "-output", "json", "-cache", str(cache),
                           "-kubernetes-version", "1.33.0", "-schema-location", "default", "-schema-location", CRD_SCHEMAS,
                           *files], capture_output=True, text=True, timeout=300)
    report = json.loads(proc.stdout)
    bad = [r for r in report["resources"] if r["status"] != "statusValid"]
    assert proc.returncode == 0 and not bad, json.dumps(bad, indent=1)[:3000]
    expected = len([d for f in files for d in yaml.safe_load_all(Path(f).read_text()) if d])
    summary = report["summary"]
    assert summary["valid"] == expected and summary["skipped"] == 0, summary


# ------------------------------------------------------------------ the Route 53 script, against a fake AWS CLI

FAKE_AWS = r'''
import json, os, sys
state = os.environ["FAKE_STATE"]
records = json.load(open(state))
args = sys.argv[1:]
def opt(name):
    return args[args.index(name) + 1]
if args[:2] == ["route53", "list-resource-record-sets"]:
    print(json.dumps({"ResourceRecordSets": records}))
elif args[:2] == ["route53", "change-resource-record-sets"]:
    batch = json.loads(opt("--change-batch"))
    log = json.load(open(state + ".log")) if os.path.exists(state + ".log") else []
    json.dump(log + [batch], open(state + ".log", "w"))
    for c in batch["Changes"]:
        rr = c["ResourceRecordSet"]
        same = lambda r: (r["Name"], r["Type"], r.get("SetIdentifier")) == (rr["Name"], rr["Type"], rr.get("SetIdentifier"))
        if c["Action"] == "DELETE":
            assert any(r == rr for r in records), "DELETE must match the record exactly"
            records = [r for r in records if r != rr]
        elif c["Action"] == "CREATE":
            assert not any(same(r) for r in records), "CREATE of an existing record"
            records.append(rr)
        else:
            records = [r for r in records if not same(r)] + [rr]
    simple = [r for r in records if "SetIdentifier" not in r]
    assert not (simple and len(records) > len(simple)), "simple and weighted records cannot coexist"
    json.dump(records, open(state, "w"))
    print("C123")
elif args[:2] == ["route53", "wait"]:
    pass
elif args[:2] == ["elbv2", "describe-load-balancers"]:
    print("ZALB")
else:
    sys.exit(f"unexpected aws call {args}")
'''


@pytest.mark.skipif(not shutil.which("jq"), reason="jq is not installed")
def test_route53_script_full_cycle(tmp_path):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "aws").write_text(f"#!/bin/sh\nexec '{sys.executable}' '{tmp_path / 'fake_aws.py'}' \"$@\"\n")
    (bin_dir / "kubectl").write_text("#!/bin/sh\necho alb-123.eu-west-1.elb.amazonaws.com\n")
    for f in bin_dir.iterdir():
        f.chmod(0o755)
    (tmp_path / "fake_aws.py").write_text(FAKE_AWS)
    original = {"Name": "api.example.com.", "Type": "A",
                "AliasTarget": {"HostedZoneId": "ZAPI", "DNSName": "d-1.execute-api.eu-west-1.amazonaws.com.", "EvaluateTargetHealth": False}}
    state = tmp_path / "records.json"
    state.write_text(json.dumps([original]))
    script = tmp_path / "cutover" / "route53-weights.sh"
    script.parent.mkdir()
    script.write_text(route53_script("ns", "orders-api-http"))
    script.chmod(0o755)
    env = {**os.environ, "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_STATE": str(state), "NO_COLOR": "1",
           "HOSTED_ZONE_ID": "Z1", "RECORD_NAME": "api.example.com"}

    def run(*args, ok=True):
        p = subprocess.run(["bash", str(script), *args], env=env, capture_output=True, text=True, timeout=30)
        assert (p.returncode == 0) == ok, p.stdout + p.stderr
        return json.loads(state.read_text())

    def weights(records):
        return {r.get("SetIdentifier", "simple"): r.get("Weight") for r in records}

    run("set", "10", ok=False)  # nothing to weight yet
    assert weights(run("init")) == {"lambda": 100, "eks": 0}
    eks = next(r for r in json.loads(state.read_text()) if r["SetIdentifier"] == "eks")
    assert eks["AliasTarget"] == {"HostedZoneId": "ZALB", "DNSName": "alb-123.eu-west-1.elb.amazonaws.com", "EvaluateTargetHealth": True}
    assert weights(run("set", "25")) == {"lambda": 75, "eks": 25}
    run("finish", ok=False)  # not at 100% yet
    run("set", "100")
    [final] = run("finish")
    assert final["AliasTarget"]["DNSName"].startswith("alb-123") and "SetIdentifier" not in final
    assert run("rollback") == [original]
    run("set", "101", ok=False)
