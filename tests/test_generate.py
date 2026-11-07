import json

import yaml

from lambda2eks import ai, cli
from lambda2eks.generate import dockerfile, manifests, trust_policy
from lambda2eks.plan import baseline_plan

from .conftest import SAMPLE


def load_all(path):
    return [d for d in yaml.safe_load_all(path.read_text()) if d]


def by_kind(docs):
    return {d["kind"]: d for d in docs}


def test_manifests_per_trigger(sample_specs):
    api = by_kind(manifests(baseline_plan(sample_specs["orders-api"], "r"), "ns", "eu-west-1", "pod-identity")["orders-api-http.yaml"])
    assert set(api) == {"Deployment", "Service", "HorizontalPodAutoscaler", "PodDisruptionBudget", "Ingress"}
    env = {e["name"]: e["value"] for e in api["Deployment"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["L2E_MODE"] == "http" and env["L2E_HTTP_EVENT_FORMAT"] == "2.0" and env["AWS_REGION"] == "eu-west-1"
    assert [p["path"] for p in api["Ingress"]["spec"]["rules"][0]["http"]["paths"]] == ["/orders"]

    worker = by_kind(manifests(baseline_plan(sample_specs["order-worker"], "r"), "ns", "eu-west-1", "irsa")["order-worker-sqs-orders.yaml"])
    so = worker["ScaledObject"]["spec"]
    assert so["minReplicaCount"] == 0 and so["maxReplicaCount"] == 5
    assert so["triggers"][0]["metadata"]["queueURL"].endswith("/orders")
    assert worker["TriggerAuthentication"]["spec"]["podIdentity"] == {"provider": "aws", "identityOwner": "workload"}
    pod = worker["Deployment"]["spec"]["template"]["spec"]
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "arm64"}

    cron = manifests(baseline_plan(sample_specs["nightly-report"], "r"), "ns", "eu-west-1", "pod-identity")["nightly-report-cron.yaml"][0]
    assert cron["spec"]["schedule"] == "30 2 * * MON-FRI" and cron["spec"]["timeZone"] == "Etc/UTC"
    assert cron["spec"]["jobTemplate"]["spec"]["backoffLimit"] == 2


def test_node_image_resolves_the_runtime_sdk(sample_specs):
    # Lambda's bootstrap sets NODE_PATH (Lambda's Node honours it for ES modules) and sizes the heap.
    # The shim replaces the bootstrap, so the image and pod must do both, or `import '@aws-sdk/...'` fails.
    plan = baseline_plan(sample_specs["order-worker"], "r")
    assert ("ENV NODE_PATH=/opt/nodejs/node20/node_modules:/opt/nodejs/node_modules:"
            "/var/runtime/node_modules:/var/runtime:/var/task") in dockerfile(plan, src_present=True)
    deploy = by_kind(manifests(plan, "ns", "eu-west-1", "pod-identity")["order-worker-sqs-orders.yaml"])["Deployment"]
    env = {e["name"]: e["value"] for e in deploy["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["NODE_OPTIONS"] == "--max-semi-space-size=17 --max-old-space-size=922"  # 1024 MB, as Lambda computes it
    python = baseline_plan(sample_specs["orders-api"], "r")
    assert "NODE_PATH" not in dockerfile(python, src_present=True)


def test_secret_values_never_written(sample_specs):
    files = manifests(baseline_plan(sample_specs["orders-api"], "r"), "ns", "eu-west-1", "pod-identity")
    assert files["secret.yaml"][0]["stringData"] == {"STRIPE_API_KEY": "REPLACE_ME"}
    assert "STRIPE_API_KEY" not in files["configmap.yaml"][0]["data"]
    assert "sk_test_do_not_ship" not in json.dumps(files)


def test_trust_policy_keeps_lambda(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    principals = [s["Principal"] for s in trust_policy(plan, "ns", "pod-identity")["Statement"]]
    assert {"Service": "lambda.amazonaws.com"} in principals and {"Service": "pods.eks.amazonaws.com"} in principals
    irsa = trust_policy(plan, "ns", "irsa")["Statement"][1]
    assert any(v == "system:serviceaccount:ns:orders-api" for v in irsa["Condition"]["StringEquals"].values())


def test_cli_end_to_end_with_ai(tmp_path, monkeypatch):
    def fake_review(plan, model, effort, provider):
        plan.ai_model = model
        settings = {k: {"value": None, "reason": "fine"} for k in ai.SETTINGS}
        if plan.spec.name == "orders-api":
            settings["concurrency"] = {"value": 4, "reason": "I/O-bound and stateless apart from a demo cache"}
        return {"summary": "ok", "migration_readiness": "ready_with_changes", "concurrency_safe": True,
                "settings": settings, "secret_env_keys": [], "iam_actions": [],
                "findings": [{"severity": "blocker", "title": "module cache", "detail": "d", "file": "app.py", "line": 6}],
                "code_changes": [{"file": "app.py", "description": "drop cache", "patch": "--- a/app.py\n+++ b/app.py\n"}],
                "trigger_advice": [{"trigger": "s3", "recommendation": "send S3 events to SQS"}]}

    monkeypatch.setattr(ai, "review", fake_review)
    out = tmp_path / "out"
    assert cli.main(["from-template", str(SAMPLE), "--out", str(out), "--provider", "anthropic"]) == 0
    report = (out / "orders-api" / "MIGRATION.md").read_text()
    assert "| concurrency | 4 | AI (rule said 1) |" in report
    assert "1 blocker(s)" in report and "ai/patches/01-app.py.diff" in report
    assert (out / "orders-api" / "ai" / "patches" / "01-app.py.diff").is_file()
    worker = (out / "order-worker" / "MIGRATION.md").read_text()
    assert "via bridge queue l2e-order-worker-s3-" in worker and "Trigger not migrated" not in worker
    assert (out / "order-worker" / "cutover" / "CUTOVER.md").is_file()
    deploy = by_kind(load_all(out / "orders-api" / "k8s" / "orders-api-http.yaml"))["Deployment"]
    env = {e["name"]: e["value"] for e in deploy["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["L2E_CONCURRENCY"] == "4"
    kust = load_all(out / "orders-api" / "k8s" / "kustomization.yaml")[0]
    for name in kust["resources"]:
        assert (out / "orders-api" / "k8s" / name).is_file()
    assert "&id" not in (out / "orders-api" / "k8s" / "orders-api-http.yaml").read_text()


def test_cli_fails_loudly_when_ai_fails(tmp_path, monkeypatch):
    def broken(plan, model, effort, provider):
        raise ai.AIReviewError("no credentials")

    monkeypatch.setattr(ai, "review", broken)
    out = tmp_path / "out"
    assert cli.main(["from-template", str(SAMPLE), "--out", str(out), "--function", "orders-api", "--provider", "anthropic"]) == 1
    assert "AI REVIEW FAILED: no credentials" in (out / "orders-api" / "MIGRATION.md").read_text()
