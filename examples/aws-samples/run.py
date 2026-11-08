#!/usr/bin/env python3
"""Deploy AWS's own sample Lambdas to a real account, migrate them with lambda2eks, and check they still work.

    run.py all         start Rancher, deploy, check-lambda, migrate, check-eks, teardown, shut Rancher down
    run.py deploy      package and deploy each sample in samples/ as a CloudFormation stack
    run.py check-lambda   send a marked event through each sample's real trigger; find the marker in CloudWatch
    run.py migrate     lambda2eks from-aws, bridge/lease stacks, cut each trigger over, build, kubectl apply
    run.py check-eks   send the same events again and find the markers in the pods' logs
    run.py teardown    delete every stack, bucket, log group and Kubernetes object this created; shut Rancher down
    run.py stop        shut Rancher Desktop down

Everything is created in one region (default eu-west-1) with the tag lambda2eks-test=true, and `all`
tears it all down at the end even when a step fails (--keep leaves it). Pods on Rancher Desktop reach the
real AWS resources with short-lived STS session credentials of the caller, stored in a Secret.

Needs: the aws CLI with credentials, npm, Rancher Desktop, and the repo's virtualenv with boto3.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import platform
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

import boto3
import yaml
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.exceptions import ClientError

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
SAMPLES_DIR = HERE / "samples"
OUT = HERE / "out"
BUILD, EKS_OUT, OVERLAYS = OUT / "build", OUT / "eks", OUT / "overlays"
NS = "lambda-migration"
REGISTRY = "lambda2eks.local"
TAG = {"Key": "lambda2eks-test", "Value": "true"}
SESSION_SECRET = "aws-session"

# Rancher Desktop lifecycle, kubectl and KEDA come from the LocalStack harness next door.
_spec = importlib.util.spec_from_file_location("rancher", REPO / "examples/test-lambdas/rancher/run.py")
rancher = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rancher)
kubectl, run, log, wait_until = rancher.kubectl, rancher.run, rancher.log, rancher.wait_until

# One per trigger kind. dir: folder under samples/, kind: how check-lambda/check-eks drive it,
# function: the logical id of the function to migrate when the stack has more than one.
SAMPLES = [
    {"dir": "apigw-http-api-lambda", "template": "template.yml", "kind": "http-v2"},
    {"dir": "apigw-rest-api-lambda-python", "kind": "rest"},
    {"dir": "lambda-function-url", "kind": "function-url"},
    {"dir": "sqs-lambda-python-sam", "kind": "sqs"},
    {"dir": "sns-lambda-nodejs-sam", "kind": "sns"},
    {"dir": "dynamodb-lambda", "kind": "dynamodb"},
    {"dir": "kinesis-lambda", "kind": "kinesis"},
    {"dir": "eventbridge-scheduled-lambda", "kind": "schedule", "expect": "Hello World"},
    # Its schedule is named "${AWS::StackName}-schedule", at most 64 characters, so the stack needs a short name.
    {"dir": "eventbridge-schedule-to-lambda-dlq-sam-python", "kind": "scheduler", "expect": "Completed Successfully",
     "stack": "l2e-sample-scheduler-dlq"},
    {"dir": "sfn-lambda-lambda", "kind": "sfn", "function": "FirstFunction"},
    {"dir": "sam-quickstart-s3", "kind": "s3"},
]


class Ctx:
    def __init__(self, region: str, profile: str | None):
        self.region = region
        self.session = boto3.Session(region_name=region, profile_name=profile)
        self.account = self.session.client("sts").get_caller_identity()["Account"]
        self.artifacts = f"l2e-samples-artifacts-{self.account}-{region}"
        self.cli_env = {**os.environ, "AWS_REGION": region, **({"AWS_PROFILE": profile} if profile else {})}

    def client(self, name: str):
        return self.session.client(name)

    def stack(self, sample: dict) -> str:
        return sample.get("stack") or f"l2e-sample-{sample['dir']}"

    def resources(self, sample: dict) -> dict[str, list[str]]:
        """CloudFormation resource type -> physical ids, for one sample's stack."""
        out: dict[str, list[str]] = {}
        for r in self.client("cloudformation").describe_stack_resources(StackName=self.stack(sample))["StackResources"]:
            out.setdefault(r["ResourceType"], []).append(r["PhysicalResourceId"])
        return out

    def function(self, sample: dict) -> str:
        if sample.get("function"):
            return self.client("cloudformation").describe_stack_resource(
                StackName=self.stack(sample), LogicalResourceId=sample["function"])["StackResourceDetail"]["PhysicalResourceId"]
        return self.resources(sample)["AWS::Lambda::Function"][0]

    def outputs(self, sample: dict) -> dict[str, str]:
        s = self.client("cloudformation").describe_stacks(StackName=self.stack(sample))["Stacks"][0]
        return {o["OutputKey"]: o["OutputValue"] for o in s.get("Outputs", [])}


def _cfn_load(path: Path) -> dict:
    class Loader(yaml.SafeLoader):
        pass
    Loader.add_multi_constructor("!", lambda loader, suffix, node: None)
    return yaml.load(path.read_text(), Loader=Loader)


def _params(ctx: Ctx, sample: dict) -> list[str]:
    if sample["dir"] == "sam-quickstart-s3":
        return [f"AppBucketName=l2e-sample-s3-{ctx.account}-{ctx.region}"]
    return []


# ------------------------------------------------------------------ deploy

# Defects in the upstream samples that break them on Lambda itself, before any migration.
# Each is applied to the staged copy only and listed in SOURCES.md.
UPSTREAM_FIXES = {
    # template says app.lambdaHandler, app.js exports handler: Runtime.HandlerNotFound on every batch.
    "kinesis-lambda": [("Handler: app.lambdaHandler", "Handler: app.handler")],
}
# Changes that only make the test faster, applied the same way.
SPEED_TWEAKS = {
    "eventbridge-schedule-to-lambda-dlq-sam-python": [("ScheduleExpression: rate(5 minutes)", "ScheduleExpression: rate(1 minute)")],
}


def stage(sample: dict) -> Path:
    """Copy a sample and install its dependencies, which is what `sam build` would do."""
    src, dst = SAMPLES_DIR / sample["dir"], BUILD / sample["dir"]
    if dst.exists():
        shutil.rmtree(dst)
    shutil.copytree(src, dst)
    template = dst / sample.get("template", "template.yaml")
    for old, new in UPSTREAM_FIXES.get(sample["dir"], []) + SPEED_TWEAKS.get(sample["dir"], []):
        text = template.read_text()
        assert old in text, f"{sample['dir']}: upstream fix no longer applies ({old!r}); drop it"
        template.write_text(text.replace(old, new))
    t = _cfn_load(template)
    glob = ((t.get("Globals") or {}).get("Function") or {})
    # `sam build` applies Globals.Function.CodeUri; `aws cloudformation package` does not and would
    # zip the template's folder instead. Copy that code to the root so the package has it at the top.
    if glob.get("CodeUri") and any(r["Type"] == "AWS::Serverless::Function" and "CodeUri" not in r["Properties"]
                                   for r in t["Resources"].values()):
        shutil.copytree(dst / glob["CodeUri"], dst, dirs_exist_ok=True)
        glob = {**glob, "CodeUri": "."}
    for res in t["Resources"].values():
        if res["Type"] != "AWS::Serverless::Function":
            continue
        props = res["Properties"]
        code = dst / (props.get("CodeUri") or glob.get("CodeUri") or ".")
        arch = (props.get("Architectures") or glob.get("Architectures") or ["x86_64"])[0]
        if (code / "requirements.txt").is_file():
            run(sys.executable, "-m", "pip", "install", "-q", "-r", str(code / "requirements.txt"), "-t", str(code),
                "--only-binary=:all:", "--implementation", "cp", "--python-version", "3.14",
                "--platform", "manylinux2014_aarch64" if arch == "arm64" else "manylinux2014_x86_64")
        if (code / "package.json").is_file():
            run("npm", "install", "--omit=dev", "--no-audit", "--no-fund", "--silent", cwd=code)
    return template


def deploy(args: argparse.Namespace, ctx: Ctx) -> None:
    s3 = ctx.client("s3")
    try:
        s3.head_bucket(Bucket=ctx.artifacts)
    except ClientError:
        s3.create_bucket(Bucket=ctx.artifacts, CreateBucketConfiguration={"LocationConstraint": ctx.region})
        s3.put_bucket_tagging(Bucket=ctx.artifacts, Tagging={"TagSet": [TAG]})
    for sample in selected(args):
        log(f"deploy {sample['dir']}")
        template = stage(sample)
        packaged = template.with_name("packaged.yaml")
        run("aws", "cloudformation", "package", "--template-file", str(template), "--s3-bucket", ctx.artifacts,
            "--s3-prefix", sample["dir"], "--output-template-file", str(packaged), capture=True, env=ctx.cli_env)
        params = _params(ctx, sample)
        run("aws", "cloudformation", "deploy", "--template-file", str(packaged), "--stack-name", ctx.stack(sample),
            "--capabilities", "CAPABILITY_IAM", "CAPABILITY_AUTO_EXPAND", "--no-fail-on-empty-changeset",
            "--tags", "lambda2eks-test=true", *(["--parameter-overrides", *params] if params else []),
            capture=True, env=ctx.cli_env)
        log(f"  {ctx.stack(sample)}: function {ctx.function(sample)}")


# ------------------------------------------------------------------ driving the triggers

def http_get(url: str, host: str | None = None, signed: Ctx | None = None) -> tuple[int, str]:
    headers = {"Host": host} if host else {}
    if signed:
        req = AWSRequest(method="GET", url=url)
        SigV4Auth(signed.session.get_credentials().get_frozen_credentials(), "lambda", signed.region).add_auth(req)
        headers.update(dict(req.headers))
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=20) as r:
            return r.status, r.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def send(ctx: Ctx, sample: dict, marker: str) -> None:
    """Put one marked event into the sample's real event source."""
    res, kind = ctx.resources(sample), sample["kind"]
    if kind == "sqs":
        ctx.client("sqs").send_message(QueueUrl=next(u for u in res["AWS::SQS::Queue"] if "DLQ" not in u), MessageBody=marker)
    elif kind == "sns":
        ctx.client("sns").publish(TopicArn=res["AWS::SNS::Topic"][0], Message=marker)
    elif kind == "dynamodb":
        ctx.client("dynamodb").put_item(TableName=res["AWS::DynamoDB::Table"][0], Item={"id": {"S": marker}})
    elif kind == "kinesis":
        ctx.client("kinesis").put_record(StreamName=res["AWS::Kinesis::Stream"][0], PartitionKey=marker, Data=marker.encode())
    elif kind == "s3":
        ctx.client("s3").put_object(Bucket=res["AWS::S3::Bucket"][0], Key=f"l2e/{marker}.json",
                                    Body=json.dumps({"marker": marker}).encode())


def lambda_logged(ctx: Ctx, fn: str, needle: str, since_ms: int) -> bool:
    try:
        events = ctx.client("logs").filter_log_events(logGroupName=f"/aws/lambda/{fn}", startTime=since_ms,
                                                      filterPattern=f'"{needle}"')["events"]
    except ClientError:
        return False
    return bool(events)


def check_lambda_one(ctx: Ctx, sample: dict) -> str:
    kind, fn = sample["kind"], ctx.function(sample)
    marker = f"l2e-{uuid.uuid4().hex[:10]}"
    since = int(time.time() * 1000) - 5000
    if kind == "http-v2":
        api = ctx.client("apigatewayv2").get_api(ApiId=ctx.resources(sample)["AWS::ApiGatewayV2::Api"][0])["ApiEndpoint"]
        status, body = http_get(f"{api}/l2e?marker={marker}")
        assert status == 200 and json.loads(body)["queryString"]["marker"] == marker, f"{status} {body[:200]}"
        return "GET returned the query string"
    if kind == "rest":
        status, body = http_get(ctx.outputs(sample)["HelloWorldApi"])
        assert status == 200 and json.loads(body)["message"] == "hello world", f"{status} {body[:200]}"
        return "GET /hello returned hello world"
    if kind == "function-url":
        url = ctx.client("lambda").get_function_url_config(FunctionName=fn)["FunctionUrl"]
        assert http_get(url)[0] == 403, "an unsigned call was not refused"
        status, body = http_get(url, signed=ctx)
        assert status == 200 and body == "Hello World", f"{status} {body[:200]}"
        return "unsigned 403, SigV4-signed 200"
    if kind in ("schedule", "scheduler"):
        wait_until("a scheduled run in CloudWatch", lambda: lambda_logged(ctx, fn, sample["expect"], since - 120_000), 150, 10)
        if kind == "schedule":
            return f"rate(1 minute) run logged {sample['expect']}"
        # The sample logs the event it was given: what Scheduler sends a function when the target has no Input.
        logged = ctx.client("logs").filter_log_events(logGroupName=f"/aws/lambda/{fn}", startTime=since - 120_000,
                                                      filterPattern='"invoked by EventBridge Scheduler"')["events"]
        event = json.loads(logged[-1]["message"].split("\t")[-1].strip())["event"] if logged else "not logged"
        (OUT / "scheduler-event.json").write_text(json.dumps(event))
        return f"Scheduler run logged {sample['expect']}; event without Input was {json.dumps(event)}"
    if kind == "sfn":
        out = run_execution(ctx, sample, marker)
        wait_until(f"{marker} in /aws/lambda/{fn}", lambda: lambda_logged(ctx, fn, marker, since), 120, 5)
        return f"execution SUCCEEDED, output {out[:80]}; marker in the Lambda's log"
    send(ctx, sample, marker)

    def seen():
        if kind == "kinesis" and not lambda_logged(ctx, fn, marker, since):
            send(ctx, sample, marker)  # LATEST: the mapping may not have been polling yet
            time.sleep(8)
        return lambda_logged(ctx, fn, marker, since)
    wait_until(f"{marker} in /aws/lambda/{fn}", seen, 240, 10)
    return f"marker {marker} in CloudWatch Logs"


def state_machine(ctx: Ctx, sample: dict) -> str:
    return ctx.resources(sample)["AWS::StepFunctions::StateMachine"][0]


def run_execution(ctx: Ctx, sample: dict, marker: str) -> str:
    """Start the state machine with a marked input; return its output once it has succeeded."""
    sfn = ctx.client("stepfunctions")
    arn = sfn.start_execution(stateMachineArn=state_machine(ctx, sample), name=marker, input=json.dumps({"marker": marker}))["executionArn"]
    wait_until(f"execution {marker} to finish", lambda: sfn.describe_execution(executionArn=arn)["status"] != "RUNNING", 180, 3)
    ex = sfn.describe_execution(executionArn=arn)
    assert ex["status"] == "SUCCEEDED", f"{ex['status']}: {ex.get('error')} {ex.get('cause', '')[:300]}"
    return ex.get("output", "")


def task_outputs(ctx: Ctx, marker: str, sample: dict) -> list[str]:
    """The output of each Task in an execution, in order: TaskSucceeded (Lambda) and ActivitySucceeded (EKS) events."""
    sfn = ctx.client("stepfunctions")
    arn = state_machine(ctx, sample).replace(":stateMachine:", ":execution:") + f":{marker}"
    events = sfn.get_execution_history(executionArn=arn, maxResults=200)["events"]
    out = []
    for e in events:
        for key in ("taskSucceededEventDetails", "activitySucceededEventDetails", "lambdaFunctionSucceededEventDetails"):
            if key in e:
                out.append(f"{key.replace('SucceededEventDetails', '')}: {e[key].get('output', '')}")
    return out


# ------------------------------------------------------------------ migrate

def plans(ctx: Ctx, fns: list[str]):
    from lambda2eks.loaders.aws import load_from_aws
    from lambda2eks.plan import baseline_plan
    return {p.spec.name: p for p in (baseline_plan(s, REGISTRY) for s in load_from_aws(fns, OUT / "reload", region=ctx.region))}


def session_secret(ctx: Ctx, hours: int) -> None:
    c = ctx.client("sts").get_session_token(DurationSeconds=hours * 3600)["Credentials"]
    doc = {"apiVersion": "v1", "kind": "Secret", "type": "Opaque",
           "metadata": {"name": SESSION_SECRET, "namespace": NS, "labels": {"app.kubernetes.io/managed-by": "lambda2eks"}},
           "stringData": {"AWS_ACCESS_KEY_ID": c["AccessKeyId"], "AWS_SECRET_ACCESS_KEY": c["SecretAccessKey"],
                          "AWS_SESSION_TOKEN": c["SessionToken"]}}
    kubectl("apply", "-f", "-", input=json.dumps(doc), capture=True)
    log(f"  session credentials in Secret {SESSION_SECRET}, expiring {c['Expiration']}")


def _creds_ops(base: str) -> list[dict]:
    env = [{"name": k, "valueFrom": {"secretKeyRef": {"name": SESSION_SECRET, "key": k}}}
           for k in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")]
    return [{"op": "add", "path": f"{base}/env/-", "value": e} for e in env] + \
           [{"op": "add", "path": f"{base}/imagePullPolicy", "value": "Never"}]


def overlay(name: str) -> dict:
    """What a laptop lacks compared with EKS: Pod Identity (session keys instead), ECR, the ALB controller."""
    base = EKS_OUT / name / "k8s"
    docs = [d for f in sorted(base.glob("*.yaml")) if f.name != "kustomization.yaml" for d in yaml.safe_load_all(f.read_text()) if d]
    patches = []
    for d in docs:
        kind, ops = d["kind"], None
        # Rancher Desktop on Apple silicon is one arm64 node; it runs x86_64 images under emulation,
        # so the arch nodeSelector EKS needs would leave x86_64 functions Pending here.
        # Emulated x86_64 also costs memory: Node's baseline roughly doubles, so those pods get 128 Mi more.
        if kind in ("Deployment", "CronJob"):
            pod_path = "/spec/template/spec" if kind == "Deployment" else "/spec/jobTemplate/spec/template/spec"
            pod = d["spec"]["template"]["spec"] if kind == "Deployment" else d["spec"]["jobTemplate"]["spec"]["template"]["spec"]
            ops = _creds_ops(f"{pod_path}/containers/0") + [{"op": "remove", "path": f"{pod_path}/nodeSelector"}]
            if pod.get("nodeSelector", {}).get("kubernetes.io/arch") == "amd64" and platform.machine() == "arm64":
                mem = f"{int(pod['containers'][0]['resources']['limits']['memory'].rstrip('Mi')) + 128}Mi"
                ops += [{"op": "replace", "path": f"{pod_path}/containers/0/resources/{k}/memory", "value": mem}
                        for k in ("requests", "limits")]
        elif kind == "Ingress":
            # Every sample's API lives at "/", so each gets its own host on Traefik.
            ops = [{"op": "replace", "path": "/spec/ingressClassName", "value": "traefik"},
                   {"op": "add", "path": "/spec/rules/0/host", "value": f"{d['metadata']['name']}.localhost"}]
        elif kind == "TriggerAuthentication":
            ops = [{"op": "remove", "path": "/spec/podIdentity"},
                   {"op": "add", "path": "/spec/secretTargetRef", "value": [
                       {"parameter": p, "name": SESSION_SECRET, "key": k} for p, k in (
                           ("awsAccessKeyID", "AWS_ACCESS_KEY_ID"), ("awsSecretAccessKey", "AWS_SECRET_ACCESS_KEY"),
                           ("awsSessionToken", "AWS_SESSION_TOKEN"))]}]
        if ops:
            patches.append({"target": {"kind": kind, "name": d["metadata"]["name"]}, "patch": yaml.safe_dump(ops)})
    return {"apiVersion": "kustomize.config.k8s.io/v1beta1", "kind": "Kustomization",
            "resources": [os.path.relpath(base, OVERLAYS / name)], "patches": patches}


def deploy_bridge_stack(ctx: Ctx, name: str) -> None:
    path = EKS_OUT / name / "infra" / "resources.yaml"
    if not path.is_file():
        return
    cfn, stack = ctx.client("cloudformation"), f"l2e-bridge-{name}"
    params = [{"ParameterKey": k, "ParameterValue": "true"} for k in (_cfn_load(path).get("Parameters") or {}) if k.endswith("Subscribe")]
    try:
        cfn.create_stack(StackName=stack, TemplateBody=path.read_text(), Parameters=params, Tags=[TAG])
        cfn.get_waiter("stack_create_complete").wait(StackName=stack)
    except ClientError as e:
        if e.response["Error"]["Code"] != "AlreadyExistsException":
            raise
    log(f"  {stack}: bridge queues / lease table ready")


def cut_over(ctx: Ctx, sample: dict, plan) -> str:
    """Move the trigger from the Lambda to the pods, as cutover/CUTOVER.md describes."""
    from lambda2eks.generate import Target, s3_queue_configs
    lam, fn, kind = ctx.client("lambda"), plan.spec.name, sample["kind"]
    if kind in ("sqs", "dynamodb", "kinesis"):
        for m in lam.list_event_source_mappings(FunctionName=fn)["EventSourceMappings"]:
            lam.update_event_source_mapping(UUID=m["UUID"], Enabled=False)
        return "Lambda event source mapping disabled"
    if kind == "s3":
        s3 = ctx.client("s3")
        for bucket, configs in s3_queue_configs(plan, Target(plan, NS, ctx.region, "pod-identity", live=True)).items():
            cfg = s3.get_bucket_notification_configuration(Bucket=bucket)
            cfg.pop("ResponseMetadata", None)
            cfg["LambdaFunctionConfigurations"] = [c for c in cfg.get("LambdaFunctionConfigurations", [])
                                                   if not c["LambdaFunctionArn"].endswith(f":function:{fn}")]
            cfg["QueueConfigurations"] = cfg.get("QueueConfigurations", []) + configs
            s3.put_bucket_notification_configuration(Bucket=bucket, NotificationConfiguration=cfg)
        return "bucket notifications moved to the bridge queue"
    if kind == "schedule":
        for rule in ctx.resources(sample).get("AWS::Events::Rule", []):
            ctx.client("events").disable_rule(Name=rule)
        return "EventBridge rule disabled"
    if kind == "scheduler":
        sched = ctx.client("scheduler")
        for name in ctx.resources(sample).get("AWS::Scheduler::Schedule", []):
            s = sched.get_schedule(Name=name)
            keep = {k: s[k] for k in ("Name", "GroupName", "ScheduleExpression", "ScheduleExpressionTimezone", "FlexibleTimeWindow",
                                      "Target", "StartDate", "EndDate", "Description", "KmsKeyArn", "ActionAfterCompletion") if s.get(k)}
            sched.update_schedule(**keep, State="DISABLED")
        return "Scheduler schedule disabled (update-schedule with State DISABLED)"
    if kind == "sfn":
        sfn = ctx.client("stepfunctions")
        files = sorted((EKS_OUT / plan.k8s_name / "cutover").glob("sfn-*.json"))
        assert files, "lambda2eks wrote no rewritten state machine definition"
        for f in files:
            sfn.update_state_machine(stateMachineArn=state_machine(ctx, sample), definition=f.read_text())
        return f"state machine updated from {', '.join(f.name for f in files)}: the Task state calls the activity"
    if kind == "sns":
        return "bridge queue subscribed (Lambda still subscribed: the overlap CUTOVER.md warns about)"
    return "HTTP: API Gateway still serves; the pods answer the same requests through Traefik"


def migrate(args: argparse.Namespace, ctx: Ctx) -> None:
    samples = selected(args)
    fns = [ctx.function(s) for s in samples]
    cmd = [sys.executable, "-m", "lambda2eks.cli", "from-aws", *fns, "--region", ctx.region, "--out", str(EKS_OUT),
           "--registry", REGISTRY, "--namespace", NS, "--live"] + ([] if args.ai else ["--no-ai"])
    log("lambda2eks " + " ".join(cmd[3:]))
    run(*cmd, cwd=REPO, check=False)  # exit 1 only flags an AI review failure; the SUMMARY says which
    found = plans(ctx, fns)
    kubectl("create", "namespace", NS, capture=True, check=False)
    install_keda()
    session_secret(ctx, args.session_hours)
    notes = {}
    for sample in samples:
        plan = found[ctx.function(sample)]
        name = plan.k8s_name
        log(f"{sample['dir']} -> {name}")
        deploy_bridge_stack(ctx, name)
        notes[sample["dir"]] = cut_over(ctx, sample, plan)
        log(f"  cut-over: {notes[sample['dir']]}")
        run("docker", "build", "-q", "--platform", f"linux/{plan.spec.arch}", "-t", plan.image, str(EKS_OUT / name), capture=True)
        (OVERLAYS / name).mkdir(parents=True, exist_ok=True)
        (OVERLAYS / name / "kustomization.yaml").write_text(yaml.safe_dump(overlay(name), sort_keys=False))
        kubectl("apply", "-k", str(OVERLAYS / name), capture=True)
    (OUT / "cutover.json").write_text(json.dumps(notes, indent=2))
    for d in kubectl("-n", NS, "get", "deploy", "-o", "name", capture=True).stdout.split():
        kubectl("-n", NS, "rollout", "status", d, "--timeout=300s", check=False)


def install_keda() -> None:
    run("helm", "repo", "add", "kedacore", "https://kedacore.github.io/charts", "--force-update", capture=True)
    run("helm", "--kube-context", rancher.CONTEXT, "upgrade", "--install", "keda", "kedacore/keda",
        "--version", rancher.KEDA_CHART_VERSION, "--namespace", "keda", "--create-namespace", "--wait", "--timeout", "5m", capture=True)


# ------------------------------------------------------------------ check on EKS

def pod_logs(selector: str) -> str:
    return kubectl("-n", NS, "logs", "-l", selector, "--tail=-1", "--since=30m", "--max-log-requests=10",
                   capture=True, check=False).stdout


def ingress_host(name: str) -> str:
    return kubectl("-n", NS, "get", "ingress", "-l", f"app.kubernetes.io/name={name}", "-o",
                   "jsonpath={.items[0].spec.rules[0].host}", capture=True).stdout


def check_eks_one(ctx: Ctx, sample: dict, name: str) -> str:
    kind, marker = sample["kind"], f"l2e-{uuid.uuid4().hex[:10]}"
    if kind in ("http-v2", "rest", "function-url"):
        path = {"http-v2": f"/l2e?marker={marker}", "rest": "/hello", "function-url": "/"}[kind]
        status, body = http_get(f"{rancher.INGRESS}{path}", host=ingress_host(name))
        if kind == "http-v2":
            assert status == 200 and json.loads(body)["queryString"]["marker"] == marker, f"{status} {body[:200]}"
            return "same request through Traefik; query string intact (payload 2.0)"
        if kind == "rest":
            assert status == 200 and json.loads(body)["message"] == "hello world", f"{status} {body[:200]}"
            return "GET /hello 200 hello world, with requests + internet egress from the pod"
        assert status == 200 and body == "Hello World", f"{status} {body[:200]}"
        return "200 Hello World; NOTE the unsigned call is accepted: AWS_IAM auth does not exist on the Ingress"
    if kind in ("schedule", "scheduler"):
        cron = kubectl("-n", NS, "get", "cronjob", "-l", f"app.kubernetes.io/name={name}", "-o", "name", capture=True).stdout.strip()
        job = f"check-{marker}"
        kubectl("-n", NS, "create", "job", job, f"--from={cron}", capture=True)
        kubectl("-n", NS, "wait", f"job/{job}", "--for=condition=complete", "--timeout=180s", capture=True)
        logs = kubectl("-n", NS, "logs", f"job/{job}", capture=True).stdout
        assert sample["expect"] in logs, logs[-300:]
        if kind == "schedule":
            return f"Job from the CronJob printed {sample['expect']}"
        seen = OUT / "scheduler-event.json"
        if seen.exists():
            # Same event as Scheduler sent the Lambda, apart from its id and time.
            line = next(l for l in logs.splitlines() if "invoked by EventBridge Scheduler" in l)
            got = json.loads(line[line.index("{"):])["event"]
            want = json.loads(seen.read_text())
            strip = lambda e: {k: v for k, v in e.items() if k not in ("id", "time")} if isinstance(e, dict) else e  # noqa: E731
            assert strip(got) == strip(want), f"Lambda got {want}, the pod got {got}"
        return f"Job from the CronJob printed {sample['expect']}, with the event Scheduler sent the Lambda (id and time aside)"
    if kind == "sfn":
        out = run_execution(ctx, sample, marker)
        selector = f"app.kubernetes.io/name={name}"
        wait_until(f"{marker} in the pod logs", lambda: marker in pod_logs(selector), 60, 3)
        steps = task_outputs(ctx, marker, sample)
        assert steps and steps[0].startswith("activity:") and "from Lambda1" in steps[0], steps
        return f"execution SUCCEEDED through the activity; first task output {steps[0][:70]}; final output {out[:60]}"
    selector = f"app.kubernetes.io/name={name}"
    if kind == "kinesis":
        # StartingPosition LATEST: only records written after the consumer holds the shard reach it.
        wait_until("the Kinesis consumer to read its shard", lambda: "shard" in pod_logs(selector).lower(), 120, 5)
        time.sleep(10)
    send(ctx, sample, marker)
    wait_until(f"{marker} in the pod logs", lambda: marker in pod_logs(selector), 240, 5)
    return f"marker {marker} in the pod logs"


# ------------------------------------------------------------------ orchestration

def selected(args: argparse.Namespace) -> list[dict]:
    return [s for s in SAMPLES if not getattr(args, "only", None) or s["dir"] in args.only]


def checks(args: argparse.Namespace, ctx: Ctx, where: str) -> list[tuple]:
    from concurrent.futures import ThreadPoolExecutor
    samples = selected(args)
    names = {}
    if where == "eks":
        from lambda2eks.plan import k8s_name
        names = {s["dir"]: k8s_name(ctx.function(s)) for s in samples}

    def one(s):
        t0 = time.time()
        try:
            detail = check_lambda_one(ctx, s) if where == "lambda" else check_eks_one(ctx, s, names[s["dir"]])
            return s["dir"], s["kind"], True, detail, time.time() - t0
        except Exception as e:  # noqa: BLE001 - report every sample
            return s["dir"], s["kind"], False, f"{type(e).__name__}: {e}", time.time() - t0
    with ThreadPoolExecutor(max_workers=len(samples)) as pool:
        rows = list(pool.map(one, samples))
    print(f"\n{where.upper()}")
    for d, kind, ok, detail, secs in rows:
        print(f"{'PASS' if ok else 'FAIL'}  {d:30} {kind:13} {secs:4.0f}s  {detail}")
    print(f"{sum(r[2] for r in rows)}/{len(rows)} passed\n")
    return rows


def report(lambda_rows: list[tuple], eks_rows: list[tuple]) -> int:
    cut = json.loads((OUT / "cutover.json").read_text()) if (OUT / "cutover.json").exists() else {}
    eks = {r[0]: r for r in eks_rows}
    lines = ["# AWS samples: Lambda vs EKS", "", "| Sample | Trigger | On Lambda | Cut-over | On EKS (Rancher) |", "|---|---|---|---|---|"]
    for d, kind, ok, detail, _ in lambda_rows:
        e = eks.get(d)
        lines.append(f"| {d} | {kind} | {'PASS' if ok else 'FAIL'}: {detail} | {cut.get(d, '-')} | "
                     f"{('PASS' if e[2] else 'FAIL') + ': ' + e[3] if e else 'not run'} |")
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    log(f"wrote {OUT / 'REPORT.md'}")
    return 0 if all(r[2] for r in lambda_rows) and eks_rows and all(r[2] for r in eks_rows) else 1


def _empty_bucket(s3, bucket: str) -> None:
    try:
        for page in s3.get_paginator("list_object_versions").paginate(Bucket=bucket):
            objs = [{"Key": o["Key"], "VersionId": o["VersionId"]} for o in page.get("Versions", []) + page.get("DeleteMarkers", [])]
            if objs:
                s3.delete_objects(Bucket=bucket, Delete={"Objects": objs})
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchBucket":
            raise


def teardown(args: argparse.Namespace, ctx: Ctx) -> None:
    log("teardown: Kubernetes objects")
    if rancher.cluster_ready():
        kubectl("-n", NS, "delete", "deploy,svc,ingress,hpa,pdb,cronjob,job,scaledobject,triggerauthentication,configmap,secret,sa",
                "-l", "app.kubernetes.io/managed-by=lambda2eks", "--ignore-not-found", capture=True, check=False)
        kubectl("-n", NS, "delete", "job", "--all", capture=True, check=False)
    cfn, s3, logs = ctx.client("cloudformation"), ctx.client("s3"), ctx.client("logs")
    stacks = [s for s in (cfn.get_paginator("list_stacks").paginate(StackStatusFilter=[
        "CREATE_COMPLETE", "UPDATE_COMPLETE", "ROLLBACK_COMPLETE", "CREATE_FAILED", "UPDATE_ROLLBACK_COMPLETE", "DELETE_FAILED"]).build_full_result()["StackSummaries"])
        if s["StackName"].startswith(("l2e-bridge-", "l2e-sample-"))]
    fns = []
    for s in stacks:
        res = cfn.describe_stack_resources(StackName=s["StackName"])["StackResources"]
        fns += [r["PhysicalResourceId"] for r in res if r["ResourceType"] == "AWS::Lambda::Function"]
        for r in res:
            if r["ResourceType"] == "AWS::S3::Bucket":
                _empty_bucket(s3, r["PhysicalResourceId"])
    # Bridge stacks first: their SNS subscriptions point at topics in the sample stacks.
    for prefix in ("l2e-bridge-", "l2e-sample-"):
        batch = [s["StackName"] for s in stacks if s["StackName"].startswith(prefix)]
        for name in batch:
            log(f"  delete stack {name}")
            cfn.delete_stack(StackName=name)
        for name in batch:
            cfn.get_waiter("stack_delete_complete").wait(StackName=name, WaiterConfig={"Delay": 10, "MaxAttempts": 90})
    for fn in fns:
        try:
            logs.delete_log_group(logGroupName=f"/aws/lambda/{fn}")
        except ClientError:
            pass
    _empty_bucket(s3, ctx.artifacts)
    try:
        s3.delete_bucket(Bucket=ctx.artifacts)
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchBucket":
            raise
    left = [s["StackName"] for s in cfn.list_stacks(StackStatusFilter=["CREATE_COMPLETE", "UPDATE_COMPLETE", "DELETE_FAILED"])["StackSummaries"]
            if s["StackName"].startswith(("l2e-bridge-", "l2e-sample-"))]
    log("teardown complete" if not left else f"teardown LEFT STACKS: {left}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--region", default="eu-west-1")
    p.add_argument("--profile")
    sub = p.add_subparsers(dest="cmd", required=True)
    for name in ("all", "deploy", "check-lambda", "migrate", "check-eks", "teardown", "stop"):
        s = sub.add_parser(name)
        s.add_argument("--only", action="append", metavar="SAMPLE", help="only this sample folder; repeatable")
        s.add_argument("--ai", action="store_true", help="run lambda2eks's AI review")
        s.add_argument("--session-hours", type=int, default=3, help="lifetime of the pods' STS session credentials")
    sub.choices["all"].add_argument("--keep", action="store_true", help="keep the AWS stacks and leave Rancher running")
    args = p.parse_args()
    if args.cmd == "stop":
        rancher.stop_rancher()
        return 0
    ctx = Ctx(args.region, args.profile)
    log(f"account {ctx.account}, region {ctx.region}")
    if args.cmd == "teardown":
        try:
            teardown(args, ctx)
        finally:
            rancher.stop_rancher()  # done with the cluster too: it holds several GB of the laptop's memory
        return 0
    if args.cmd == "all":
        lam_rows, eks_rows = [], []
        try:
            deploy(args, ctx)
            lam_rows = checks(args, ctx, "lambda")
            rancher.start_rancher()
            migrate(args, ctx)
            eks_rows = checks(args, ctx, "eks")
            return report(lam_rows, eks_rows)
        finally:
            if not args.keep:
                teardown(args, ctx)
                rancher.stop_rancher()
    if args.cmd == "deploy":
        deploy(args, ctx)
    elif args.cmd == "check-lambda":
        return 0 if all(r[2] for r in checks(args, ctx, "lambda")) else 1
    elif args.cmd == "migrate":
        rancher.start_rancher()
        migrate(args, ctx)
    elif args.cmd == "check-eks":
        return 0 if all(r[2] for r in checks(args, ctx, "eks")) else 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
