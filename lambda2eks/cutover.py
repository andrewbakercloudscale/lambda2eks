"""The cut-over runbook: move each trigger from the Lambda to EKS in stages, with a rollback for every step."""

from __future__ import annotations

import json
from importlib import resources

from .generate import PAUSED, Target, _cfn_id, sfn_rewrites
from .model import DYNAMODB, HTTP, INVOKE, KINESIS, S3, SCHEDULE, SFN, SNS, SQS
from .plan import Plan, Workload


def route53_script(namespace: str, ingress: str) -> str:
    text = resources.files("lambda2eks.templates").joinpath("route53-weights.sh").read_text()
    return text.replace("__NAMESPACE__", namespace).replace("__INGRESS__", ingress)


def _sh(*lines: str) -> list[str]:
    return ["```bash", *lines, "```", ""]


def _fn_arn(plan: Plan, t: Target) -> str:
    return f"arn:aws:lambda:{t.region}:{t.account}:function:{plan.spec.name}"


def _esm_lookup(plan: Plan, source_arn_expr: str) -> str:
    return (f"UUID=$(aws lambda list-event-source-mappings --function-name {plan.spec.name} "
            f"--event-source-arn {source_arn_expr} --query 'EventSourceMappings[0].UUID' --output text)")


def _unpause(ns: str, w: Workload) -> str:
    return f"kubectl -n {ns} annotate scaledobject {w.name} {PAUSED}-"


def _pause(ns: str, w: Workload) -> str:
    return f"kubectl -n {ns} annotate scaledobject {w.name} {PAUSED}=0 --overwrite"


def _http(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns = t.namespace
    out = [f"### HTTP: `{w.name}` (weighted DNS)", "",
           "Lambda and EKS answer side by side while Route 53 moves the share of DNS lookups. "
           "`cutover/route53-weights.sh` makes each change in one atomic batch and keeps the original record for rollback.", "",
           "Before you start:", "",
           "- Clients must reach the API through a Route 53 name (an API Gateway custom domain). "
           "If they call the `execute-api` URL directly, move them to a custom domain first; weighted DNS cannot split that URL.",
           "- The ALB must serve the same name over HTTPS: re-run lambda2eks with `--certificate-arn` for the domain's ACM certificate"
           + (" (done)." if t.certificate_arn else ". **Not set yet.**"),
           f"- A public API needs `ingress_scheme: internet-facing` (now `{plan.ingress_scheme}`).",
           "- API Gateway authorizers, throttling and CORS do not exist on the ALB. Handle them first (see MIGRATION.md).", "",
           "Steps:", ""]
    out += _sh(
        "export HOSTED_ZONE_ID=Z_REPLACE_ME RECORD_NAME=api.example.com",
        f"kubectl -n {ns} rollout status deploy/{w.name}",
        "./cutover/route53-weights.sh init        # lambda=100, eks=0",
        "./cutover/route53-weights.sh set 5       # then 25, 50, 100, watching each for a while",
        "./cutover/route53-weights.sh finish      # once 100% has been quiet: one simple alias to the ALB")
    out += ["Watch at each step: the ALB's `HTTPCode_Target_5XX_Count` and `TargetResponseTime`, "
            f"`kubectl -n {ns} logs deploy/{w.name}`, and the Lambda's own error rate falling.", "",
            "Rollback: `./cutover/route53-weights.sh set 0` at any step, or `rollback` (restores the original record, also after `finish`).", ""]
    return out


def _invoke(plan: Plan, w: Workload, t: Target) -> list[str]:
    url = f"http://{w.name}.{t.namespace}.svc.cluster.local"
    return [f"### Direct invocations: `{w.name}`", "",
            "Callers keep using the Lambda Invoke API; point their SDK at the Service. Requests are not authenticated, "
            "so keep the Service inside the cluster (add a NetworkPolicy if other namespaces must not call it).", "",
            *_sh("# Python", f'boto3.client("lambda", endpoint_url="{url}").invoke(FunctionName="{plan.spec.name}", Payload=b"{{}}")',
                 "# Node", f'new LambdaClient({{ endpoint: "{url}" }})'),
            "Move callers one at a time. Rollback: remove the endpoint override.", ""]


def _sqs(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns, d = t.namespace, w.trigger.details
    arn = d.get("queue_arn") or "QUEUE_ARN"
    return [f"### SQS: `{w.name}`", "",
            "SQS hands each message to one consumer, so the Lambda and the worker can share the queue during the switch.", "",
            *_sh(f"{_esm_lookup(plan, arn)}",
                 "# 1. one worker beside the Lambda",
                 f"kubectl -n {ns} patch scaledobject {w.name} --type merge -p '{{\"spec\":{{\"maxReplicaCount\":1}}}}'",
                 _unpause(ns, w),
                 "# 2. watch the worker's logs and the queue's DLQ; then stop the Lambda consuming",
                 "aws lambda update-event-source-mapping --uuid \"$UUID\" --no-enabled",
                 "# 3. full scale",
                 f"kubectl -n {ns} patch scaledobject {w.name} --type merge -p '{{\"spec\":{{\"maxReplicaCount\":{plan.sqs_max_replicas}}}}}'"),
            "Rollback:", "",
            *_sh("aws lambda update-event-source-mapping --uuid \"$UUID\" --enabled", _pause(ns, w))]


def _s3(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns, bucket = t.namespace, w.trigger.details.get("bucket") or "REPLACE_ME_BUCKET"
    cfg = f"infra/s3-{bucket}-queue-config.json"
    return [f"### S3: `{w.name}` (bucket `{bucket}`)", "",
            "S3 refuses two destinations for overlapping events and prefixes, so this is a switch, not a ramp. "
            "Events already sent to the Lambda finish there; new ones go to the queue.", "",
            *_sh(_unpause(ns, w) + "   # idle until events arrive",
                 f"aws s3api get-bucket-notification-configuration --bucket {bucket} > cutover/.state/s3-{bucket}-before.json",
                 f"jq --slurpfile q {cfg} --arg fn {_fn_arn(plan, t)} \\",
                 "  '.LambdaFunctionConfigurations = [(.LambdaFunctionConfigurations // [])[] | select(.LambdaFunctionArn | startswith($fn) | not)]",
                 "   | .QueueConfigurations = ((.QueueConfigurations // []) + $q[0])' \\",
                 f"  cutover/.state/s3-{bucket}-before.json > cutover/.state/s3-{bucket}-after.json",
                 f"aws s3api put-bucket-notification-configuration --bucket {bucket} \\",
                 f"  --notification-configuration file://cutover/.state/s3-{bucket}-after.json"),
            "Check: upload a test object that matches the filter and watch the worker's logs. "
            "S3 sends one `s3:TestEvent` when the configuration changes; the worker drops it.", "",
            "Rollback:", "",
            *_sh(f"aws s3api put-bucket-notification-configuration --bucket {bucket} \\",
                 f"  --notification-configuration file://cutover/.state/s3-{bucket}-before.json",
                 _pause(ns, w))]


def _sns(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns, topic = t.namespace, w.trigger.details.get("topic_arn") or "REPLACE_ME_TOPIC_ARN"
    param = f"{_cfn_id(w.name)}Subscribe"
    stack = f"lambda2eks-{plan.k8s_name}"
    fn_arn = _fn_arn(plan, t)
    policy = w.trigger.details.get("filter_policy")
    resub = [f"aws sns subscribe --topic-arn {topic} --protocol lambda --notification-endpoint {fn_arn}"
             + (f" \\\n  --attributes '{json.dumps({'FilterPolicy': policy if isinstance(policy, str) else json.dumps(policy)})}'"
                if policy else "")]
    return [f"### SNS: `{w.name}`", "",
            "Between subscribing the queue and removing the Lambda subscription, both get each message, so the handler "
            "sees a few duplicates. Reverse steps 2 and 3 if losing a few messages is better than duplicates.", "",
            *_sh(_unpause(ns, w),
                 f"aws cloudformation deploy --stack-name {stack} --template-file infra/resources.yaml "
                 f"--parameter-overrides {param}=true   # 2. subscribe the queue",
                 f"SUB=$(aws sns list-subscriptions-by-topic --topic-arn {topic} \\",
                 f"  --query \"Subscriptions[?Endpoint=='{fn_arn}'].SubscriptionArn | [0]\" --output text)",
                 "aws sns unsubscribe --subscription-arn \"$SUB\"   # 3. the Lambda stops receiving"),
            "Rollback (the Lambda keeps its resource policy, so re-subscribing is enough):", "",
            *_sh(*resub,
                 f"aws cloudformation deploy --stack-name {stack} --template-file infra/resources.yaml --parameter-overrides {param}=false",
                 _pause(ns, w))]


def _stream(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns, d = t.namespace, w.trigger.details
    if w.kind == KINESIS:
        arn = d.get("stream_arn") or f"arn:aws:kinesis:{t.region}:{t.account}:stream/{d.get('stream_name')}"
        lookup = [f"STREAM_ARN={arn}"]
    else:
        lookup = [f"STREAM_ARN=$(aws dynamodb describe-table --table-name {d.get('table_name') or 'REPLACE_ME'} "
                  "--query Table.LatestStreamArn --output text)"]
    return [f"### {'Kinesis' if w.kind == KINESIS else 'DynamoDB stream'}: `{w.name}`", "",
            "Every reader of a stream sees every record, so the Lambda and EKS must not both run. The worker starts a little "
            "before the moment the Lambda stopped, so nothing is skipped and the overlap is processed twice. "
            "Make the overlap longer than the Lambda's worst `IteratorAge`.", "",
            *_sh(*lookup, _esm_lookup(plan, '"$STREAM_ARN"'),
                 f"aws cloudwatch get-metric-statistics --namespace AWS/Lambda --metric-name IteratorAge --statistics Maximum \\",
                 f"  --dimensions Name=FunctionName,Value={plan.spec.name} --period 300 \\",
                 "  --start-time \"$(date -u -v-1H +%FT%TZ 2>/dev/null || date -u -d '-1 hour' +%FT%TZ)\" --end-time \"$(date -u +%FT%TZ)\"",
                 "OVERLAP_S=300   # more than the IteratorAge maximum above, in seconds",
                 "aws lambda update-event-source-mapping --uuid \"$UUID\" --no-enabled",
                 "START=$(( $(date +%s) - OVERLAP_S ))",
                 f"kubectl -n {ns} set env deploy/{w.name} L2E_STREAM_START_TIMESTAMP=$START",
                 _unpause(ns, w)),
            "The start time only applies to shards with no checkpoint yet; after that the lease table holds each shard's position.", "",
            "Rollback: the event source mapping resumes from its own checkpoint, so records EKS handled in the meantime are processed again.", "",
            *_sh(_pause(ns, w), "aws lambda update-event-source-mapping --uuid \"$UUID\" --enabled")]


def _scheduler(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns, d = t.namespace, w.trigger.details
    name, group = d.get("schedule_name") or "REPLACE_ME_SCHEDULE", d.get("group_name") or "default"
    state = f"cutover/.state/schedule-{group}-{name}.json"
    # update-schedule replaces the whole schedule, so the change is made on a copy of what get-schedule returned.
    keep = ("Name, GroupName, ScheduleExpression, ScheduleExpressionTimezone, FlexibleTimeWindow, Target, StartDate, "
            "EndDate, Description, KmsKeyArn, ActionAfterCompletion")

    def set_state(value: str) -> str:
        return (f"jq '{{{keep}}} | with_entries(select(.value != null)) | .State = \"{value}\"' {state} > {state}.{value.lower()} && "
                f"aws scheduler update-schedule --cli-input-json file://{state}.{value.lower()}")
    return [f"### Schedule (EventBridge Scheduler): `{w.name}`", "",
            "The CronJob is created suspended. Switch between two runs so the job never runs twice or not at all"
            + (f"; it runs in the schedule's time zone, {d['timezone']}." if d.get("timezone") else "."), "",
            *_sh(f"aws scheduler get-schedule --name {name} --group-name {group} > {state}",
                 set_state("DISABLED"),
                 f"kubectl -n {ns} patch cronjob {w.name} -p '{{\"spec\":{{\"suspend\":false}}}}'"),
            f"To try the job first, `kubectl -n {ns} create job --from=cronjob/{w.name} {w.name}-manual` runs it once, "
            "with the same side effects as a scheduled run.", "",
            "Rollback:", "",
            *_sh(f"kubectl -n {ns} patch cronjob {w.name} -p '{{\"spec\":{{\"suspend\":true}}}}'", set_state("ENABLED"))]


def _schedule(plan: Plan, w: Workload, t: Target) -> list[str]:
    if w.trigger.details.get("source") == "scheduler":
        return _scheduler(plan, w, t)
    ns = t.namespace
    rule = w.trigger.details.get("rule_arn")
    find = (f"RULE={rule.split('/')[-1]}" if rule else
            f"RULE=$(aws events list-rule-names-by-target --target-arn {_fn_arn(plan, t)} --query 'RuleNames[0]' --output text)")
    return [f"### Schedule: `{w.name}`", "",
            "The CronJob is created suspended. Switch between two runs so the job never runs twice or not at all.", "",
            *_sh(find, "aws events disable-rule --name \"$RULE\"",
                 f"kubectl -n {ns} patch cronjob {w.name} -p '{{\"spec\":{{\"suspend\":false}}}}'"),
            f"To try the job first, `kubectl -n {ns} create job --from=cronjob/{w.name} {w.name}-manual` runs it once, "
            "with the same side effects as a scheduled run.", "",
            "Rollback:", "",
            *_sh(f"kubectl -n {ns} patch cronjob {w.name} -p '{{\"spec\":{{\"suspend\":true}}}}'", "aws events enable-rule --name \"$RULE\"")]


def _sfn(plan: Plan, w: Workload, t: Target) -> list[str]:
    ns = t.namespace
    out = [f"### Step Functions: `{w.name}` (activity `{t.activity_name(plan)}`)", "",
           "The workers are already polling the activity; nothing reaches it until a state machine calls it. Switching a "
           "state machine affects executions started afterwards. Executions already running keep calling the Lambda, so "
           "keep the Lambda until they have finished.", ""]
    for name, (_new, done, notes, d) in sfn_rewrites(plan, t).items():
        arn = d.get("state_machine_arn") or f"arn:aws:states:{t.region}:{t.account}:stateMachine:{name}"
        out += [f"State machine `{name}`: {', '.join(f'`{p}`' for p in done)} now call the activity." + (
            " Others stay on Lambda: " + "; ".join(notes) if notes else ""), ""]
        if not d.get("state_machine_arn"):
            out += ["The definition came from the template, so its `${...}` substitutions are still in "
                    f"`cutover/sfn-{name}.json`. Put the rewritten states into the template instead of running the update below.", ""]
        out += _sh(f"kubectl -n {ns} rollout status deploy/{w.name}",
                   f"aws stepfunctions describe-state-machine --state-machine-arn {arn} --query definition --output text \\",
                   f"  > cutover/.state/sfn-{name}-before.json",
                   f"diff <(jq -S . cutover/.state/sfn-{name}-before.json) <(jq -S . cutover/sfn-{name}.json)   # only the function's states",
                   f"aws stepfunctions update-state-machine --state-machine-arn {arn} --definition file://cutover/sfn-{name}.json")
        out += ["Check: start an execution with a known input and watch "
                f"`kubectl -n {ns} logs deploy/{w.name}`; the state's output should match what the Lambda returned.", "",
                "Rollback:", "",
                *_sh(f"aws stepfunctions update-state-machine --state-machine-arn {arn} --definition file://cutover/.state/sfn-{name}-before.json")]
    return out


SECTIONS = {HTTP: _http, INVOKE: _invoke, SQS: _sqs, S3: _s3, SNS: _sns, KINESIS: _stream, DYNAMODB: _stream, SCHEDULE: _schedule,
            SFN: _sfn}


def runbook(plan: Plan, t: Target, has_infra: bool, has_worker_policy: bool) -> str:
    ns = t.namespace
    out = [f"# Cut-over: {plan.spec.name} from Lambda to EKS", "",
           "The Lambda keeps running until each trigger is switched; the IAM trust policy lets the same role serve both. "
           + ("Queue and stream workers start paused and CronJobs start suspended, so nothing on EKS consumes events until its step below. "
              if not t.live else "**Generated with `--live`: workers and CronJobs are already active. Do not run them beside the Lambda.** ")
           + "Every step has a rollback.", "",
           "## 0. Before any traffic moves", ""]
    steps = ["./build.sh"]
    if has_infra:
        steps.append(f"aws cloudformation deploy --stack-name lambda2eks-{plan.k8s_name} --template-file infra/resources.yaml")
    steps.append(f"aws iam update-assume-role-policy --role-name {(plan.spec.role_arn or 'ROLE').split('/')[-1]} "
                 "--policy-document file://iam/trust-policy.json")
    if has_worker_policy:
        steps.append(f"aws iam put-role-policy --role-name {(plan.spec.role_arn or 'ROLE').split('/')[-1]} "
                     "--policy-name lambda2eks-worker --policy-document file://iam/worker-policy.json")
    steps += [f"kubectl create namespace {ns} --dry-run=client -o yaml | kubectl apply -f -",
              "kubectl apply -k k8s/          # or: helm upgrade --install " + plan.k8s_name + f" helm/ -n {ns}",
              "mkdir -p cutover/.state"]
    out += _sh(*steps)
    out += ["Fill every `REPLACE_ME` first (MIGRATION.md lists them). Order the triggers below however suits you; each is independent.", "",
            "## 1. Switch each trigger", ""]
    for w in plan.workloads:
        out += SECTIONS[w.kind](plan, w, t)
    out += ["## 2. Finish", "",
            "After a quiet period with everything on EKS:", "",
            "- Delete the Lambda's triggers and then the function, through the IaC that created them.",
            "- Remove the `KeepLambdaDuringMigration` statement from `iam/trust-policy.json` and apply it again.",
            "- Keep `cutover/.state/` until then; it holds the records needed to roll back.", ""]
    return "\n".join(out)
