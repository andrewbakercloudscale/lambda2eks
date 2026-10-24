"""Load functions from a SAM or CloudFormation template (YAML or JSON)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from ..model import HTTP, SCHEDULE, SQS, UNSUPPORTED, FunctionSpec, Trigger


class _CfnLoader(yaml.SafeLoader):
    pass


def _intrinsic(loader: yaml.Loader, suffix: str, node: yaml.Node) -> dict[str, Any]:
    name = "Ref" if suffix == "Ref" else f"Fn::{suffix}"
    if isinstance(node, yaml.ScalarNode):
        value: Any = loader.construct_scalar(node)
        if suffix == "GetAtt" and isinstance(value, str):
            value = value.split(".", 1)
    elif isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    else:
        value = loader.construct_mapping(node, deep=True)
    return {name: value}


_CfnLoader.add_multi_constructor("!", lambda l, s, n: _intrinsic(l, s, n))

_UNSUPPORTED_EVENTS = {
    "S3": "s3", "SNS": "sns", "DynamoDB": "dynamodb", "Kinesis": "kinesis", "MSK": "kafka",
    "SelfManagedKafka": "kafka", "MQ": "mq", "EventBridgeRule": "eventbridge-rule",
    "CloudWatchEvent": "eventbridge-rule", "CloudWatchLogs": "cloudwatch-logs", "IoTRule": "iot",
    "AlexaSkill": "alexa", "Cognito": "cognito", "DocumentDB": "documentdb",
}


def load_template(path: Path, only: list[str] | None = None) -> list[FunctionSpec]:
    text = path.read_text()
    doc = json.loads(text) if path.suffix == ".json" else yaml.load(text, Loader=_CfnLoader)  # noqa: S506 - SafeLoader subclass
    resources: dict[str, Any] = doc.get("Resources") or {}
    globals_fn: dict[str, Any] = (doc.get("Globals") or {}).get("Function") or {}
    base = path.parent

    specs = []
    for logical_id, res in resources.items():
        rtype = res.get("Type")
        if rtype not in ("AWS::Serverless::Function", "AWS::Lambda::Function"):
            continue
        props = dict(res.get("Properties") or {})
        if rtype == "AWS::Serverless::Function":
            props = _merge_globals(globals_fn, props)
        name = props.get("FunctionName")
        name = name if isinstance(name, str) else logical_id
        if only and name not in only and logical_id not in only:
            continue
        specs.append(_to_spec(logical_id, name, rtype, props, resources, base))
    return specs


def _merge_globals(globals_fn: dict[str, Any], props: dict[str, Any]) -> dict[str, Any]:
    merged = dict(globals_fn)
    for k, v in props.items():
        if isinstance(v, dict) and isinstance(merged.get(k), dict):
            merged[k] = {**merged[k], **v}
            if k == "Environment":
                merged[k]["Variables"] = {
                    **(globals_fn.get("Environment", {}).get("Variables") or {}),
                    **(v.get("Variables") or {}),
                }
        else:
            merged[k] = v
    return merged


def _to_spec(logical_id: str, name: str, rtype: str, props: dict[str, Any],
             resources: dict[str, Any], base: Path) -> FunctionSpec:
    warnings: list[str] = []

    def scalar(key: str, default: Any = None) -> Any:
        v = props.get(key, default)
        if isinstance(v, dict):
            warnings.append(f"{key} is an intrinsic ({_describe(v)}); using default {default!r}")
            return default
        return v

    env = {}
    for k, v in ((props.get("Environment") or {}).get("Variables") or {}).items():
        if isinstance(v, (dict, list)):
            env[k] = "REPLACE_ME"
            warnings.append(f"environment variable {k} is an intrinsic ({_describe(v)}); set its value by hand")
        else:
            env[k] = "" if v is None else str(v)

    role = props.get("Role")
    role_arn = role if isinstance(role, str) else None
    if role is not None and role_arn is None:
        warnings.append(f"Role is an intrinsic ({_describe(role)}); fill in the role ARN in the ServiceAccount/IAM steps")
    if props.get("Policies"):
        warnings.append("SAM Policies define the execution role inline; create an equivalent IAM role for the pod")

    code_dir, image_uri = _code_location(props, rtype, base, warnings)

    spec = FunctionSpec(
        name=name,
        runtime=scalar("Runtime"),
        handler=scalar("Handler"),
        memory_mb=int(scalar("MemorySize", 128)),
        timeout_s=int(scalar("Timeout", 3)),
        ephemeral_storage_mb=int(((props.get("EphemeralStorage") or {}).get("Size")) or 512),
        environment=env,
        role_arn=role_arn,
        code_dir=code_dir,
        image_uri=image_uri,
        architectures=props.get("Architectures") or ["x86_64"],
        layers=[l if isinstance(l, str) else _describe(l) for l in props.get("Layers") or []],
        vpc=bool(props.get("VpcConfig")),
        reserved_concurrency=props.get("ReservedConcurrentExecutions") if isinstance(props.get("ReservedConcurrentExecutions"), int) else None,
        dead_letter_target=_dlq(props),
        warnings=warnings,
    )

    if role_arn and role_arn.count(":") >= 4:
        spec.account_id = role_arn.split(":")[4] or None

    if rtype == "AWS::Serverless::Function":
        for ev_id, ev in (props.get("Events") or {}).items():
            spec.triggers.extend(_sam_event(ev_id, ev, resources, warnings))
        if props.get("FunctionUrlConfig"):
            spec.triggers.append(Trigger(HTTP, {"method": "ANY", "path": None, "event_format": "2.0", "source": "function-url"}))
    spec.triggers.extend(_cfn_triggers(logical_id, resources, warnings))
    # A template has no region of its own; a queue ARN is the one place it shows up.
    for t in spec.triggers:
        arn = t.details.get("queue_arn") or ""
        if arn.startswith("arn:aws") and "REPLACE_ME" not in arn:
            spec.region = arn.split(":")[3]
            break
    return spec


def _code_location(props: dict[str, Any], rtype: str, base: Path, warnings: list[str]) -> tuple[Path | None, str | None]:
    if props.get("PackageType") == "Image" or props.get("ImageUri"):
        uri = props.get("ImageUri") or (props.get("Code") or {}).get("ImageUri")
        return None, uri if isinstance(uri, str) else None
    if rtype == "AWS::Serverless::Function":
        code = props.get("CodeUri") or props.get("InlineCode")
        if isinstance(code, str) and not code.startswith("s3://"):
            path = (base / code).resolve()
            if path.is_dir():
                return path, None
            warnings.append(f"CodeUri {code} is not a directory next to the template")
            return None, None
        warnings.append("code is in S3 or inline; use --from-aws to download it, or point CodeUri at a local folder")
        return None, None
    code = props.get("Code") or {}
    if "ZipFile" in code:
        warnings.append("inline ZipFile code is not extracted; move it into a source folder")
    else:
        warnings.append("Code points at S3; use the aws loader to download it")
    return None, None


def _sam_event(ev_id: str, ev: dict[str, Any], resources: dict[str, Any], warnings: list[str]) -> list[Trigger]:
    etype = ev.get("Type")
    p = ev.get("Properties") or {}
    if etype == "Api":
        return [Trigger(HTTP, {"method": str(p.get("Method", "ANY")).upper(), "path": p.get("Path", "/"), "event_format": "1.0", "source": "api-gateway-rest"})]
    if etype == "HttpApi":
        fmt = str(p.get("PayloadFormatVersion", "2.0"))
        return [Trigger(HTTP, {"method": str(p.get("Method", "ANY")).upper(), "path": p.get("Path"), "event_format": fmt, "source": "api-gateway-http"})]
    if etype == "SQS":
        arn, url = _queue_ref(p.get("Queue"), resources, warnings)
        return [Trigger(SQS, {
            "queue_arn": arn, "queue_url": url,
            "batch_size": int(p.get("BatchSize", 10)),
            "report_batch_item_failures": "ReportBatchItemFailures" in (p.get("FunctionResponseTypes") or []),
        })]
    if etype in ("Schedule", "ScheduleV2"):
        expr = p.get("Schedule") or p.get("ScheduleExpression")
        if not isinstance(expr, str):
            warnings.append(f"event {ev_id}: schedule expression is not a literal")
            return []
        if p.get("Enabled") is False or p.get("State") == "DISABLED":
            warnings.append(f"event {ev_id}: schedule is disabled in the template; CronJob is created suspended")
        return [Trigger(SCHEDULE, {"expression": expr, "input": p.get("Input") if isinstance(p.get("Input"), str) else None,
                                   "suspended": p.get("Enabled") is False or p.get("State") == "DISABLED"})]
    source = _UNSUPPORTED_EVENTS.get(etype, str(etype).lower())
    return [Trigger(UNSUPPORTED, {"source": source, "detail": f"SAM event {ev_id} ({etype})"})]


def _cfn_triggers(logical_id: str, resources: dict[str, Any], warnings: list[str]) -> list[Trigger]:
    """Raw CloudFormation: event source mappings and schedule rules that target this function."""
    out = []
    for rid, res in resources.items():
        p = res.get("Properties") or {}
        if res.get("Type") == "AWS::Lambda::EventSourceMapping" and _refers_to(p.get("FunctionName"), logical_id):
            src = p.get("EventSourceArn")
            src_text = src if isinstance(src, str) else json.dumps(src)
            if ":sqs:" in src_text or _is_queue_ref(src, resources):
                arn, url = _queue_ref(src, resources, warnings)
                out.append(Trigger(SQS, {"queue_arn": arn, "queue_url": url, "batch_size": int(p.get("BatchSize", 10)),
                                         "report_batch_item_failures": "ReportBatchItemFailures" in (p.get("FunctionResponseTypes") or [])}))
            else:
                out.append(Trigger(UNSUPPORTED, {"source": "stream", "detail": f"EventSourceMapping {rid} from {src_text}"}))
        if res.get("Type") == "AWS::Events::Rule":
            if any(_refers_to(t.get("Arn"), logical_id) for t in p.get("Targets") or []):
                if isinstance(p.get("ScheduleExpression"), str):
                    target = next(t for t in p["Targets"] if _refers_to(t.get("Arn"), logical_id))
                    out.append(Trigger(SCHEDULE, {"expression": p["ScheduleExpression"],
                                                  "input": target.get("Input") if isinstance(target.get("Input"), str) else None,
                                                  "suspended": p.get("State") == "DISABLED"}))
                else:
                    out.append(Trigger(UNSUPPORTED, {"source": "eventbridge-rule", "detail": f"Events rule {rid} (event pattern)"}))
    return out


def _refers_to(value: Any, logical_id: str) -> bool:
    if isinstance(value, dict):
        if value.get("Ref") == logical_id:
            return True
        ga = value.get("Fn::GetAtt")
        if isinstance(ga, list) and ga and ga[0] == logical_id:
            return True
    return value == logical_id


def _is_queue_ref(value: Any, resources: dict[str, Any]) -> bool:
    rid = _ref_id(value)
    return bool(rid and resources.get(rid, {}).get("Type") == "AWS::SQS::Queue")


def _ref_id(value: Any) -> str | None:
    if isinstance(value, dict):
        if isinstance(value.get("Ref"), str):
            return value["Ref"]
        ga = value.get("Fn::GetAtt")
        if isinstance(ga, list) and ga:
            return ga[0]
    return None


def _queue_ref(value: Any, resources: dict[str, Any], warnings: list[str]) -> tuple[str | None, str | None]:
    if isinstance(value, str) and value.startswith("arn:aws"):
        return value, _queue_url_from_arn(value)
    rid = _ref_id(value)
    if rid:
        qname = ((resources.get(rid) or {}).get("Properties") or {}).get("QueueName")
        if isinstance(qname, str):
            warnings.append(f"queue {rid} is created by the template; its URL needs your account and region (REPLACE_ME placeholders)")
            return (f"arn:aws:sqs:REPLACE_ME_REGION:REPLACE_ME_ACCOUNT:{qname}",
                    f"https://sqs.REPLACE_ME_REGION.amazonaws.com/REPLACE_ME_ACCOUNT/{qname}")
        warnings.append(f"queue {rid} has a generated name; set L2E_SQS_QUEUE_URL and the KEDA queueURL by hand")
        return None, None
    warnings.append(f"SQS queue reference {_describe(value)} could not be resolved")
    return None, None


def _queue_url_from_arn(arn: str) -> str | None:
    parts = arn.split(":")
    if len(parts) == 6 and parts[2] == "sqs":
        return f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}"
    return None


def _dlq(props: dict[str, Any]) -> str | None:
    dlq = props.get("DeadLetterQueue") or props.get("DeadLetterConfig")
    if not dlq:
        return None
    target = dlq.get("TargetArn") if isinstance(dlq, dict) else None
    return target if isinstance(target, str) else _describe(target)


def _describe(value: Any) -> str:
    return json.dumps(value, default=str)[:120]
