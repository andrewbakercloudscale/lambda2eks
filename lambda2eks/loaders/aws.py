"""Load deployed functions from an AWS account (read-only API calls)."""

from __future__ import annotations

import io
import json
import urllib.request
import zipfile
from pathlib import Path
from typing import Any

from ..model import DYNAMODB, HTTP, KINESIS, S3, SCHEDULE, SNS, SQS, UNSUPPORTED, FunctionSpec, Trigger
from ..plan import k8s_name


def load_from_aws(names: list[str], download_to: Path, region: str | None = None, profile: str | None = None) -> list[FunctionSpec]:
    try:
        import boto3
    except ImportError as e:  # pragma: no cover - depends on environment
        raise SystemExit("the aws loader needs boto3: pip install 'lambda2eks[aws]'") from e

    session = boto3.Session(region_name=region, profile_name=profile)
    clients = {n: session.client(n) for n in ("lambda", "events", "apigatewayv2", "s3", "sns")}
    return [_load_one(clients, name, download_to) for name in names]


def _load_one(clients: dict[str, Any], name: str, download_to: Path) -> FunctionSpec:
    lam = clients["lambda"]
    fn = lam.get_function(FunctionName=name)
    cfg = fn["Configuration"]
    arn = cfg["FunctionArn"]
    _, _, _, region, account, *_ = arn.split(":")
    warnings: list[str] = []

    code_dir = image_uri = None
    if cfg.get("PackageType") == "Image":
        image_uri = fn.get("Code", {}).get("ImageUri")
    else:
        code_dir = download_to / k8s_name(cfg["FunctionName"]) / "src"
        _download_zip(fn["Code"]["Location"], code_dir)

    concurrency = None
    try:
        concurrency = lam.get_function_concurrency(FunctionName=name).get("ReservedConcurrentExecutions")
    except lam.exceptions.ResourceNotFoundException:
        pass

    spec = FunctionSpec(
        name=cfg["FunctionName"],
        runtime=cfg.get("Runtime"),
        handler=cfg.get("Handler"),
        memory_mb=cfg.get("MemorySize", 128),
        timeout_s=cfg.get("Timeout", 3),
        ephemeral_storage_mb=(cfg.get("EphemeralStorage") or {}).get("Size", 512),
        environment=(cfg.get("Environment") or {}).get("Variables") or {},
        role_arn=cfg.get("Role"),
        region=region,
        account_id=account,
        code_dir=code_dir,
        prebuilt=code_dir is not None,
        image_uri=image_uri,
        architectures=cfg.get("Architectures") or ["x86_64"],
        layers=[l["Arn"] for l in cfg.get("Layers") or []],
        vpc=bool((cfg.get("VpcConfig") or {}).get("SubnetIds")),
        reserved_concurrency=concurrency,
        dead_letter_target=(cfg.get("DeadLetterConfig") or {}).get("TargetArn"),
        warnings=warnings,
    )
    spec.triggers = _event_source_mappings(lam, name, warnings) + _policy_triggers(clients, name, arn, warnings)
    try:
        lam.get_function_url_config(FunctionName=name)
        spec.triggers.append(Trigger(HTTP, {"method": "ANY", "path": None, "event_format": "2.0", "source": "function-url"}))
    except lam.exceptions.ResourceNotFoundException:
        pass
    return spec


def _download_zip(url: str, dest: Path) -> None:
    with urllib.request.urlopen(url, timeout=120) as resp:  # noqa: S310 - presigned S3 URL from the Lambda API
        data = resp.read()
    dest.mkdir(parents=True, exist_ok=True)
    root = dest.resolve()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for member in zf.infolist():
            target = (root / member.filename).resolve()
            if not target.is_relative_to(root):
                raise ValueError(f"refusing zip entry outside the destination: {member.filename}")
        zf.extractall(root)


def _event_source_mappings(lam: Any, name: str, warnings: list[str]) -> list[Trigger]:
    out = []
    for page in lam.get_paginator("list_event_source_mappings").paginate(FunctionName=name):
        for m in page["EventSourceMappings"]:
            src = m.get("EventSourceArn", "")
            common = {"esm_uuid": m.get("UUID"), "enabled": m.get("State") == "Enabled", "filter_criteria": m.get("FilterCriteria"),
                      "report_batch_item_failures": "ReportBatchItemFailures" in (m.get("FunctionResponseTypes") or [])}
            if m.get("FilterCriteria"):
                warnings.append(f"event source mapping {m.get('UUID')}: FilterCriteria is not applied on EKS, so the handler sees every record")
            if ":sqs:" in src:
                parts = src.split(":")
                out.append(Trigger(SQS, {
                    "queue_arn": src,
                    "queue_url": f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}",
                    "batch_size": m.get("BatchSize", 10), **common,
                }))
            elif ":kinesis:" in src or ":dynamodb:" in src:
                kind = KINESIS if ":kinesis:" in src else DYNAMODB
                details = {
                    "stream_arn": src, "starting_position": m.get("StartingPosition", "LATEST"),
                    "starting_timestamp": str(m["StartingPositionTimestamp"]) if m.get("StartingPositionTimestamp") else None,
                    "batch_size": m.get("BatchSize", 100), "max_retry_attempts": m.get("MaximumRetryAttempts", -1),
                    "bisect_on_error": bool(m.get("BisectBatchOnFunctionError")),
                    "parallelization_factor": m.get("ParallelizationFactor", 1),
                    "detail": f"event source mapping {m.get('UUID')} from {src}", **common,
                }
                if kind == KINESIS:
                    details["stream_name"] = src.split("/", 1)[1]
                else:
                    details["table_name"] = src.split(":table/", 1)[1].split("/")[0]
                if details["parallelization_factor"] > 1:
                    warnings.append(f"{src}: ParallelizationFactor {details['parallelization_factor']} is not reproduced; one batch per shard at a time")
                if (m.get("DestinationConfig") or {}).get("OnFailure", {}).get("Destination"):
                    warnings.append(f"{src}: the on-failure destination is not reproduced; records that exhaust their retries are logged and skipped")
                out.append(Trigger(kind, details))
            else:
                kind = src.split(":")[2] if src.count(":") >= 2 else "unknown"
                out.append(Trigger(UNSUPPORTED, {"source": kind, "detail": f"event source mapping from {src or m.get('SelfManagedEventSource')}"}))
    return out


def _policy_triggers(clients: dict[str, Any], name: str, fn_arn: str, warnings: list[str]) -> list[Trigger]:
    lam = clients["lambda"]
    try:
        policy = json.loads(lam.get_policy(FunctionName=name)["Policy"])
    except lam.exceptions.ResourceNotFoundException:
        return []
    out: list[Trigger] = []
    for st in policy.get("Statement", []):
        principal = (st.get("Principal") or {}).get("Service", "")
        source_arn = ((st.get("Condition") or {}).get("ArnLike") or {}).get("AWS:SourceArn", "")
        if principal == "apigateway.amazonaws.com":
            out.extend(_apigw_trigger(clients["apigatewayv2"], source_arn, fn_arn, warnings))
        elif principal == "events.amazonaws.com":
            out.extend(_events_trigger(clients["events"], source_arn, fn_arn, warnings))
        elif principal == "s3.amazonaws.com" and source_arn.startswith("arn:aws:s3:::"):
            out.append(_s3_trigger(clients["s3"], source_arn.split(":::", 1)[1], fn_arn, warnings))
        elif principal == "s3.amazonaws.com":
            # SAM grants S3 with AWS:SourceAccount only (a SourceArn on a bucket in the same template would be
            # a circular dependency), so the bucket has to be found from the notification side.
            out.extend(_s3_triggers_by_scan(clients["s3"], fn_arn, warnings))
        elif principal == "sns.amazonaws.com" and source_arn.startswith("arn:aws:sns:"):
            out.append(_sns_trigger(clients["sns"], source_arn, fn_arn, warnings))
        elif principal:
            out.append(Trigger(UNSUPPORTED, {"source": principal.split(".")[0], "detail": f"{principal} via {source_arn or 'any source'}"}))
    return out


def _apigw_trigger(apigw2: Any, source_arn: str, fn_arn: str, warnings: list[str]) -> list[Trigger]:
    # arn:aws:execute-api:region:account:apiId/stage/METHOD/path...
    try:
        resource = source_arn.split(":", 5)[5]
        # apiId/stage/METHOD/path, but a quick-create HTTP API grants just apiId/* (any stage, method, path).
        api_id, *rest = resource.split("/")
        _stage, method, *path = rest + ["*"] * max(0, 2 - len(rest))
        if not api_id:
            raise ValueError(resource)
    except (IndexError, ValueError):
        warnings.append(f"could not parse API Gateway source ARN {source_arn}")
        return [Trigger(HTTP, {"method": "ANY", "path": None, "event_format": "1.0", "source": "api-gateway"})]
    path_s = "/" + "/".join(path) if path and path != ["*"] else None
    method = "ANY" if method in ("*", "") else method
    fmt, source = "1.0", "api-gateway-rest"
    try:
        apigw2.get_api(ApiId=api_id)
        source, fmt = "api-gateway-http", "2.0"
        for page in apigw2.get_paginator("get_integrations").paginate(ApiId=api_id):
            for integ in page["Items"]:
                if fn_arn in integ.get("IntegrationUri", ""):
                    fmt = integ.get("PayloadFormatVersion", "2.0")
    except apigw2.exceptions.NotFoundException:
        pass
    except Exception as e:  # noqa: BLE001 - permissions vary; fall back to REST format
        warnings.append(f"could not inspect API {api_id} ({e.__class__.__name__}); assuming REST payload format 1.0")
    if path_s and "*" in path_s:
        warnings.append(f"API Gateway permission for {source_arn} uses a wildcard path; routes are approximated")
        path_s = path_s.replace("*", "{proxy+}")
    return [Trigger(HTTP, {"method": method, "path": path_s, "event_format": fmt, "source": source})]


def _events_trigger(events: Any, source_arn: str, fn_arn: str, warnings: list[str]) -> list[Trigger]:
    rule_name = source_arn.split("/")[-1] if "/" in source_arn else None
    if not rule_name:
        return [Trigger(UNSUPPORTED, {"source": "eventbridge", "detail": f"EventBridge permission {source_arn}"})]
    bus = source_arn.split("/")[-2] if source_arn.count("/") > 1 else None
    kwargs = {"Name": rule_name, **({"EventBusName": bus} if bus else {})}
    try:
        rule = events.describe_rule(**kwargs)
    except Exception as e:  # noqa: BLE001
        warnings.append(f"could not read EventBridge rule {rule_name} ({e.__class__.__name__})")
        return []
    if not rule.get("ScheduleExpression"):
        return [Trigger(UNSUPPORTED, {"source": "eventbridge-rule", "detail": f"rule {rule_name} pattern {rule.get('EventPattern')}"})]
    payload = None
    for t in events.list_targets_by_rule(**{"Rule": rule_name, **({"EventBusName": bus} if bus else {})}).get("Targets", []):
        if t.get("Arn") == fn_arn:
            payload = t.get("Input")
    return [Trigger(SCHEDULE, {"expression": rule["ScheduleExpression"], "input": payload,
                               "rule_arn": rule.get("Arn"), "suspended": rule.get("State") == "DISABLED"})]


def _s3_trigger(s3: Any, bucket: str, fn_arn: str, warnings: list[str]) -> Trigger:
    events: list[str] = []
    rules: list[dict[str, str]] = []
    try:
        cfg = s3.get_bucket_notification_configuration(Bucket=bucket)
        for c in cfg.get("LambdaFunctionConfigurations") or []:
            if c.get("LambdaFunctionArn", "").split(":function:")[-1].split(":")[0] == fn_arn.split(":function:")[-1]:
                events += c.get("Events") or []
                rules += ((c.get("Filter") or {}).get("Key") or {}).get("FilterRules") or []
    except Exception as e:  # noqa: BLE001 - permissions vary
        warnings.append(f"could not read the notification configuration of bucket {bucket} ({e.__class__.__name__}); "
                        "events default to s3:ObjectCreated:*")
    return Trigger(S3, {"bucket": bucket, "events": events or ["s3:ObjectCreated:*"], "filter_rules": rules,
                        "detail": f"S3 bucket {bucket} notification"})


S3_SCAN_LIMIT = 1000


def _s3_triggers_by_scan(s3: Any, fn_arn: str, warnings: list[str]) -> list[Trigger]:
    region = fn_arn.split(":")[3]
    try:
        try:
            buckets = [b["Name"] for page in s3.get_paginator("list_buckets").paginate(BucketRegion=region)
                       for b in page.get("Buckets", [])]
        except Exception:  # noqa: BLE001 - older botocore has no BucketRegion filter
            buckets = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    except Exception as e:  # noqa: BLE001 - permissions vary
        warnings.append(f"S3 may invoke this function, but the buckets could not be listed ({e.__class__.__name__}); "
                        "add the S3 trigger by hand")
        return []
    if len(buckets) > S3_SCAN_LIMIT:
        warnings.append(f"S3 may invoke this function; only the first {S3_SCAN_LIMIT} of {len(buckets)} buckets were checked")
    want = fn_arn.split(":function:")[-1]
    out = []
    for bucket in buckets[:S3_SCAN_LIMIT]:
        try:
            cfg = s3.get_bucket_notification_configuration(Bucket=bucket)
        except Exception:  # noqa: BLE001 - another region or no access: not ours
            continue
        if any(c.get("LambdaFunctionArn", "").split(":function:")[-1].split(":")[0] == want
               for c in cfg.get("LambdaFunctionConfigurations") or []):
            out.append(_s3_trigger(s3, bucket, fn_arn, warnings))
    if not out:
        warnings.append("S3 may invoke this function (resource policy), but no bucket notification targets it")
    return out


def _sns_trigger(sns: Any, topic_arn: str, fn_arn: str, warnings: list[str]) -> Trigger:
    details: dict[str, Any] = {"topic_arn": topic_arn, "detail": f"SNS topic {topic_arn}"}
    try:
        for page in sns.get_paginator("list_subscriptions_by_topic").paginate(TopicArn=topic_arn):
            for sub in page["Subscriptions"]:
                if sub.get("Protocol") == "lambda" and sub.get("Endpoint", "").startswith(fn_arn):
                    attrs = sns.get_subscription_attributes(SubscriptionArn=sub["SubscriptionArn"])["Attributes"]
                    details.update(subscription_arn=sub["SubscriptionArn"], filter_policy=attrs.get("FilterPolicy"),
                                   filter_policy_scope=attrs.get("FilterPolicyScope"))
    except Exception as e:  # noqa: BLE001 - permissions vary
        warnings.append(f"could not read the subscriptions of {topic_arn} ({e.__class__.__name__}); copy any filter policy by hand")
    return Trigger(SNS, details)
