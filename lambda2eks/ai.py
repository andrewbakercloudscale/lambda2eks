"""AI review: a model reads each function's code and config and decides how it should run on EKS.

Claude (Anthropic API) is the default. Open models on Together AI can do the review instead
(--provider together); the prompt, schema and guard rails are the same for both.

The rule-based plan is the starting point. Claude may change only the settings listed in
SETTINGS, each within the bounds checked in `apply_review`, and every change it makes is
recorded with its reason. Code changes come back as suggested patches that are written
to disk for a person to review; nothing is applied to the function's source.
"""

from __future__ import annotations

import json
import math
import os
import re
import ssl
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path
from typing import Any

import truststore

from .model import BRIDGED, HTTP, INVOKE, SCHEDULE, SQS, STREAMS
from .plan import MAX_REPLICAS_CAP, SECRET_KEY, Plan

DEFAULT_MODEL = "claude-opus-5-5"
DEFAULT_MODELS = {"anthropic": DEFAULT_MODEL, "together": "deepseek-ai/DeepSeek-V4-Pro-0813"}
FALLBACK_BETA = "server-side-fallback-2026-07-01"
TOGETHER_URL = "https://api.together.xyz/v1/chat/completions"

SOURCE_EXTS = {".py", ".js", ".mjs", ".cjs", ".ts", ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini", ".txt", ".sql", ".sh"}
SKIP_DIRS = {"node_modules", ".git", "__pycache__", ".venv", "venv", ".aws-sam", "dist-packages", "site-packages"}
SKIP_FILES = {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "poetry.lock"}
MAX_FILE_CHARS = 200_000
MAX_TOTAL_CHARS = 1_500_000

# workload kinds a setting acts on; settings missing here act on every workload
SETTING_KINDS: dict[str, tuple[str, ...]] = {
    "http_min_replicas": (HTTP, INVOKE), "http_max_replicas": (HTTP, INVOKE), "ingress_scheme": (HTTP,),
    "sqs_max_replicas": (SQS, *BRIDGED), "stream_max_replicas": STREAMS, "schedule_concurrency_policy": (SCHEDULE,),
}

# setting name -> (JSON type, description shown to the model)
SETTINGS: dict[str, tuple[str, str]] = {
    "concurrency": ("integer", "Invocations one pod runs at the same time (1-64). Lambda runs 1. Raise it only if the handler is safe to run concurrently in one process."),
    "cpu_millicores": ("integer", "CPU request in millicores (50-16000)."),
    "memory_mb": ("integer", "Memory request and limit in MB (64-32768)."),
    "http_min_replicas": ("integer", "Minimum replicas for HTTP/invoke workloads (1-200)."),
    "http_max_replicas": ("integer", "Maximum replicas for HTTP/invoke workloads (1-200)."),
    "sqs_max_replicas": ("integer", "Maximum replicas for SQS workers, including the S3/SNS bridge workers (1-200). KEDA scales to zero when the queue is empty."),
    "stream_max_replicas": ("integer", "Maximum replicas for Kinesis/DynamoDB stream consumers (1-200). KEDA runs about one pod per shard up to this."),
    "ingress_scheme": ("string", "ALB scheme: 'internal' or 'internet-facing'."),
    "schedule_concurrency_policy": ("string", "CronJob concurrencyPolicy: 'Allow', 'Forbid' or 'Replace'."),
}

SYSTEM_PROMPT = """You are a senior platform engineer reviewing one AWS Lambda function that is being moved to Amazon EKS by the lambda2eks tool. You get the function's configuration, the plan the tool's rules produced, and the function's source code. Your answer sets the plan's final values and tells the team what must change before the function runs well on Kubernetes.

How lambda2eks runs a function on EKS, so you know what is already handled:
- The image is the AWS Lambda base image for the runtime with the code copied to /var/task. A shim replaces the Lambda runtime and calls the unchanged handler.
- HTTP triggers (API Gateway REST = payload 1.0, HTTP API / function URL = payload 2.0) become a Deployment, Service, ALB Ingress and HPA. The shim builds the API Gateway event, including pathParameters from the route templates, and converts the handler's response back to HTTP.
- SQS triggers become a worker Deployment that long-polls the queue, passes Lambda-shaped batches to the handler, deletes on success, honours ReportBatchItemFailures, and leaves failures for the queue's redrive policy. KEDA scales it on queue depth, down to zero.
- S3 notifications and SNS subscriptions are moved to a new SQS "bridge" queue (with a DLQ after 3 tries). A worker reads it, rebuilds the exact event Lambda would have received (S3 Records, or the SNS Records envelope), and invokes the handler once per event. Delivery is at least once, so duplicates are possible, as on Lambda. The SNS subscription's filter policy and the S3 event types and prefix/suffix filters are copied to the new subscription and notification, so the handler sees the same events as before.
- Kinesis and DynamoDB Streams triggers become a consumer Deployment. Replicas share the shards through leases in a DynamoDB table, keep a checkpoint per shard, finish parent shards before children, and pass Lambda-shaped batches. BatchSize, MaximumRetryAttempts, BisectBatchOnFunctionError and ReportBatchItemFailures behave as on Lambda. ParallelizationFactor > 1, tumbling windows, FilterCriteria and on-failure destinations are NOT reproduced. A record can be processed twice after a crash or during the cut-over.
- EventBridge schedules become a CronJob (UTC) that sends the scheduled event or the rule's constant input. backoffLimit 2 matches Lambda's async retries.
- Functions with no trigger get a Deployment and Service exposing the Lambda Invoke API path (POST /2015-03-31/functions/function/invocations), so callers can switch endpoints.
- The context object provides function_name, memory_limit_in_mb, aws_request_id, invoked_function_arn, log_group_name, log_stream_name and get_remaining_time_in_millis (Node: the camelCase equivalents). The invocation timeout is enforced by answering 504 / failing the batch, but the handler keeps running in the background.
- AWS_LAMBDA_FUNCTION_NAME, AWS_LAMBDA_FUNCTION_MEMORY_SIZE, AWS_LAMBDA_FUNCTION_VERSION, AWS_REGION and AWS_DEFAULT_REGION are set. /tmp is an emptyDir sized like the function's ephemeral storage, and the root filesystem is read-only.
- IAM comes from EKS Pod Identity or IRSA using the function's execution role. Logs go to stdout, not to CloudWatch Logs unless the cluster ships them.
- Lambda layers, extensions, VPC config, provisioned concurrency, destinations, and API Gateway authorizers, throttling and CORS are NOT migrated by the tool.

What to decide:
1. Concurrency. Lambda never runs two invocations in one process, and handlers are often written on that assumption. Read the code. It is unsafe to raise concurrency if module-level mutable state is changed per request, /tmp paths are shared between invocations, non-thread-safe clients are reused, or (for Python) the work is CPU-bound, so threads gain nothing. Set concurrency_safe from what you see, and raise concurrency only when it is safe and the work is I/O-bound.
2. Resources. Lambda gives CPU in proportion to memory. Keep the memory unless the code clearly needs more or much less, and size the CPU for what the code does, e.g. image processing versus a thin API proxy.
3. Scaling bounds and the ingress scheme. Choose internet-facing only if the trigger was a public API with no sign the API was private. Without an authorizer or auth in code, say in a finding that the ALB has no authentication.
4. Overlapping scheduled runs. Use Forbid when overlapping runs would double-process or corrupt data.
5. Which environment variables hold secrets. Values were redacted before they reached you when they looked sensitive. Judge by name, redaction and use in code.
6. IAM actions the handler code itself calls, as service:Action strings, for a least-privilege review of the role. Leave out what the worker does for the handler (reading queues and streams, the lease table): lambda2eks grants that separately.
7. Findings: anything that breaks or behaves differently on EKS. Examples: reliance on Lambda-only features (layers, extensions, Powertools metrics through EMF without a log shipper, X-Ray daemon, Lambda destinations, context.callbackWaitsForEmptyEventLoop, invoking other Lambdas that are being migrated too), heavy cold-start work, writing outside /tmp, hard-coded Lambda paths, timeouts longer than load balancer limits, SQS, S3, SNS or stream handlers that are not idempotent (duplicates happen during the cut-over), code that relies on FilterCriteria having removed records, and missing dependencies. Give a file and line when you can. Severity: blocker = will not work until fixed; high = works but wrong or unsafe; medium = should fix; low = worth knowing.
8. Code changes: only minimal, concrete changes that a finding needs, as unified diffs against the files you were shown (paths as given). They are written out for review and never applied automatically. An empty list is fine.
9. Unsupported triggers (Kafka, MQ, EventBridge event patterns and others the tool does not convert): for each one listed, give a specific recommendation, e.g. route the EventBridge rule to an SQS queue and consume it as an SQS trigger, or keep the function on Lambda.

For every setting give a value and a one-sentence reason grounded in the code or config. Give null as the value to keep the plan's value. Give null for the settings in settings_not_used: this function has no workload they act on. Do not invent facts the input does not support. If the source is missing or was cut short, say so in the summary and decide conservatively."""


class AIReviewError(RuntimeError):
    pass


def _nullable(t: str) -> dict[str, Any]:
    return {"anyOf": [{"type": t}, {"type": "null"}]}


def response_schema() -> dict[str, Any]:
    setting = lambda t: {  # noqa: E731
        "type": "object", "additionalProperties": False, "required": ["value", "reason"],
        "properties": {"value": _nullable(t), "reason": {"type": "string"}},
    }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": ["summary", "migration_readiness", "concurrency_safe", "settings", "secret_env_keys",
                     "iam_actions", "findings", "code_changes", "trigger_advice"],
        "properties": {
            "summary": {"type": "string"},
            "migration_readiness": {"type": "string", "enum": ["ready", "ready_with_changes", "needs_rework", "not_recommended"]},
            "concurrency_safe": {"type": "boolean"},
            "settings": {
                "type": "object", "additionalProperties": False, "required": list(SETTINGS),
                "properties": {k: setting(t) for k, (t, _) in SETTINGS.items()},
            },
            "secret_env_keys": {"type": "array", "items": {"type": "string"}},
            "iam_actions": {"type": "array", "items": {"type": "string"}},
            "findings": {"type": "array", "items": {
                "type": "object", "additionalProperties": False,
                "required": ["severity", "title", "detail", "file", "line"],
                "properties": {
                    "severity": {"type": "string", "enum": ["blocker", "high", "medium", "low"]},
                    "title": {"type": "string"}, "detail": {"type": "string"},
                    "file": _nullable("string"), "line": _nullable("integer"),
                },
            }},
            "code_changes": {"type": "array", "items": {
                "type": "object", "additionalProperties": False, "required": ["file", "description", "patch"],
                "properties": {"file": {"type": "string"}, "description": {"type": "string"}, "patch": {"type": "string"}},
            }},
            "trigger_advice": {"type": "array", "items": {
                "type": "object", "additionalProperties": False, "required": ["trigger", "recommendation"],
                "properties": {"trigger": {"type": "string"}, "recommendation": {"type": "string"}},
            }},
        },
    }


# ---------------------------------------------------------------- input building

def _entropy(s: str) -> float:
    counts = Counter(s)
    return -sum(c / len(s) * math.log2(c / len(s)) for c in counts.values())


def _looks_random(s: str) -> bool:
    mixed = any(c.isdigit() for c in s) and any(c.isalpha() for c in s)
    return len(set(s)) == 1 or (mixed and _entropy(s) > 3.0)


def redact_env_value(key: str, value: str) -> str:
    """Hide anything that could be a credential; the model judges secrets from the name and shape."""
    if SECRET_KEY.search(key):
        return f"<redacted: {len(value)} chars>"
    value = re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1<redacted>@", value)
    if len(value) >= 20 and " " not in value and "/" not in value and _entropy(value) > 3.5:
        return f"<redacted: {len(value)} chars, high entropy>"
    # Capability URLs (webhooks, presigned links) carry the secret in a path segment or query value.
    value = re.sub(r"(?:(?<=[^/:]/)|(?<==))[A-Za-z0-9_\-+]{16,}(?=[/?&#]|$)",
                   lambda m: "<redacted>" if _looks_random(m.group()) else m.group(), value)
    return value[:300]


def collect_sources(code_dir: Path | None) -> tuple[list[tuple[str, str]], list[str]]:
    """Return ([(relative path, text)], [omitted files with reason]). Vendored packages are skipped."""
    if not code_dir or not code_dir.is_dir():
        return [], []
    vendored = {p.name.split("-")[0].lower().replace("_", "-") for p in code_dir.glob("*.dist-info")}
    vendored |= {p.name.split("-")[0].lower().replace("_", "-") for p in code_dir.glob("*.egg-info")}
    files, omitted, total = [], [], 0
    for path in sorted(code_dir.rglob("*")):
        rel = path.relative_to(code_dir)
        top = rel.parts[0]
        if any(part in SKIP_DIRS or part.endswith((".dist-info", ".egg-info")) for part in rel.parts):
            continue
        if len(rel.parts) > 1 and top.lower().replace("_", "-") in vendored:
            continue
        if not path.is_file() or path.suffix not in SOURCE_EXTS or path.name in SKIP_FILES or path.name.startswith(".env"):
            continue
        try:
            text = path.read_text()
        except UnicodeDecodeError:
            continue
        if len(text) > MAX_FILE_CHARS:
            omitted.append(f"{rel} ({len(text)} chars, over the per-file limit)")
            continue
        if total + len(text) > MAX_TOTAL_CHARS:
            omitted.append(f"{rel} (total source limit reached)")
            continue
        files.append((str(rel), text))
        total += len(text)
    return files, omitted


def _applies(plan: Plan, setting: str) -> bool:
    kinds = SETTING_KINDS.get(setting)
    return kinds is None or any(w.kind in kinds for w in plan.workloads)


def build_user_message(plan: Plan, files: list[tuple[str, str]], omitted: list[str]) -> str:
    spec = plan.spec
    config = {
        "name": spec.name, "runtime": spec.runtime, "handler": spec.handler,
        "memory_mb": spec.memory_mb, "timeout_s": spec.timeout_s, "ephemeral_storage_mb": spec.ephemeral_storage_mb,
        "architectures": spec.architectures, "layers": spec.layers, "vpc": spec.vpc,
        "reserved_concurrency": spec.reserved_concurrency, "dead_letter_target": spec.dead_letter_target,
        "container_image": spec.image_uri,
        "environment": {k: redact_env_value(k, v) for k, v in sorted(spec.environment.items())},
        "triggers": [{"kind": t.kind, **t.details} for t in spec.triggers],
        "loader_warnings": spec.warnings,
    }
    rule_plan = {
        "settings": {k: {"value": getattr(plan, k), "reason": plan.decisions[k].reason} for k in SETTINGS
                     if _applies(plan, k)},
        "settings_not_used": sorted(k for k in SETTINGS if not _applies(plan, k)),
        "secret_env_keys": plan.secret_env_keys,
        "workloads": [{"kind": w.kind, "name": w.name} for w in plan.workloads],
        "unsupported_triggers": [t.details for t in plan.unsupported_triggers],
    }
    parts = [
        "<function_config>", json.dumps(config, indent=2, default=str), "</function_config>",
        "<rule_plan>", json.dumps(rule_plan, indent=2, default=str), "</rule_plan>",
    ]
    if not files:
        parts.append("<source_code>No source code was available for this function.</source_code>")
    else:
        parts.append("<source_code>")
        for rel, text in files:
            parts.append(f'<file path="{rel}">\n{text}\n</file>')
        parts.append("</source_code>")
    if omitted:
        parts.append("<omitted_files>\n" + "\n".join(omitted) + "\n</omitted_files>")
    return "\n".join(parts)


# ---------------------------------------------------------------- the call

def resolve_provider(provider: str) -> str:
    """auto: Anthropic when its key is set, else Together when its key is set. Explicit choices pass through."""
    if provider != "auto":
        return provider
    if os.environ.get("ANTHROPIC_API_KEY"):
        return "anthropic"
    if os.environ.get("TOGETHER_API_KEY"):
        return "together"
    raise AIReviewError("no AI credentials: set ANTHROPIC_API_KEY or TOGETHER_API_KEY, or pass --no-ai")


def review(plan: Plan, model: str | None = None, effort: str = "high", client: Any = None, provider: str = "anthropic") -> dict[str, Any]:
    files, omitted = collect_sources(plan.spec.code_dir)
    if omitted:
        plan.ai_notes.append("not sent to the AI review: " + "; ".join(omitted))
    user = build_user_message(plan, files, omitted)
    model = model or DEFAULT_MODELS[provider]
    if provider == "together":
        text, plan.ai_model = _call_together(plan, model, user, client)
    else:
        text, plan.ai_model = _call_anthropic(plan, model, effort, user, client)
    try:
        result = json.loads(_json_text(text))
    except json.JSONDecodeError as e:
        raise AIReviewError(f"the review of {plan.spec.name} was not valid JSON: {e}") from e
    problems = check_result(result)
    if problems:
        raise AIReviewError(f"the review of {plan.spec.name} does not match the schema: {'; '.join(problems[:5])}")
    _flag_unknown_files(plan, result, [rel for rel, _ in files])
    return result


def _flag_unknown_files(plan: Plan, result: dict[str, Any], sent: list[str]) -> None:
    """Mark findings and patches that cite a file the model was never shown: a sign it made them up."""
    known = set(sent) | {Path(f).name for f in sent}
    for kind, items in (("finding", result.get("findings") or []), ("code change", result.get("code_changes") or [])):
        for item in items:
            f = item.get("file")
            if not f or not re.search(r"[./]", f) or f in known or Path(f).name in known:
                continue
            item["unverified"] = True
            item["title" if kind == "finding" else "description"] = "[UNVERIFIED: cites a file that was not sent] " + \
                item.get("title" if kind == "finding" else "description", "")
            plan.ai_notes.append(f"the AI review cites `{f}`, which is not in the source it was sent; check that {kind} by hand")


def _call_anthropic(plan: Plan, model: str, effort: str, user: str, client: Any) -> tuple[str, str]:
    import anthropic

    client = client or anthropic.Anthropic()
    try:
        with client.beta.messages.stream(
            model=model,
            max_tokens=64000,
            betas=[FALLBACK_BETA],
            fallbacks="default",
            system=[{"type": "text", "text": SYSTEM_PROMPT, "cache_control": {"type": "ephemeral"}}],
            output_config={"effort": effort, "format": {"type": "json_schema", "schema": response_schema()}},
            messages=[{"role": "user", "content": user}],
        ) as stream:
            message = stream.get_final_message()
    except anthropic.AuthenticationError as e:
        raise AIReviewError("Anthropic credentials rejected; set ANTHROPIC_API_KEY or run `ant auth login`, or pass --no-ai") from e
    except anthropic.RateLimitError as e:
        raise AIReviewError(f"rate limited by the Anthropic API: {e.message}") from e
    except anthropic.APIStatusError as e:
        raise AIReviewError(f"Anthropic API error {e.status_code}: {e.message}") from e
    except anthropic.APIConnectionError as e:
        raise AIReviewError(f"could not reach the Anthropic API: {e}") from e

    if message.stop_reason == "refusal":
        raise AIReviewError(f"the model declined to review {plan.spec.name}")
    if message.stop_reason == "max_tokens":
        raise AIReviewError(f"the review of {plan.spec.name} hit the output limit before finishing")
    text = next((b.text for b in message.content if b.type == "text"), None)
    if text is None:
        raise AIReviewError(f"the review of {plan.spec.name} returned no answer (stop_reason={message.stop_reason})")
    return text, message.model


def _call_together(plan: Plan, model: str, user: str, post: Any = None) -> tuple[str, str]:
    """Together's OpenAI-compatible chat API with a strict JSON schema. `post` replaces the HTTP call in tests."""
    key = os.environ.get("TOGETHER_API_KEY")
    if not key and post is None:
        raise AIReviewError("TOGETHER_API_KEY is not set; set it, choose --provider anthropic, or pass --no-ai")
    body = {
        "model": model,
        "max_tokens": 32000,
        "messages": [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user}],
        "response_format": {"type": "json_schema", "json_schema": {"name": "lambda_review", "strict": True, "schema": response_schema()}},
    }
    if post is None:
        def post(payload: dict[str, Any]) -> dict[str, Any]:
            req = urllib.request.Request(TOGETHER_URL, data=json.dumps(payload).encode(), method="POST", headers={
                "Authorization": f"Bearer {key}", "Content-Type": "application/json", "User-Agent": "lambda2eks"})
            # The OS trust store, like the Anthropic SDK: corporate TLS proxies add their CA there.
            ctx = truststore.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            with urllib.request.urlopen(req, timeout=900, context=ctx) as resp:  # noqa: S310 - fixed https URL
                return json.loads(resp.read())
    try:
        resp = post(body)
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", "replace")[:300]
        if e.code in (401, 403):
            raise AIReviewError(f"Together credentials rejected ({e.code}); check TOGETHER_API_KEY, or pass --no-ai") from e
        if e.code == 429:
            raise AIReviewError(f"rate limited by Together: {detail}") from e
        raise AIReviewError(f"Together API error {e.code}: {detail}") from e
    except (urllib.error.URLError, TimeoutError) as e:
        raise AIReviewError(f"could not reach the Together API: {e}") from e
    choice = (resp.get("choices") or [{}])[0]
    finish = choice.get("finish_reason")
    text = (choice.get("message") or {}).get("content")
    if finish == "length":
        raise AIReviewError(f"the review of {plan.spec.name} hit the output limit before finishing")
    if not text:
        raise AIReviewError(f"the review of {plan.spec.name} returned no answer (finish_reason={finish})")
    return text, f"together:{resp.get('model') or model}"


def _json_text(text: str) -> str:
    """The JSON object in a reply, without the Markdown fences or preamble some open models add."""
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.S).strip()
    if text.startswith("{"):
        return text
    start, end = text.find("{"), text.rfind("}")
    return text[start:end + 1] if 0 <= start < end else text


def check_result(result: Any) -> list[str]:
    """Shape problems in a review. Anthropic enforces the schema; open models may not, so check both."""
    if not isinstance(result, dict):
        return ["the answer is not an object"]
    problems = []
    for key, typ in (("summary", str), ("migration_readiness", str), ("concurrency_safe", bool), ("settings", dict),
                     ("secret_env_keys", list), ("iam_actions", list), ("findings", list), ("code_changes", list),
                     ("trigger_advice", list)):
        if not isinstance(result.get(key), typ):
            problems.append(f"{key} missing or not a {typ.__name__}")
    for i, f in enumerate(result.get("findings") if isinstance(result.get("findings"), list) else []):
        if not isinstance(f, dict) or f.get("severity") not in ("blocker", "high", "medium", "low") or not f.get("title"):
            problems.append(f"findings[{i}] has no valid severity and title")
    for i, c in enumerate(result.get("code_changes") if isinstance(result.get("code_changes"), list) else []):
        if not isinstance(c, dict) or not isinstance(c.get("file"), str) or not isinstance(c.get("patch"), str):
            problems.append(f"code_changes[{i}] needs file and patch strings")
    return problems


# ---------------------------------------------------------------- applying the answer

_BOUNDS = {
    "concurrency": (1, 64), "cpu_millicores": (50, 16000), "memory_mb": (64, 32768),
    "http_min_replicas": (1, MAX_REPLICAS_CAP), "http_max_replicas": (1, MAX_REPLICAS_CAP),
    "sqs_max_replicas": (1, MAX_REPLICAS_CAP), "stream_max_replicas": (1, MAX_REPLICAS_CAP),
}
_ENUMS = {"ingress_scheme": {"internal", "internet-facing"}, "schedule_concurrency_policy": {"Allow", "Forbid", "Replace"}}


def apply_review(plan: Plan, result: dict[str, Any]) -> None:
    """Apply the model's answer to the plan, keeping every value inside its bounds."""
    plan.ai_used = True
    plan.ai_summary = f"[{result.get('migration_readiness', 'unknown')}] {result.get('summary', '')}"

    settings = result.get("settings") or {}
    for key in SETTINGS:
        if not _applies(plan, key):
            continue
        entry = settings.get(key) or {}
        value, reason = entry.get("value"), (entry.get("reason") or "").strip()
        if value is None:
            if reason:
                plan.decisions[key].reason += f" (AI agreed: {reason})"
            continue
        if key in _BOUNDS:
            lo, hi = _BOUNDS[key]
            if not isinstance(value, int) or isinstance(value, bool):
                plan.ai_notes.append(f"ignored {key}={value!r}: not an integer")
                continue
            if not lo <= value <= hi:
                plan.ai_notes.append(f"{key}={value} is outside {lo}-{hi}; clamped")
                value = min(max(value, lo), hi)
        elif value not in _ENUMS[key]:
            plan.ai_notes.append(f"ignored {key}={value!r}: not one of {sorted(_ENUMS[key])}")
            continue
        if value != getattr(plan, key):
            plan.decide(key, value, "ai", reason or "no reason given")
        elif reason:
            plan.decisions[key].reason += f" (AI agreed: {reason})"

    if not result.get("concurrency_safe", False) and plan.concurrency != 1:
        plan.ai_notes.append(f"concurrency {plan.concurrency} reset to 1 because the review marked the handler not concurrency-safe")
        plan.decide("concurrency", 1, "ai", "the review found the handler is not safe to run concurrently")
    if plan.http_min_replicas > plan.http_max_replicas:
        plan.ai_notes.append("http_min_replicas was above http_max_replicas; max raised to match")
        plan.decide("http_max_replicas", plan.http_min_replicas, "ai", "raised to the minimum replica count")

    # The AI may add secret keys but not remove ones the name rule caught: erring towards a Secret is safe.
    ai_keys = {k for k in result.get("secret_env_keys") or [] if k in plan.spec.environment}
    dropped = set(result.get("secret_env_keys") or []) - set(plan.spec.environment)
    if dropped:
        plan.ai_notes.append(f"ignored secret keys that are not environment variables: {sorted(dropped)}")
    added = ai_keys - set(plan.secret_env_keys)
    if added:
        plan.decide("secret_env_keys", sorted(set(plan.secret_env_keys) | ai_keys), "ai",
                    f"the review also treats {', '.join(sorted(added))} as secret")

    plan.iam_actions = sorted({a for a in result.get("iam_actions") or [] if re.fullmatch(r"[a-z0-9-]+:[A-Za-z0-9*]+", a)})
    order = {"blocker": 0, "high": 1, "medium": 2, "low": 3}
    plan.findings = sorted(result.get("findings") or [], key=lambda f: order.get(f.get("severity"), 9))
    plan.code_changes = [c for c in result.get("code_changes") or [] if c.get("patch", "").strip()]
    plan.trigger_advice = [a for a in result.get("trigger_advice") or []
                           if isinstance(a, dict) and a.get("trigger") and a.get("recommendation")]
