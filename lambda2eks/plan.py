"""The migration plan for one function: rule-based defaults that the AI review may then adjust."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

from .model import BRIDGED, HTTP, INVOKE, SCHEDULE, SQS, STREAMS, UNSUPPORTED, FunctionSpec, Trigger

LAMBDA_MB_PER_VCPU = 1769  # Lambda allocates one full vCPU at 1,769 MB
SECRET_KEY = re.compile(r"(SECRET|PASSWORD|PASSWD|TOKEN|API_?KEY|PRIVATE|CREDENTIAL|DSN|CONN(ECTION)?_?STR|WEBHOOK)", re.I)
MAX_REPLICAS_CAP = 200


@dataclass
class Workload:
    kind: str            # http | invoke | sqs | schedule | s3 | sns | kinesis | dynamodb
    name: str            # Kubernetes object name
    trigger: Trigger | None
    bridge_queue: str | None = None  # s3/sns: the SQS queue lambda2eks puts between the source and the worker


@dataclass
class Decision:
    setting: str
    value: Any
    source: str          # "rule" or "ai"
    reason: str
    baseline: Any = None


@dataclass
class Plan:
    spec: FunctionSpec
    k8s_name: str
    image: str
    supported: bool = True
    unsupported_reason: str | None = None
    memory_mb: int = 128
    cpu_millicores: int = 100
    concurrency: int = 1
    http_min_replicas: int = 2
    http_max_replicas: int = 10
    sqs_max_replicas: int = 20
    stream_max_replicas: int = 4
    ingress_scheme: str = "internal"
    schedule_concurrency_policy: str = "Allow"
    secret_env_keys: list[str] = field(default_factory=list)
    workloads: list[Workload] = field(default_factory=list)
    unsupported_triggers: list[Trigger] = field(default_factory=list)
    decisions: dict[str, Decision] = field(default_factory=dict)
    # Filled by the AI review; empty when it is skipped.
    ai_used: bool = False
    ai_model: str | None = None
    ai_summary: str | None = None
    findings: list[dict[str, Any]] = field(default_factory=list)
    code_changes: list[dict[str, Any]] = field(default_factory=list)
    iam_actions: list[str] = field(default_factory=list)
    trigger_advice: list[dict[str, str]] = field(default_factory=list)
    ai_notes: list[str] = field(default_factory=list)

    def decide(self, setting: str, value: Any, source: str, reason: str) -> None:
        prev = self.decisions.get(setting)
        baseline = prev.value if prev and source == "ai" else None
        setattr(self, setting, value)
        self.decisions[setting] = Decision(setting, value, source, reason, baseline)


def _unique(plan: Plan, name: str) -> str:
    taken = {w.name for w in plan.workloads}
    n, i = name, 2
    while n in taken:
        n, i = f"{name[:48]}-{i}", i + 1
    return n


def k8s_name(name: str) -> str:
    n = re.sub(r"[^a-z0-9-]+", "-", name.lower()).strip("-")
    return (n or "fn")[:40].rstrip("-")


def baseline_plan(spec: FunctionSpec, registry: str) -> Plan:
    name = k8s_name(spec.name)
    plan = Plan(spec=spec, k8s_name=name, image=f"{registry.rstrip('/')}/{name}:latest")

    if spec.runtime_family is None and not spec.image_uri:
        plan.supported = False
        plan.unsupported_reason = (f"runtime {spec.runtime!r} has no shim yet (supported: python3.x, nodejs18.x+)"
                                   if spec.runtime else "runtime unknown")
    elif spec.image_uri and not spec.handler:
        plan.supported = False
        plan.unsupported_reason = "container-image function: pass the handler with --handler so the shim knows what to call"
    elif spec.image_uri and spec.runtime_family is None:
        plan.supported = False
        plan.unsupported_reason = "container-image function: pass --runtime (python or node) so the right shim is installed"
    elif not spec.image_uri and not spec.code_dir:
        plan.supported = False
        plan.unsupported_reason = "function code not found locally"

    plan.decide("memory_mb", max(spec.memory_mb, 128), "rule", "same memory as the Lambda (request = limit)")
    cpu = max(100, round(spec.memory_mb / LAMBDA_MB_PER_VCPU * 1000))
    plan.decide("cpu_millicores", cpu, "rule", f"Lambda CPU share at {spec.memory_mb} MB ({LAMBDA_MB_PER_VCPU} MB = 1 vCPU)")
    plan.decide("concurrency", 1, "rule", "Lambda runs one invocation per environment at a time")
    if spec.reserved_concurrency:
        cap = min(spec.reserved_concurrency, MAX_REPLICAS_CAP)
        plan.decide("http_max_replicas", cap, "rule", f"reserved concurrency {spec.reserved_concurrency} at 1 request per pod")
        plan.decide("sqs_max_replicas", cap, "rule", f"reserved concurrency {spec.reserved_concurrency} at 1 batch per pod")
        plan.decide("stream_max_replicas", cap, "rule", f"reserved concurrency {spec.reserved_concurrency}; KEDA also caps at one pod per shard")
    else:
        plan.decide("http_max_replicas", 10, "rule", "no reserved concurrency set; conservative ceiling")
        plan.decide("sqs_max_replicas", 20, "rule", "no reserved concurrency set; conservative ceiling")
        plan.decide("stream_max_replicas", 4, "rule", "no reserved concurrency set; KEDA scales with the shard count up to this")
    plan.decide("http_min_replicas", 2, "rule", "two replicas for availability across nodes")
    plan.decide("ingress_scheme", "internal", "rule", "internal load balancer until someone decides it should be public")
    plan.decide("schedule_concurrency_policy", "Allow", "rule", "EventBridge lets scheduled invocations overlap")
    keys = sorted(k for k in spec.environment if SECRET_KEY.search(k))
    plan.decide("secret_env_keys", keys, "rule", "names that look like credentials go into a Secret with placeholder values")

    http = [t for t in spec.triggers if t.kind == HTTP]
    if http:
        formats = {t.details.get("event_format", "1.0") for t in http}
        if len(formats) > 1:
            spec.warnings.append("HTTP triggers use both payload formats 1.0 and 2.0; one http workload per format is generated")
        for fmt in sorted(formats):
            suffix = "http" if len(formats) == 1 else f"http-v{fmt[0]}"
            plan.workloads.append(Workload(HTTP, f"{name}-{suffix}", Trigger(HTTP, {
                "event_format": fmt,
                "routes": [{"method": t.details.get("method", "ANY"), "path": t.details.get("path")}
                           for t in http if t.details.get("event_format", "1.0") == fmt],
                "sources": sorted({t.details.get("source", "api-gateway") for t in http}),
            })))
    sqs = [t for t in spec.triggers if t.kind == SQS]
    for i, t in enumerate(sqs):
        qname = (t.details.get("queue_arn") or f"queue{i}").split(":")[-1]
        plan.workloads.append(Workload(SQS, f"{name}-sqs-{k8s_name(qname)}"[:52].rstrip("-"), t))
    for i, t in enumerate(t for t in spec.triggers if t.kind == SCHEDULE):
        plan.workloads.append(Workload(SCHEDULE, f"{name}-cron-{i}" if i else f"{name}-cron", t))
    for t in (t for t in spec.triggers if t.kind in BRIDGED):
        source = t.details.get("bucket") if t.kind == "s3" else (t.details.get("topic_arn") or "topic").split(":")[-1]
        wname = _unique(plan, f"{name}-{t.kind}-{k8s_name(source or t.kind)}"[:52].rstrip("-"))
        plan.workloads.append(Workload(t.kind, wname, t, bridge_queue=f"l2e-{wname}"[:80]))
    for t in (t for t in spec.triggers if t.kind in STREAMS):
        source = t.details.get("stream_name") or t.details.get("table_name") or "stream"
        plan.workloads.append(Workload(t.kind, _unique(plan, f"{name}-{t.kind}-{k8s_name(source)}"[:52].rstrip("-")), t))
    plan.unsupported_triggers = [t for t in spec.triggers if t.kind == UNSUPPORTED]
    if not plan.workloads:
        plan.workloads.append(Workload(INVOKE, f"{name}-invoke", None))
    return plan
