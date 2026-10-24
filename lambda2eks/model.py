"""The normalised description of a Lambda function that every loader produces."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Trigger kinds the tool turns into a workload. Anything else is reported, not migrated.
HTTP = "http"          # API Gateway REST/HTTP API or a function URL -> Deployment + Service + Ingress
SQS = "sqs"            # SQS event source mapping -> worker Deployment + KEDA ScaledObject
SCHEDULE = "schedule"  # EventBridge schedule -> CronJob
INVOKE = "invoke"      # no trigger found: called through the Invoke API -> Deployment + Service
UNSUPPORTED = "unsupported"


@dataclass
class Trigger:
    kind: str
    # http: method, path, event_format ("1.0" REST, "2.0" HTTP API / function URL)
    # sqs: queue_arn, queue_url, batch_size, report_batch_item_failures
    # schedule: expression, input (JSON string or None)
    # unsupported: source (e.g. "s3", "dynamodb"), detail
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class FunctionSpec:
    name: str
    runtime: str | None
    handler: str | None
    memory_mb: int = 128
    timeout_s: int = 3
    ephemeral_storage_mb: int = 512
    environment: dict[str, str] = field(default_factory=dict)
    role_arn: str | None = None
    region: str | None = None
    account_id: str | None = None
    code_dir: Path | None = None
    image_uri: str | None = None
    architectures: list[str] = field(default_factory=lambda: ["x86_64"])
    layers: list[str] = field(default_factory=list)
    vpc: bool = False
    reserved_concurrency: int | None = None
    dead_letter_target: str | None = None
    triggers: list[Trigger] = field(default_factory=list)
    # Things the loader could not resolve; carried into the report.
    warnings: list[str] = field(default_factory=list)

    @property
    def runtime_family(self) -> str | None:
        if not self.runtime:
            return None
        if self.runtime.startswith("python"):
            return "python"
        if self.runtime.startswith("nodejs"):
            return "node"
        return None

    @property
    def arch(self) -> str:
        return "arm64" if "arm64" in self.architectures else "amd64"

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["code_dir"] = str(self.code_dir) if self.code_dir else None
        return d
