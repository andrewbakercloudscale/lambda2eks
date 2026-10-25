"""The normalised description of a Lambda function that every loader produces."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

# Trigger kinds the tool turns into a workload. Anything else is reported, not migrated.
HTTP = "http"          # API Gateway REST/HTTP API or a function URL -> Deployment + Service + Ingress
SQS = "sqs"            # SQS event source mapping -> worker Deployment + KEDA ScaledObject
SCHEDULE = "schedule"  # EventBridge schedule -> CronJob
S3 = "s3"              # S3 notifications -> bridge SQS queue -> worker that unwraps them
SNS = "sns"            # SNS subscription -> bridge SQS queue -> worker that rebuilds the SNS event
KINESIS = "kinesis"    # Kinesis event source mapping -> stream consumer + KEDA
DYNAMODB = "dynamodb"  # DynamoDB stream event source mapping -> stream consumer + KEDA
INVOKE = "invoke"      # no trigger found: called through the Invoke API -> Deployment + Service
UNSUPPORTED = "unsupported"

BRIDGED = (S3, SNS)
STREAMS = (KINESIS, DYNAMODB)


@dataclass
class Trigger:
    kind: str
    # http: method, path, event_format ("1.0" REST, "2.0" HTTP API / function URL)
    # sqs: queue_arn, queue_url, batch_size, report_batch_item_failures
    # schedule: expression, input (JSON string or None)
    # s3: bucket, events, filter_rules ([{"Name": "prefix", "Value": ...}])
    # sns: topic_arn, filter_policy, filter_policy_scope
    # kinesis/dynamodb: stream_arn, stream_name / table_name, starting_position, batch_size,
    #   max_retry_attempts, bisect_on_error, report_batch_item_failures, parallelization_factor
    # sqs/kinesis/dynamodb may carry esm_uuid (the event source mapping) and filter_criteria
    # unsupported: source (e.g. "kafka", "eventbridge-rule"), detail
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
