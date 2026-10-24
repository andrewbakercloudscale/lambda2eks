# lambda2eks

Moves AWS Lambda functions to EKS. For each function it writes a container build, Kubernetes manifests, an IAM trust policy and a migration report. **Claude reads each function's code** and makes the decisions a rule can't make safely.

## What the AI review decides

The rules produce a starting plan with Lambda-parity defaults. Claude (`claude-opus-5-5`) then gets the function's config, that plan and the source code, and returns structured JSON:

| Decision | Why it needs the code |
|---|---|
| Concurrency per pod | Lambda never runs two invocations in one process. Raising it is only safe if the handler has no shared mutable state, and only useful if the work is I/O-bound. |
| CPU / memory | Lambda ties CPU to memory. The right size depends on what the code does. |
| Replica bounds, ingress scheme | Is the API public? Is there an authorizer or auth in the code? |
| CronJob overlap policy | Would two overlapping runs double-process data? |
| Which env vars are secrets | Judged by name, value shape and how the code uses them. |
| IAM actions used | Read from the SDK calls, for a least-privilege review. |
| Findings + suggested patches | Lambda-only features, non-idempotent consumers, cold-start work, writes outside `/tmp`, and so on. |
| Unsupported triggers | A specific plan for each (e.g. S3 → SQS → worker). |

Guard rails:
- Every AI change is bounded (e.g. concurrency 1–64) and shown in the report beside the rule's value and the AI's reason.
- The AI can add secret keys but never remove one the name rule caught.
- Patches are written to `ai/patches/` for review. Nothing is applied to your code.
- Env var values that look like credentials are redacted before they are sent. `.env` files and vendored packages are never sent.
- **If the review fails, the run exits 1** and the report says so. Rule-only output needs an explicit `--no-ai`.

## Trigger mapping

| Lambda | EKS |
|---|---|
| API Gateway REST (payload 1.0), HTTP API / function URL (2.0) | Deployment + Service + ALB Ingress + HPA + PDB |
| SQS event source | Worker Deployment + KEDA ScaledObject (scales to zero) |
| EventBridge schedule | CronJob (UTC, 2 retries like async Lambda) |
| No trigger | Deployment + Service exposing the Lambda Invoke API path |
| S3, SNS, DynamoDB/Kinesis streams, event-pattern rules | Reported, with AI advice |

The image is the AWS Lambda base image for the runtime (Python 3.x, Node.js 18+). A shim (`lambda2eks/shims/`) takes the place of the Lambda runtime and calls your **unchanged** handler. It builds API Gateway events (with path parameters), converts responses, handles SQS batches including `ReportBatchItemFailures`, provides the `context` object, and enforces the timeout.

## Usage

```bash
pip install -e '.[aws,test]'
export ANTHROPIC_API_KEY=...          # or `ant auth login`

# From a SAM / CloudFormation template
lambda2eks from-template template.yaml --registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com

# From deployed functions (read-only AWS calls; downloads the code)
lambda2eks from-aws orders-api order-worker --region eu-west-1

# Options
#   --function NAME      only some functions (template mode)
#   --iam irsa           IRSA instead of EKS Pod Identity
#   --namespace NS       default lambda-migration
#   --effort LEVEL       low|medium|high|xhigh|max (default high)
#   --model ID           default claude-opus-5-5
#   --no-ai              rule-based settings only
```

Output, per function in `out/<name>/`:

```
Dockerfile, build.sh, src/, lambda2eks-shim.*   build context
k8s/  (kustomization.yaml)                      kubectl apply -k k8s/
iam/trust-policy.json                           keeps the Lambda trust and adds EKS
ai/review.json, ai/patches/*.diff               the raw review and suggested fixes
MIGRATION.md                                    decisions, findings, steps, placeholders to fill
```

`out/SUMMARY.md` lists every function with its readiness and blocker count.

## Not migrated

Layers, extensions, VPC config, provisioned concurrency, destinations, async DLQs, and API Gateway authorizers, throttling and CORS. The report lists those that apply to each function.

## Tests

```bash
pytest
```

The shim tests run both shims for real, as HTTP servers, against payload formats 1.0 and 2.0. The AI tests use a fake client.
