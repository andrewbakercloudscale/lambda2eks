# lambda2eks

Moves AWS Lambda functions to EKS. For each function it writes a container build, Kubernetes manifests, an IAM trust policy and a migration report. **An AI model reads each function's code** and makes the decisions a rule can't make safely: Claude through the Anthropic API, or open models through Together AI.

## What the AI review decides

The rules produce a starting plan with Lambda-parity defaults. The model (`claude-opus-5-5`, or `deepseek-ai/DeepSeek-V4-Pro-0813` on Together) then gets the function's config, that plan and the source code, and returns structured JSON:

| Decision | Why it needs the code |
|---|---|
| Concurrency per pod | Lambda never runs two invocations in one process. Raising it is only safe if the handler has no shared mutable state, and only useful if the work is I/O-bound. |
| CPU / memory | Lambda ties CPU to memory. The right size depends on what the code does. |
| Replica bounds, ingress scheme | Is the API public? Is there an authorizer or auth in the code? |
| CronJob overlap policy | Would two overlapping runs double-process data? |
| Which env vars are secrets | Judged by name, value shape and how the code uses them. |
| IAM actions used | Read from the SDK calls, for a least-privilege review. |
| Findings + suggested patches | Lambda-only features, non-idempotent consumers, cold-start work, writes outside `/tmp`, and so on. |
| Unsupported triggers | A specific plan for each. |
| Idempotency | Bridged and stream events can arrive twice, especially during the cut-over. |

Guard rails:
- Every AI change is bounded (e.g. concurrency 1–64) and shown in the report beside the rule's value and the AI's reason.
- The AI can add secret keys but never remove one the name rule caught.
- Patches are written to `ai/patches/` for review. Nothing is applied to your code.
- Env var values that look like credentials are redacted before they are sent. `.env` files and vendored packages are never sent.
- Settings for workloads the function does not have are not sent and are ignored if returned.
- Findings or patches that cite a file the model was never shown are marked `[UNVERIFIED]`.
- The answer is checked against the JSON schema; any mismatch fails the review.
- **If the review fails, the run exits 1** and the report says so. Rule-only output needs an explicit `--no-ai`.

## Trigger mapping

| Lambda | EKS |
|---|---|
| API Gateway REST (payload 1.0), HTTP API / function URL (2.0) | Deployment + Service + ALB Ingress + HPA + PDB |
| SQS event source | Worker Deployment + KEDA ScaledObject (scales to zero) |
| EventBridge schedule | CronJob (UTC, 2 retries like async Lambda) |
| No trigger | Deployment + Service exposing the Lambda Invoke API path |
| S3 notification, SNS subscription | New SQS **bridge** queue (+ DLQ) and a KEDA worker that rebuilds the exact S3/SNS event. Filter policy and prefix/suffix rules are copied. |
| Kinesis, DynamoDB Streams | Consumer Deployment + KEDA. Replicas share shards via leases in a DynamoDB table, checkpoint per shard, parent before child. BatchSize, MaximumRetryAttempts, BisectBatchOnFunctionError, ReportBatchItemFailures as on Lambda. |
| Event-pattern rules, other sources | Reported, with AI advice |

The image is the AWS Lambda base image for the runtime (Python 3.x, Node.js 18+). A shim (`lambda2eks/shims/`) takes the place of the Lambda runtime and calls your **unchanged** handler. It builds API Gateway events (with path parameters), converts responses, handles SQS batches including `ReportBatchItemFailures`, unwraps bridged S3/SNS events, runs the stream consumer, provides the `context` object, and enforces the timeout.

## Usage

```bash
pip install -e '.[aws,test]'
export ANTHROPIC_API_KEY=...          # or TOGETHER_API_KEY=... (auto picks Anthropic first)

# From a SAM / CloudFormation template
lambda2eks from-template template.yaml --registry 123456789012.dkr.ecr.eu-west-1.amazonaws.com

# From deployed functions (read-only AWS calls; downloads the code)
lambda2eks from-aws orders-api order-worker --region eu-west-1

# Options
#   --function NAME      only some functions (template mode)
#   --iam irsa           IRSA instead of EKS Pod Identity
#   --namespace NS       default lambda-migration
#   --provider P         auto|anthropic|together (default auto)
#   --model ID           default claude-opus-5-5 / deepseek-ai/DeepSeek-V4-Pro-0813
#   --effort LEVEL       low|medium|high|xhigh|max (default high, Claude only)
#   --no-ai              rule-based settings only
#   --format F           kustomize|helm|both (default kustomize)
#   --certificate-arn A  ACM certificate: HTTPS on the ALB
#   --live               start workers and CronJobs active (default: paused)
```

Output, per function in `out/<name>/`:

```
Dockerfile, build.sh, src/, lambda2eks-shim.*   build context
k8s/  (kustomization.yaml)                      kubectl apply -k k8s/
helm/                                           the same objects as a chart (--format helm|both)
iam/trust-policy.json                           keeps the Lambda trust and adds EKS
iam/worker-policy.json                          bridge queues, lease table, stream reads
infra/resources.yaml                            CloudFormation: bridge queues + DLQs, SNS subscription, lease table
cutover/CUTOVER.md, cutover/route53-weights.sh  per-trigger switch-over with rollback
ai/review.json, ai/patches/*.diff               the raw review and suggested fixes
MIGRATION.md                                    decisions, findings, steps, placeholders to fill
```

`out/SUMMARY.md` lists every function with its readiness and blocker count.

## Cut-over

Applying the output moves no events: queue and stream workers start paused (KEDA `paused-replicas: 0`), CronJobs start suspended, and SNS subscriptions stay off (`<Id>Subscribe=false`). `cutover/CUTOVER.md` then switches one trigger at a time, each with a rollback:

- HTTP: Route 53 weighted records, moved in steps by `route53-weights.sh init | set N | finish | rollback` (A records; it saves the original record set).
- SQS: unpause the worker, then disable the Lambda event source mapping.
- S3: switch the bucket notification to the bridge queue.
- SNS: subscribe the bridge queue, then remove the Lambda subscription (short overlap, so expect duplicates).
- Streams: start the consumer at `LATEST` next to the Lambda, wait for its IteratorAge to settle, then disable the mapping.
- Schedules: disable the rule, unsuspend the CronJob.

`--live` skips the pausing for greenfield installs.

## Not migrated

Layers, extensions, VPC config, FilterCriteria on event source mappings, ParallelizationFactor > 1, tumbling windows, provisioned concurrency, destinations, async DLQs, and API Gateway authorizers, throttling and CORS. The report lists those that apply to each function.

## End-to-end on a laptop

`examples/test-lambdas/` has ten test Lambdas, one for each trigger. `rancher/run.py all` migrates them onto Rancher Desktop's Kubernetes, with LocalStack standing in for the AWS account and KEDA installed. It then sends real events through every trigger and checks the results. See [its README](examples/test-lambdas/README.md).

AWS's own samples on a real account: `examples/aws-samples/run.py all` deploys nine of them, checks
them on Lambda, migrates them with `from-aws --live` and checks them again on Rancher Desktop with the
real queues, streams and buckets. 9/9 pass; [findings](examples/aws-samples/README.md).

## DynamoDB to PostgreSQL

`lambda2eks ddb2pg` moves a table to any PostgreSQL (`pip install 'lambda2eks[postgres]'`):

```bash
lambda2eks ddb2pg plan Orders --out ddb2pg                       # infer columns, indexes, entity views
lambda2eks ddb2pg apply    --database-url $PG --schema ddb2pg/orders.schema.json
lambda2eks ddb2pg backfill --database-url $PG --schema ddb2pg/orders.schema.json   # parallel Scan
lambda2eks ddb2pg sync     --database-url $PG --schema ddb2pg/orders.schema.json   # stream, until cut-over
lambda2eks ddb2pg verify   --database-url $PG --schema ddb2pg/orders.schema.json   # counts + sampled items
lambda2eks ddb2pg rewrite  ./src --schema ddb2pg/orders.schema.json                # AI patches, DynamoDB calls -> SQL
```

- Every item is kept whole in `doc jsonb`; keys, index attributes and consistently typed attributes
  also get typed columns. An attribute with mixed types stays in `doc` only, and is reported.
- Single-table designs get a view per entity prefix (`USER#`, `ORDER#`), from the partition or sort key.
- `sync` reads the stream from TRIM_HORIZON or its checkpoint (kept in PostgreSQL), parents before
  children; KEYS_ONLY and OLD_IMAGE streams are read back with a consistent GetItem.
- TTL becomes a note: schedule the `DELETE` yourself (pg_cron or a CronJob).

`examples/ddb2pg/run.py` runs it end to end on Rancher Desktop (LocalStack + PostgreSQL 17): seed,
plan, apply, backfill, change the table, sync, verify. 8/8 checks pass; Rancher is shut down after.

## Tests

```bash
pytest
```

The shim tests run both shims for real, as HTTP servers, against payload formats 1.0 and 2.0. The stream consumer and bridge tests use fake AWS clients. The AI tests use fake Anthropic and Together clients. The manifests are validated with `kubeconform -strict` against the Kubernetes and KEDA CRD schemas (skipped if kubeconform is not installed; `brew install kubeconform`), the Helm chart is rendered with `helm template` and compared with the kustomize output, and the Route 53 script runs a full cycle against a fake `aws`.

## License

MIT. See [LICENSE](LICENSE).
