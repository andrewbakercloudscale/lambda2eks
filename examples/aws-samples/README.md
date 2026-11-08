# AWS's own samples, on real AWS, migrated to Kubernetes

Eleven sample Lambdas written by AWS (`samples/`, see `samples/SOURCES.md` for commits and licences) are
deployed to a real account, checked on Lambda, migrated with `lambda2eks from-aws --live`, and checked
again on Rancher Desktop's Kubernetes. The pods use the real queues, streams, buckets and topics. Since 8 Nov the run also covers EventBridge Scheduler and Step Functions.

```bash
.venv/bin/python examples/aws-samples/run.py all --region eu-west-1   # ~25 min; tears down AWS and shuts Rancher down
```

## Result (eu-west-1, 7-8 Nov 2026)

| sample | trigger | Lambda | EKS |
|---|---|---|---|
| apigw-http-api-lambda | HTTP API (payload 2.0) | PASS | PASS |
| apigw-rest-api-lambda-python | REST API, `requests` | PASS | PASS (egress from the pod) |
| lambda-function-url | function URL, AWS_IAM | PASS | PASS, but see below |
| sqs-lambda-python-sam | SQS | PASS | PASS |
| sns-lambda-nodejs-sam | SNS | PASS | PASS (bridge queue) |
| dynamodb-lambda | DynamoDB stream | PASS | PASS (stream consumer, leases) |
| kinesis-lambda | Kinesis | PASS after fix | PASS |
| eventbridge-scheduled-lambda | rate(1 minute) | PASS | PASS (CronJob) |
| sam-quickstart-s3 | S3, suffix filter | PASS | PASS (bridge queue) |
| eventbridge-schedule-to-lambda-dlq-sam-python | Scheduler rate(1 minute), no Input | PASS | PASS (CronJob, same event as Lambda got) |
| sfn-lambda-lambda | Step Functions Standard, Task on the function ARN | PASS | PASS (activity worker, machine rewritten at cut-over) |

Afterwards every stack, bucket, log group, queue, table and role was gone.

## What the run found in lambda2eks (fixed)

- **S3 trigger missed for SAM functions.** SAM grants S3 with `AWS:SourceAccount` only, no
  `SourceArn`, so `from-aws` made the function an invoke-only service. It now finds the bucket from
  the notification side.
- **HTTP API read as REST.** A quick-create HTTP API grants `apiId/*`; the parser wanted
  `apiId/stage/METHOD/path`, fell back to payload 1.0, and the handler crashed on
  `requestContext.http`. Fixed.
- **Stream and queue pods OOM-killed at 128 MB.** On Lambda the poller runs outside the function's
  memory; in the pod it shares it. Queue pods now get 64 Mi more, stream pods 96 Mi.
- **Deployed packages were reinstalled.** `from-aws` downloads the package Lambda ran, dependencies
  included, but the Dockerfile ran `npm ci` again, which re-resolved the lockfile against a registry
  the build could not reach. Deployed packages are now built as they are.
- The stream shims now log which shard they hold and from where.
- **Scheduler without Input does not send `{}`.** The shim assumed it did; real AWS sends a Scheduled
  Event with `source: aws.scheduler`, the schedule ARN in `resources` and `detail` as the string `"{}"`.
  The shim now builds that.
- **Scheduler names count against the stack name.** `${AWS::StackName}-schedule` must stay within 64
  characters; the harness uses a short stack name for that sample.
- **Step Functions needs no new execution role permission for activities.** The sample's role allows
  only `lambda:InvokeFunction` and the rewritten machine ran through the activity.

## Findings that stay findings

- **Function URL `AWS_IAM` auth does not exist on an Ingress.** The pod answers unsigned requests.
  Put an authenticating proxy or ALB OIDC in front, or keep the function URL.
- **SNS overlap.** The bridge queue is subscribed while Lambda still is; unsubscribe Lambda at cut-over
  (`CUTOVER.md` says so).
- **S3 cut-over is one-way for `from-aws`.** After the bucket notifies the bridge queue, a second
  `from-aws` run no longer sees an S3 trigger. Keep the first run's output.

## Upstream defects

| sample | defect | handling |
|---|---|---|
| `kinesis-lambda` | template `Handler: app.lambdaHandler`, `app.js` exports `handler`: every batch fails with `Runtime.HandlerNotFound` on Lambda itself and the shard stalls | `run.py` patches the staged copy (`UPSTREAM_FIXES`); `samples/` stays verbatim |
| `apigw-http-api-lambda` | `CodeUri` under `Globals`; `sam build` honours it, `aws cloudformation package` does not | `run.py` copies that code to the package root |

## Laptop-only adjustments (`run.py` overlay, not lambda2eks output)

Session credentials instead of Pod Identity, `imagePullPolicy: Never`, Traefik instead of the ALB, no
arch `nodeSelector` (Apple silicon runs x86_64 images emulated) and 128 Mi more for emulated pods.
