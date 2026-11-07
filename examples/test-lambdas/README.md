# Test Lambdas on Rancher Desktop

This folder has ten test Lambda functions, one for each trigger lambda2eks handles plus one it doesn't. `rancher/run.py` migrates them onto the Kubernetes cluster in Rancher Desktop, then sends real events through each trigger and checks what every function recorded.

| Function | Runtime | Trigger | Becomes on Kubernetes |
|---|---|---|---|
| greeter-api | Python | HTTP API (2.0) | Deployment, Service, Ingress, HPA, PDB |
| todo-rest | Node | REST API (1.0) | Deployment, Service, Ingress, HPA, PDB |
| order-processor | Python | SQS, partial batch failures | KEDA worker (scales from 0) |
| upload-indexer | Node | S3, prefix filter | bridge SQS queue + KEDA worker |
| alert-router | Python | SNS, filter policy | bridge SQS queue + KEDA worker |
| inventory-audit | Python | DynamoDB stream | lease-based stream consumer |
| clickstream-counter | Node | Kinesis | lease-based stream consumer |
| nightly-cleanup | Python | EventBridge schedule | CronJob |
| price-calculator | Node | none (Invoke API), secret env | Deployment + Service |
| audit-shipper | Python | EventBridge pattern rule | reported, not migrated; Invoke API |

## Run

Rancher Desktop must have Kubernetes enabled, with the `rancher-desktop` context in kubectl and the moby (dockerd) engine, so images you build locally can be used without pushing them. The run was tested on k3s 1.35, with a VM of 4 CPUs and 8 GB.

```bash
pip install -e '.[aws,test]'                     # from the repo root
python examples/test-lambdas/rancher/run.py all  # up, seed, migrate, verify
```

The steps, one at a time:

| Step | What it does |
|---|---|
| `up` | Installs LocalStack 4.14.0 (pinned: the last release that runs without an auth token) and KEDA 2.21.0. LocalStack is reachable from the laptop on `localhost:4566`. |
| `seed` | Creates the AWS resources the functions already use: tables, the orders queue with its DLQ, the bucket, the topic and the stream. |
| `migrate` | Runs `lambda2eks from-template --live`, builds one image per function, deploys each `infra/resources.yaml` to LocalStack as a CloudFormation stack, points the bucket at its bridge queue, then runs `kubectl apply -k` with the local overlay. |
| `verify` | Sends HTTP requests, SQS messages, S3 uploads, SNS publishes, DynamoDB writes and Kinesis records. It runs a Job from the CronJob and calls the Invoke API. It then checks the `l2e-test-results` table and exits 1 if any check fails. |
| `down` | Deletes the migrated workloads. `down --all` also removes LocalStack, KEDA and the namespaces. |

`migrate --ai` runs the AI review as well. It needs `ANTHROPIC_API_KEY` or `TOGETHER_API_KEY`.

## What is different from EKS

The manifests are the ones lambda2eks generated, in `out/<fn>/k8s/`. Each function gets an overlay in `out/_rancher/<fn>/kustomization.yaml`, and the overlay changes only these things:

- Pods get `AWS_ENDPOINT_URL` set to LocalStack and test keys, instead of Pod Identity.
- SQS queue URLs use LocalStack's form. The JavaScript SDK sends SQS calls to the queue URL's host, so the AWS form would reach the real AWS.
- Images use `imagePullPolicy: Never`, since they are built into Rancher's dockerd and there is no ECR.
- The Ingress class is Traefik instead of the ALB controller. HTTP functions answer on `http://localhost/greet/...` and `http://localhost/todos`.
- KEDA's AWS scalers use a Secret with the test keys and `awsEndpoint`, instead of Pod Identity.
- Secret values come from the template, instead of Secrets Manager.

`--live` starts the workers and CronJobs active. That is safe here only because no Lambda is running. On a real migration, leave it off and follow `cutover/CUTOVER.md`.
