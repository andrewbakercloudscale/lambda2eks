from lambda2eks.model import HTTP, SCHEDULE, SQS, UNSUPPORTED
from lambda2eks.plan import baseline_plan, k8s_name


def test_loads_sample(sample_specs):
    api, worker, report = sample_specs["orders-api"], sample_specs["order-worker"], sample_specs["nightly-report"]
    assert api.runtime == "python3.12" and api.memory_mb == 512 and api.timeout_s == 10
    assert api.environment == {"LOG_LEVEL": "INFO", "TABLE_NAME": "orders", "STRIPE_API_KEY": "sk_test_do_not_ship"}
    assert [t.details["path"] for t in api.triggers] == ["/orders/{id}", "/orders"]
    assert all(t.kind == HTTP and t.details["event_format"] == "2.0" for t in api.triggers)
    assert api.account_id == "123456789012"

    kinds = [t.kind for t in worker.triggers]
    assert kinds == [SQS, UNSUPPORTED]
    sqs = worker.triggers[0].details
    assert sqs["queue_url"] == "https://sqs.eu-west-1.amazonaws.com/123456789012/orders"
    assert sqs["batch_size"] == 5 and sqs["report_batch_item_failures"] is True
    assert worker.region == "eu-west-1" and worker.arch == "arm64" and worker.reserved_concurrency == 5

    assert report.triggers[0].kind == SCHEDULE and report.triggers[0].details["input"] == '{"report": "daily"}'
    assert report.role_arn is None and any("Role is an intrinsic" in w for w in report.warnings)


def test_baseline_plan(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "reg.example.com")
    assert plan.supported
    assert plan.image == "reg.example.com/orders-api:latest"
    assert plan.cpu_millicores == round(512 / 1769 * 1000)
    assert plan.concurrency == 1
    assert plan.secret_env_keys == ["STRIPE_API_KEY"]
    [w] = plan.workloads
    assert w.kind == HTTP and len(w.trigger.details["routes"]) == 2

    worker = baseline_plan(sample_specs["order-worker"], "r")
    assert worker.sqs_max_replicas == 5
    assert [t.details["source"] for t in worker.unsupported_triggers] == ["s3"]


def test_unsupported_runtime(sample_specs):
    spec = sample_specs["orders-api"]
    spec.runtime = "java21"
    plan = baseline_plan(spec, "r")
    assert not plan.supported and "java21" in plan.unsupported_reason


def test_k8s_name():
    assert k8s_name("My_Function.Name") == "my-function-name"
    assert len(k8s_name("x" * 100)) <= 40
