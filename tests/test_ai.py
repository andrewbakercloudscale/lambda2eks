import json
from types import SimpleNamespace

import pytest

from lambda2eks import ai
from lambda2eks.plan import baseline_plan


def answer(**overrides):
    base = {
        "summary": "Thin API over DynamoDB.",
        "migration_readiness": "ready_with_changes",
        "concurrency_safe": True,
        "settings": {k: {"value": None, "reason": ""} for k in ai.SETTINGS},
        "secret_env_keys": [],
        "iam_actions": ["dynamodb:GetItem"],
        "findings": [],
        "code_changes": [],
        "trigger_advice": [],
    }
    for k, v in overrides.items():
        if k in ai.SETTINGS:
            base["settings"][k] = {"value": v, "reason": f"because {k}"}
        else:
            base[k] = v
    return base


class FakeStream:
    def __init__(self, message, calls, kwargs):
        self.message, self.calls, self.kwargs = message, calls, kwargs

    def __enter__(self):
        self.calls.append(self.kwargs)
        return self

    def __exit__(self, *a):
        return False

    def get_final_message(self):
        return self.message


def fake_client(message):
    calls = []
    client = SimpleNamespace(beta=SimpleNamespace(messages=SimpleNamespace(
        stream=lambda **kw: FakeStream(message, calls, kw))))
    return client, calls


def message(text, stop_reason="end_turn"):
    content = [SimpleNamespace(type="thinking", thinking=""), SimpleNamespace(type="text", text=text)] if text else []
    return SimpleNamespace(stop_reason=stop_reason, content=content, model="claude-opus-5-5")


def test_review_request_shape(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    client, calls = fake_client(message(json.dumps(answer())))
    result = ai.review(plan, client=client)
    assert result["summary"] == "Thin API over DynamoDB."
    [kw] = calls
    assert kw["model"] == "claude-opus-5-5"
    assert kw["fallbacks"] == "default" and kw["betas"] == [ai.FALLBACK_BETA]
    assert kw["system"][0]["cache_control"] == {"type": "ephemeral"}
    assert kw["output_config"]["format"]["type"] == "json_schema"
    assert "thinking" not in kw and "temperature" not in kw
    user = kw["messages"][0]["content"]
    assert '<file path="app.py">' in user and "sk_test_do_not_ship" not in user


@pytest.mark.parametrize("msg, err", [
    (message(None, "refusal"), "declined"),
    (message("{", "max_tokens"), "output limit"),
    (message("not json"), "not valid JSON"),
    (message(None), "no answer"),
])
def test_review_failures_are_loud(sample_specs, msg, err):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    client, _ = fake_client(msg)
    with pytest.raises(ai.AIReviewError, match=err):
        ai.review(plan, client=client)


def test_apply_changes_with_reasons(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(concurrency=8, ingress_scheme="internet-facing", cpu_millicores=250))
    assert plan.concurrency == 8 and plan.ingress_scheme == "internet-facing" and plan.cpu_millicores == 250
    d = plan.decisions["concurrency"]
    assert d.source == "ai" and d.baseline == 1 and d.reason == "because concurrency"
    assert plan.decisions["memory_mb"].source == "rule"
    assert plan.iam_actions == ["dynamodb:GetItem"]


def test_apply_bounds_and_enums(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(concurrency=500, ingress_scheme="public", memory_mb="lots"))
    assert plan.concurrency == 64
    assert plan.ingress_scheme == "internal" and plan.memory_mb == 512
    assert any("clamped" in n for n in plan.ai_notes)
    assert any("ingress_scheme" in n for n in plan.ai_notes) and any("memory_mb" in n for n in plan.ai_notes)


def test_unsafe_handler_keeps_concurrency_1(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(concurrency=8, concurrency_safe=False))
    assert plan.concurrency == 1


def test_secret_keys_only_grow(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(secret_env_keys=["TABLE_NAME", "NOT_AN_ENV_VAR"]))
    assert plan.secret_env_keys == ["STRIPE_API_KEY", "TABLE_NAME"]
    plan2 = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan2, answer(secret_env_keys=[]))
    assert plan2.secret_env_keys == ["STRIPE_API_KEY"]


def test_min_above_max_is_fixed(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(http_min_replicas=12, http_max_replicas=4))
    assert plan.http_max_replicas == 12


def test_findings_sorted_and_empty_patches_dropped(sample_specs):
    plan = baseline_plan(sample_specs["orders-api"], "r")
    ai.apply_review(plan, answer(
        findings=[{"severity": "low", "title": "a", "detail": "", "file": None, "line": None},
                  {"severity": "blocker", "title": "b", "detail": "", "file": "app.py", "line": 3}],
        code_changes=[{"file": "app.py", "description": "x", "patch": "  "}],
    ))
    assert [f["severity"] for f in plan.findings] == ["blocker", "low"]
    assert plan.code_changes == []


@pytest.mark.parametrize("key, value, hidden", [
    ("DB_PASSWORD", "hunter2", True),
    ("DATABASE_URL", "postgres://app:s3cret@db:5432/x", True),
    ("WEBHOOK", "Zk9s8Q2vX1mP4rT7yB3nL6", True),
    ("TABLE_NAME", "orders", False),
    ("BASE_URL", "https://example.com/api/v1/endpoint", False),
])
def test_redaction(key, value, hidden):
    out = ai.redact_env_value(key, value)
    secret = value.split(":")[2].split("@")[0] if "@" in value else value
    assert (secret not in out) == hidden


def test_collect_sources_skips_vendored(tmp_path):
    (tmp_path / "app.py").write_text("x = 1")
    (tmp_path / "requests").mkdir()
    (tmp_path / "requests" / "api.py").write_text("vendored")
    (tmp_path / "requests-2.31.0.dist-info").mkdir()
    (tmp_path / "node_modules" / "lib").mkdir(parents=True)
    (tmp_path / "node_modules" / "lib" / "i.js").write_text("vendored")
    (tmp_path / ".env").write_text("SECRET=1")
    (tmp_path / "big.json").write_text("x" * (ai.MAX_FILE_CHARS + 1))
    files, omitted = ai.collect_sources(tmp_path)
    assert [f for f, _ in files] == ["app.py"]
    assert omitted and "big.json" in omitted[0]


def test_schema_is_strict():
    schema = ai.response_schema()

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                assert node.get("additionalProperties") is False
                assert set(node["required"]) == set(node["properties"])
            for v in node.values():
                walk(v)
        elif isinstance(node, list):
            for v in node:
                walk(v)
    walk(schema)
