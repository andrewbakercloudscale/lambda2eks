"""EventBridge Scheduler schedules and Step Functions Task states: detection, rewriting, manifests, shims."""

import importlib.util
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml

from lambda2eks.generate import manifests, write_plan
from lambda2eks.loaders.aws import _scheduler_triggers, _sfn_triggers
from lambda2eks.loaders.template import load_template
from lambda2eks.model import SCHEDULE, SFN, UNSUPPORTED, FunctionSpec, Trigger
from lambda2eks.plan import baseline_plan
from lambda2eks.schedule import at_time, schedule_years, to_k8s_cron
from lambda2eks.stepfunctions import INVOKE_WRAPPER, find_tasks, rewrite

SHIMS = Path(__file__).resolve().parent.parent / "lambda2eks" / "shims"
FN = "arn:aws:lambda:eu-west-1:111122223333:function:worker"
ACTIVITY = "arn:aws:states:eu-west-1:111122223333:activity:l2e-worker"


def spec(**kw):
    s = FunctionSpec(name="worker", runtime="python3.13", handler="app.handler", region="eu-west-1",
                     account_id="111122223333", code_dir=Path("."), timeout_s=10)
    for k, v in kw.items():
        setattr(s, k, v)
    return s


# ------------------------------------------------------------------ schedule expressions

def test_one_time_schedule_runs_once():
    assert to_k8s_cron("at(2031-03-14T09:30:00)")[0] == "30 9 14 3 *"
    assert schedule_years("at(2031-03-14T09:30:00)") == "2031"
    assert at_time("at(2031-03-14T09:30:00)", "Europe/London").utcoffset().total_seconds() == 0
    assert at_time("rate(1 hour)", None) is None


def test_cron_year_becomes_a_filter():
    assert schedule_years("cron(0 9 * * ? 2027-2028)") == "2027-2028"
    assert schedule_years("cron(0 9 * * ? *)") is None
    assert schedule_years("cron(0 9 * * ? 2027/2)") is None  # a step the shim does not check: warned, not filtered
    assert "edit" not in to_k8s_cron("cron(0 9 * * ? 2027)")[1][0]


# ------------------------------------------------------------------ Step Functions definitions

DEFINITION = {
    "StartAt": "Direct",
    "States": {
        "Direct": {"Type": "Task", "Resource": FN, "Parameters": {"Payload.$": "$"}, "Next": "Invoke"},
        "Invoke": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke",
                   "Parameters": {"FunctionName": FN + ":$LATEST", "Payload.$": "$.order"}, "OutputPath": "$.Payload", "Next": "Fan"},
        "Fan": {"Type": "Parallel", "Next": "Each", "Branches": [{"StartAt": "Selected", "States": {
            "Selected": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke", "End": True,
                         "Parameters": {"FunctionName": "worker", "Payload": {"id.$": "$.id"}},
                         "ResultSelector": {"body.$": "$.Payload.body", "code.$": "$.StatusCode"}}}}]},
        "Each": {"Type": "Map", "Next": "Callback", "ItemProcessor": {"StartAt": "Item", "States": {
            "Item": {"Type": "Task", "Resource": FN, "End": True}}}},
        "Callback": {"Type": "Task", "Resource": "arn:aws:states:::lambda:invoke.waitForTaskToken",
                     "Parameters": {"FunctionName": FN, "Payload": {"token.$": "$$.Task.Token"}}, "Next": "Other"},
        "Other": {"Type": "Task", "Resource": FN.replace("worker", "someone-else"), "End": True},
    },
}


def test_find_tasks_walks_branches_and_maps():
    tasks = {t["path"]: t for t in find_tasks(DEFINITION, "worker")}
    assert set(tasks) == {"Direct", "Invoke", "Fan/branch0/Selected", "Each/map/Item", "Callback"}
    assert tasks["Direct"]["form"] == "direct" and tasks["Invoke"]["form"] == "invoke"
    assert tasks["Callback"]["problem"] and "waits for a task token" in tasks["Callback"]["problem"]
    assert all(t["problem"] is None for p, t in tasks.items() if p != "Callback")


def test_rewrite_keeps_what_later_states_see():
    new, done, notes = rewrite(DEFINITION, "worker", [], ACTIVITY, 10)
    states = new["States"]
    assert done == ["Direct", "Invoke", "Fan/branch0/Selected", "Each/map/Item"]
    assert states["Direct"] == {"Type": "Task", "Resource": ACTIVITY, "Parameters": {"Payload.$": "$"}, "Next": "Invoke",
                                "TimeoutSeconds": 310}
    # lambda:invoke: the payload path becomes the input, and the {Payload, StatusCode} wrapper is rebuilt.
    assert states["Invoke"]["InputPath"] == "$.order" and "Parameters" not in states["Invoke"]
    assert states["Invoke"]["ResultSelector"] == INVOKE_WRAPPER and states["Invoke"]["OutputPath"] == "$.Payload"
    sel = states["Fan"]["Branches"][0]["States"]["Selected"]
    assert sel["Parameters"] == {"id.$": "$.id"} and sel["ResultSelector"] == {"body.$": "$.body", "code": 200}
    assert states["Callback"]["Resource"].endswith("waitForTaskToken") and any("Callback" in n for n in notes)
    assert states["Other"]["Resource"].endswith("someone-else")
    assert DEFINITION["States"]["Direct"]["Resource"] == FN  # the input is not modified


def test_jsonata_is_left_alone():
    d = {"QueryLanguage": "JSONata", "StartAt": "A", "States": {"A": {"Type": "Task", "Resource": FN, "End": True}}}
    new, done, notes = rewrite(d, "worker", [], ACTIVITY, 3)
    assert done == [] and new == d and "JSONata" in notes[0]


# ------------------------------------------------------------------ plan and manifests

def sfn_trigger(kind="STANDARD", definition=DEFINITION):
    return Trigger(SFN, {"state_machine_arn": "arn:aws:states:eu-west-1:111122223333:stateMachine:orders",
                         "state_machine_name": "orders", "type": kind, "definition": definition, "refs": [FN],
                         "tasks": find_tasks(definition, "worker", [FN])})


def test_plan_makes_one_activity_worker_and_reports_express():
    plan = baseline_plan(spec(triggers=[sfn_trigger(), sfn_trigger("EXPRESS")]), "r")
    assert [(w.kind, w.name) for w in plan.workloads] == [(SFN, "worker-sfn")]
    assert [t.details["source"] for t in plan.unsupported_triggers] == ["stepfunctions-express"]
    assert any("Callback" in w for w in plan.spec.warnings)


def test_only_unconvertible_states_fall_back_to_invoke():
    d = {"StartAt": "C", "States": {"C": DEFINITION["States"]["Callback"] | {"End": True}}}
    plan = baseline_plan(spec(triggers=[sfn_trigger(definition=d)]), "r")
    assert [w.kind for w in plan.workloads] == ["invoke"] and plan.unsupported_triggers[0].kind == UNSUPPORTED


def test_activity_worker_manifests_infra_and_cutover(tmp_path):
    plan = baseline_plan(spec(triggers=[sfn_trigger()]), "r")
    dep = manifests(plan, "ns", "eu-west-1", "pod-identity")["worker-sfn.yaml"][0]
    c = dep["spec"]["template"]["spec"]["containers"][0]
    env = {e["name"]: e["value"] for e in c["env"]}
    assert env["L2E_MODE"] == "activity" and env["L2E_SFN_ACTIVITY_ARN"] == ACTIVITY
    assert dep["spec"]["replicas"] == 2 and "ports" not in c and c["resources"]["limits"]["memory"] == "192Mi"

    write_plan(plan, tmp_path, "ns", None, "pod-identity", None, fmt="both")
    infra = yaml.safe_load((tmp_path / "infra" / "resources.yaml").read_text())
    assert infra["Resources"]["Activity"] == {"Type": "AWS::StepFunctions::Activity", "Properties": {"Name": "l2e-worker"}}
    policy = json.loads((tmp_path / "iam" / "worker-policy.json").read_text())
    assert {"states:GetActivityTask", "states:SendTaskSuccess"} <= set(policy["Statement"][0]["Action"])
    new = json.loads((tmp_path / "cutover" / "sfn-orders.json").read_text())
    assert new["States"]["Direct"]["Resource"] == ACTIVITY
    runbook = (tmp_path / "cutover" / "CUTOVER.md").read_text()
    assert "update-state-machine" in runbook and "sfn-orders-before.json" in runbook
    assert '(index .Values.workloads "worker-sfn").replicas' in (tmp_path / "helm/templates/worker-sfn.yaml").read_text()
    assert "Step Functions orders" in (tmp_path / "MIGRATION.md").read_text()


def scheduler_trigger(**kw):
    return Trigger(SCHEDULE, {"expression": "cron(0 9 ? * MON-FRI *)", "timezone": "Africa/Johannesburg", "source": "scheduler",
                              "schedule_name": "daily", "group_name": "default", "input": '{"at": "<aws.scheduler.scheduled-time>"}',
                              "schedule_arn": "arn:aws:scheduler:eu-west-1:111122223333:schedule/default/daily", **kw})


def test_scheduler_cronjob_keeps_the_time_zone(tmp_path):
    plan = baseline_plan(spec(triggers=[scheduler_trigger()]), "r")
    cron = manifests(plan, "ns", "eu-west-1", "pod-identity")["worker-cron.yaml"][0]
    assert cron["spec"]["timeZone"] == "Africa/Johannesburg" and cron["spec"]["schedule"] == "0 9 * * MON-FRI"
    env = {e["name"]: e["value"] for e in cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["L2E_SCHEDULE_SOURCE"] == "scheduler" and env["L2E_SCHEDULE_ARN"].endswith("/default/daily")
    write_plan(plan, tmp_path, "ns", None, "pod-identity", None)
    runbook = (tmp_path / "cutover" / "CUTOVER.md").read_text()
    assert "aws scheduler get-schedule --name daily --group-name default" in runbook and '.State = \\"DISABLED\\"' not in runbook
    assert '.State = "DISABLED"' in runbook and '.State = "ENABLED"' in runbook


def test_past_one_time_schedule_is_suspended():
    plan = baseline_plan(spec(triggers=[scheduler_trigger(expression="at(2020-01-01T00:00:00)")]), "r")
    cron = manifests(plan, "ns", "eu-west-1", "pod-identity", live=True)["worker-cron.yaml"][0]
    env = {e["name"]: e["value"] for e in cron["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert cron["spec"]["suspend"] is True and env["L2E_SCHEDULE_YEARS"] == "2020"
    assert any("already run" in w for w in plan.spec.warnings)


# ------------------------------------------------------------------ loaders

class Pages:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kw):
        return self.pages


class FakeScheduler:
    def get_paginator(self, name):
        return Pages([{"Schedules": [{"Name": "daily", "GroupName": "default", "Target": {"Arn": FN}},
                                     {"Name": "other", "GroupName": "default", "Target": {"Arn": FN + "-2"}}]}])

    def get_schedule(self, Name, GroupName):
        return {"Arn": f"arn:aws:scheduler:eu-west-1:111122223333:schedule/{GroupName}/{Name}", "ScheduleExpression": "rate(5 minutes)",
                "ScheduleExpressionTimezone": "Europe/Dublin", "State": "ENABLED", "FlexibleTimeWindow": {"Mode": "FLEXIBLE"},
                "Target": {"Arn": FN, "Input": '{"a": 1}', "DeadLetterConfig": {"Arn": "arn:aws:sqs:eu-west-1:111122223333:dlq"}}}


class FakeSfn:
    def get_paginator(self, name):
        return Pages([{"stateMachines": [{"stateMachineArn": "arn:aws:states:eu-west-1:111122223333:stateMachine:orders"},
                                         {"stateMachineArn": "arn:aws:states:eu-west-1:111122223333:stateMachine:unrelated"}]}])

    def describe_state_machine(self, stateMachineArn):
        name = stateMachineArn.split(":")[-1]
        d = DEFINITION if name == "orders" else {"StartAt": "X", "States": {"X": {"Type": "Pass", "End": True}}}
        return {"stateMachineArn": stateMachineArn, "name": name, "type": "STANDARD", "definition": json.dumps(d)}


def test_aws_loader_finds_scheduler_schedules_and_state_machines():
    clients, warnings = {"scheduler": FakeScheduler(), "stepfunctions": FakeSfn()}, []
    [sched] = _scheduler_triggers(clients, FN, warnings)
    assert sched.details["timezone"] == "Europe/Dublin" and sched.details["source"] == "scheduler" and sched.details["input"] == '{"a": 1}'
    assert any("flexible" in w for w in warnings) and any("dead-letter" in w for w in warnings)
    [sfn] = _sfn_triggers(clients, "worker", FN, warnings)
    assert sfn.details["state_machine_name"] == "orders" and len(sfn.details["tasks"]) == 5


TEMPLATE = """
Transform: AWS::Serverless-2016-10-31
Resources:
  Worker:
    Type: AWS::Serverless::Function
    Properties:
      Handler: app.handler
      Runtime: python3.13
      CodeUri: src/
      Events:
        Daily:
          Type: ScheduleV2
          Properties:
            ScheduleExpression: cron(0 9 * * ? *)
            ScheduleExpressionTimezone: America/New_York
  Hourly:
    Type: AWS::Scheduler::Schedule
    Properties:
      ScheduleExpression: rate(1 hour)
      FlexibleTimeWindow: {Mode: 'OFF'}
      Target:
        Arn: !GetAtt Worker.Arn
        RoleArn: arn:aws:iam::111122223333:role/s
  Machine:
    Type: AWS::Serverless::StateMachine
    Properties:
      DefinitionUri: machine.asl.json
      DefinitionSubstitutions:
        WorkerArn: !GetAtt Worker.Arn
"""


def test_template_loader_reads_scheduler_and_state_machines(tmp_path):
    (tmp_path / "src").mkdir()
    (tmp_path / "template.yaml").write_text(TEMPLATE)
    (tmp_path / "machine.asl.json").write_text(json.dumps(
        {"StartAt": "W", "States": {"W": {"Type": "Task", "Resource": "${WorkerArn}", "End": True}}}))
    [s] = load_template(tmp_path / "template.yaml")
    scheds = [t for t in s.triggers if t.kind == SCHEDULE]
    assert [(t.details["expression"], t.details.get("timezone")) for t in scheds] == [
        ("cron(0 9 * * ? *)", "America/New_York"), ("rate(1 hour)", None)]
    [sfn] = [t for t in s.triggers if t.kind == SFN]
    assert sfn.details["tasks"] == [{"path": "W", "form": "direct", "problem": None}]


# ------------------------------------------------------------------ shims

def load_py_shim():
    mod = importlib.util.spec_from_file_location("l2e_shim", SHIMS / "shim.py")
    shim = importlib.util.module_from_spec(mod)
    mod.loader.exec_module(shim)
    return shim


class FakeStates:
    def __init__(self):
        self.calls = []

    def send_task_success(self, **kw):
        self.calls.append(("success", kw))

    def send_task_failure(self, **kw):
        self.calls.append(("failure", kw))

    def send_task_heartbeat(self, **kw):
        self.calls.append(("heartbeat", kw))


def test_python_activity_worker_reports_results_and_errors():
    shim = load_py_shim()

    def handler(event, context):
        if event.get("boom"):
            raise KeyError("missing")
        return {"seen": event}
    inv = shim.Invoker(handler, 1)
    sfn = FakeStates()
    assert shim.run_task(sfn, inv, {"taskToken": "t1", "input": '{"a": 1}'}, 0)
    assert not shim.run_task(sfn, inv, {"taskToken": "t2", "input": '{"boom": true}'}, 0)
    (k1, ok), (k2, bad) = sfn.calls
    assert k1 == "success" and json.loads(ok["output"]) == {"seen": {"a": 1}}
    assert k2 == "failure" and bad["error"] == "KeyError" and json.loads(bad["cause"])["errorType"] == "KeyError"
    assert shim.task_failure({"errorType": "TimeoutError", "errorMessage": "Task timed out after 3.00 seconds"})[0] == "Sandbox.Timedout"


def test_python_schedule_helpers(monkeypatch):
    shim = load_py_shim()
    assert shim.year_allowed("2026-2028,2031", 2027) and shim.year_allowed("2031", 2031) and not shim.year_allowed("2026-2028", 2029)
    monkeypatch.setenv("L2E_SCHEDULE_ARN", "arn:s")
    out = shim.scheduler_input('{"a": "<aws.scheduler.schedule-arn>", "t": "<aws.scheduler.scheduled-time>"}',
                               now=datetime(2026, 5, 1, 9, 0, 41, tzinfo=timezone.utc))
    assert json.loads(out) == {"a": "arn:s", "t": "2026-05-01T09:00:00Z"}
    monkeypatch.setenv("L2E_SCHEDULE_SOURCE", "scheduler")
    monkeypatch.delenv("L2E_SCHEDULE_INPUT", raising=False)
    ev = shim.schedule_event()
    assert (ev["source"], ev["detail-type"], ev["resources"], ev["detail"]) == ("aws.scheduler", "Scheduled Event", ["arn:s"], "{}")


@pytest.mark.skipif(not os.popen("which node").read(), reason="node missing")
def test_node_activity_worker_and_schedule_helpers():
    script = """
import { Invoker, runTask, taskFailure, yearAllowed, schedulerInput } from SHIM;
const calls = [];
const sfn = async (op, input) => { calls.push([op, input]); return {}; };
const inv = new Invoker(async (e) => { if (e.boom) { const err = new Error('nope'); err.name = 'OrderError'; throw err; } return { seen: e }; }, 1);
await runTask(sfn, inv, { taskToken: 't1', input: '{"a":1}' }, 0);
await runTask(sfn, inv, { taskToken: 't2', input: '{"boom":true}' }, 0);
process.env.L2E_SCHEDULE_ARN = 'arn:s';
console.log(JSON.stringify({ calls, timeout: taskFailure({ errorType: 'TimeoutError', errorMessage: 'Task timed out after 3.00 seconds' })[0],
  years: [yearAllowed('2026-2028,2031', 2027), yearAllowed('2026-2028', 2029)],
  input: schedulerInput('<aws.scheduler.schedule-arn>|<aws.scheduler.scheduled-time>', new Date('2026-05-01T09:00:41Z')) }));
""".replace("SHIM", json.dumps((SHIMS / "shim.mjs").as_uri()))
    stdout = subprocess.run(["node", "--input-type=module", "-e", script], capture_output=True, text=True, check=True).stdout
    out = json.loads(stdout.strip().splitlines()[-1])  # earlier lines are the shim's own log of the failed task
    (op1, ok), (op2, bad) = out["calls"]
    assert op1 == "SendTaskSuccess" and json.loads(ok["output"]) == {"seen": {"a": 1}}
    assert op2 == "SendTaskFailure" and bad["error"] == "OrderError"
    assert out["timeout"] == "Sandbox.Timedout" and out["years"] == [True, False]
    assert out["input"] == "arn:s|2026-05-01T09:00:00Z"
