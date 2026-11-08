"""Step Functions: find the Task states that call a function, and rewrite them to call an activity on EKS.

On Lambda a Task state calls the function, either directly (Resource is the function ARN) or through the
optimised integration (Resource arn:aws:states:::lambda:invoke, the function in Parameters.FunctionName).
On EKS the same state calls an activity, and the shim's activity mode polls it, runs the handler and
reports the result. Nothing has to reach the cluster from outside, and Retry/Catch keep working: a
handler error is reported with the same error name (the exception type) that Lambda would give.
"""

from __future__ import annotations

import copy
from collections.abc import Iterator
from typing import Any

LAMBDA_INVOKE = "arn:aws:states:::lambda:invoke"
# What the optimised integration wraps around the handler's result, kept so later states see the same shape.
INVOKE_WRAPPER = {"Payload.$": "$", "StatusCode": 200, "ExecutedVersion": "$LATEST"}
# Activity tasks wait for a worker with no limit of their own; a state without TimeoutSeconds would wait
# a year for a worker that died mid-task. This much on top of the function's timeout covers a queue of work.
TIMEOUT_MARGIN_S = 300


def refers(value: Any, name: str, refs: list[str] | tuple[str, ...] = ()) -> bool:
    """Whether a Resource or FunctionName value names this function (name, partial or full ARN, with or without qualifier)."""
    if not isinstance(value, str) or not value:
        return False
    if value in refs:
        return True
    tail = value.split(":function:", 1)[1] if ":function:" in value else value
    return tail.split(":")[0] == name


def iter_states(states: dict[str, Any], prefix: str = "") -> Iterator[tuple[str, dict[str, Any]]]:
    """(path, state) for every state, including those inside Parallel branches and Map iterators."""
    for sname, state in (states or {}).items():
        if not isinstance(state, dict):
            continue
        path = f"{prefix}{sname}"
        yield path, state
        for i, branch in enumerate(state.get("Branches") or []):
            yield from iter_states((branch or {}).get("States") or {}, f"{path}/branch{i}/")
        proc = state.get("ItemProcessor") or state.get("Iterator")
        if isinstance(proc, dict):
            yield from iter_states(proc.get("States") or {}, f"{path}/map/")


def _state_at(definition: dict[str, Any], path: str) -> dict[str, Any] | None:
    return next((s for p, s in iter_states(definition.get("States") or {}) if p == path), None)


def find_tasks(definition: dict[str, Any], name: str, refs: list[str] | tuple[str, ...] = ()) -> list[dict[str, Any]]:
    """The Task states that call this function: [{path, form, problem}]. problem is None when the state can be rewritten."""
    out = []
    top_jsonata = definition.get("QueryLanguage") == "JSONata"
    for path, state in iter_states(definition.get("States") or {}):
        if state.get("Type") != "Task":
            continue
        res = state.get("Resource")
        if not isinstance(res, str):
            continue
        args = state.get("Parameters") or state.get("Arguments") or {}
        if res.startswith(LAMBDA_INVOKE):
            if not refers(args.get("FunctionName"), name, refs):
                continue
            form = "invoke"
        elif not res.startswith("arn:aws:states:") and refers(res, name, refs):
            form = "direct"
        else:
            continue
        problem = None
        if top_jsonata or state.get("QueryLanguage") == "JSONata":
            problem = "uses JSONata; rewrite it by hand (Resource to the activity ARN, Arguments to the payload)"
        elif res.endswith(".waitForTaskToken"):
            problem = ("waits for a task token the function sends later; an activity has its own token, so keep this state "
                       "on Lambda or send the payload to an SQS queue with sqs:sendMessage.waitForTaskToken and consume it as an SQS trigger")
        elif form == "invoke" and args.get("InvocationType") in ("Event", "DryRun"):
            problem = f"invokes the function with InvocationType {args.get('InvocationType')}, which an activity cannot reproduce"
        elif form == "invoke" and "Payload" in args and not isinstance(args["Payload"], dict):
            problem = "passes a literal non-object Payload; an activity input must be a JSON object built with Parameters"
        elif form == "invoke" and isinstance(args.get("Payload.$"), str) and args["Payload.$"].startswith("$$"):
            problem = "builds the Payload from the context object ($$); move that into Parameters by hand"
        elif form == "invoke" and "Payload.$" in args and state.get("InputPath") not in (None, "$"):
            problem = "has both an InputPath and a Payload.$ path; combine them into one InputPath by hand"
        out.append({"path": path, "form": form, "problem": problem})
    return out


def _rewrite_selector(selector: dict[str, Any], notes: list[str], path: str) -> dict[str, Any]:
    """A ResultSelector written for the lambda:invoke wrapper, made to read the activity's bare result."""
    out: dict[str, Any] = {}
    for k, v in selector.items():
        if k.endswith(".$") and isinstance(v, str) and v.startswith("$.Payload"):
            out[k] = "$" + v[len("$.Payload"):]
        elif k.endswith(".$") and v == "$.StatusCode":
            out[k[:-2]] = 200
        elif k.endswith(".$") and isinstance(v, str) and v.startswith("$.") and not v.startswith("$.Payload"):
            out[k] = v
            notes.append(f"{path}: ResultSelector reads {v}, which the activity result does not have; check it by hand")
        elif isinstance(v, dict):
            out[k] = _rewrite_selector(v, notes, path)
        else:
            out[k] = v
    return out


def rewrite(definition: dict[str, Any], name: str, refs: list[str] | tuple[str, ...], activity_arn: str,
            timeout_s: int) -> tuple[dict[str, Any], list[str], list[str]]:
    """(new definition, rewritten state paths, notes). States with a problem are left calling the Lambda."""
    new = copy.deepcopy(definition)
    done: list[str] = []
    notes: list[str] = []
    for task in find_tasks(definition, name, refs):
        path = task["path"]
        if task["problem"]:
            notes.append(f"{path}: not rewritten, it {task['problem']}")
            continue
        state = _state_at(new, path)
        if task["form"] == "invoke":
            params = state.pop("Parameters", None) or {}
            if isinstance(params.get("Payload"), dict):
                state["Parameters"] = params["Payload"]
            elif isinstance(params.get("Payload.$"), str) and params["Payload.$"] != "$":
                state["InputPath"] = params["Payload.$"]
            elif "Payload" not in params and "Payload.$" not in params:
                state["Parameters"] = {}
                notes.append(f"{path}: the lambda:invoke call had no Payload; the activity gets an empty object")
            state["ResultSelector"] = (_rewrite_selector(state["ResultSelector"], notes, path)
                                       if isinstance(state.get("ResultSelector"), dict) else dict(INVOKE_WRAPPER))
        state["Resource"] = activity_arn
        if "TimeoutSeconds" not in state and "TimeoutSecondsPath" not in state:
            state["TimeoutSeconds"] = timeout_s + TIMEOUT_MARGIN_S
        done.append(path)
    return new, done, notes


def machine_name(arn_or_name: str) -> str:
    return arn_or_name.split(":stateMachine:", 1)[1].split(":")[0] if ":stateMachine:" in arn_or_name else arn_or_name
