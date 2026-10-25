"""Turn the generated manifests into a Helm chart whose default values render the same objects."""

from __future__ import annotations

import copy
import re
from typing import Any

import yaml

from .generate import PAUSED, dump_yaml
from .model import BRIDGED, HTTP, INVOKE, SCHEDULE, SQS, STREAMS
from .plan import Plan


class _Tokens:
    """Placeholders put into the manifests before dumping, replaced by template expressions after."""

    def __init__(self) -> None:
        self.inline: dict[str, str] = {}
        self.blocks: dict[str, str] = {}

    def value(self, expr: str) -> str:
        token = f"__L2E{len(self.inline)}__"
        self.inline[token] = "{{ " + expr + " }}"
        return token

    def block(self, expr: str) -> str:
        token = f"__L2EB{len(self.blocks)}__"
        self.blocks[token] = expr
        return token

    def render(self, text: str) -> str:
        def block(m: re.Match[str]) -> str:
            indent = len(m.group(1)) + 2
            return f"{m.group(1)}{m.group(2)}:\n{{{{- {self.blocks[m.group(3)]} | nindent {indent} }}}}"

        text = re.sub(r"^( *)([\w.-]+): (__L2EB\d+__)$", block, text, flags=re.M)
        for token, expr in self.inline.items():
            text = text.replace(f"'{token}'", expr).replace(token, expr)
        return text


def _wl(name: str) -> str:
    return f'(index .Values.workloads "{name}")'


def chart(plan: Plan, files: dict[str, list[dict[str, Any]]], iam_mode: str) -> dict[str, str]:
    """Chart files (path -> text) for one function."""
    tok = _Tokens()
    files = copy.deepcopy(files)
    image_repo, _, image_tag = plan.image.rpartition(":")
    values: dict[str, Any] = {
        "image": {"repository": image_repo, "tag": image_tag},
        "roleArn": plan.spec.role_arn or "REPLACE_ME",
        "resources": {"cpu": f"{plan.cpu_millicores}m", "memory": f"{plan.memory_mb}Mi"},
        "concurrency": plan.concurrency,
        "workloads": {},
    }
    kinds = {w.name: w.kind for w in plan.workloads}

    for docs in files.values():
        for doc in docs:
            kind, name = doc["kind"], doc["metadata"]["name"]
            wkind = kinds.get(name)
            if kind == "ConfigMap":
                values["env"] = doc["data"]
                doc["data"] = tok.block("toYaml .Values.env")
            elif kind == "Secret":
                values["secrets"] = doc["stringData"]
                doc["stringData"] = tok.block("toYaml .Values.secrets")
            elif kind == "ServiceAccount" and iam_mode == "irsa":
                doc["metadata"]["annotations"]["eks.amazonaws.com/role-arn"] = tok.value(".Values.roleArn")
            elif kind == "TriggerAuthentication" and "roleArn" in doc["spec"]["podIdentity"]:
                doc["spec"]["podIdentity"]["roleArn"] = tok.value(".Values.roleArn")
            elif kind == "HorizontalPodAutoscaler":
                wv = values["workloads"].setdefault(name, {})
                wv.update(minReplicas=doc["spec"]["minReplicas"], maxReplicas=doc["spec"]["maxReplicas"])
                doc["spec"]["minReplicas"] = tok.value(f"{_wl(name)}.minReplicas")
                doc["spec"]["maxReplicas"] = tok.value(f"{_wl(name)}.maxReplicas")
            elif kind == "Ingress":
                ann = doc["metadata"]["annotations"]
                values["workloads"].setdefault(name, {})["ingressScheme"] = ann["alb.ingress.kubernetes.io/scheme"]
                ann["alb.ingress.kubernetes.io/scheme"] = tok.value(f"{_wl(name)}.ingressScheme")
            elif kind == "ScaledObject":
                wv = values["workloads"].setdefault(name, {})
                wv.update(maxReplicas=doc["spec"]["maxReplicaCount"], paused=PAUSED in (doc["metadata"].get("annotations") or {}))
                doc["spec"]["maxReplicaCount"] = tok.value(f"{_wl(name)}.maxReplicas")
                doc["metadata"]["annotations"] = tok.value(
                    f'ternary (dict "{PAUSED}" "0") (dict) {_wl(name)}.paused | toJson')
            elif kind == "CronJob":
                wv = values["workloads"].setdefault(name, {})
                wv.update(schedule=doc["spec"]["schedule"], suspend=doc["spec"]["suspend"])
                doc["spec"]["schedule"] = tok.value(f"{_wl(name)}.schedule | quote")
                doc["spec"]["suspend"] = tok.value(f"{_wl(name)}.suspend")

            if kind == "Deployment" and wkind in (HTTP, INVOKE):
                doc["spec"]["replicas"] = tok.value(f"{_wl(name)}.minReplicas")
            elif kind == "Deployment" and wkind in (SQS, *BRIDGED, *STREAMS):
                doc["spec"]["replicas"] = tok.value(f"ternary 0 1 {_wl(name)}.paused")
            pod = (doc["spec"]["template"]["spec"] if kind == "Deployment" else
                   doc["spec"]["jobTemplate"]["spec"]["template"]["spec"] if kind == "CronJob" else None)
            if pod and wkind in (HTTP, INVOKE, SQS, SCHEDULE, *BRIDGED, *STREAMS):
                for c in pod["containers"]:
                    c["image"] = tok.value('printf "%s:%s" .Values.image.repository .Values.image.tag')
                    c["resources"] = {"requests": {"cpu": tok.value(".Values.resources.cpu"), "memory": tok.value(".Values.resources.memory")},
                                      "limits": {"memory": tok.value(".Values.resources.memory")}}
                    for e in c["env"]:
                        if e["name"] == "L2E_CONCURRENCY":
                            e["value"] = tok.value(".Values.concurrency | quote")

    out = {
        "Chart.yaml": yaml.safe_dump({
            "apiVersion": "v2", "name": plan.k8s_name, "version": "0.1.0", "appVersion": image_tag or "latest",
            "description": f"{plan.spec.name}, migrated from AWS Lambda by lambda2eks",
        }, sort_keys=False),
        "values.yaml": "# Defaults render the same objects as k8s/. Secret values are placeholders.\n"
                       + yaml.safe_dump(values, sort_keys=False),
    }
    for name, docs in files.items():
        out[f"templates/{name}"] = tok.render(dump_yaml(docs))
    return out
