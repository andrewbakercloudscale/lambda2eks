"""lambda2eks command line."""

from __future__ import annotations

import argparse
import concurrent.futures
import sys
from pathlib import Path

from . import ai
from .generate import write_plan
from .model import FunctionSpec
from .plan import Plan, baseline_plan


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        prog="lambda2eks",
        description="Turn AWS Lambda functions into EKS workloads. An AI review (Claude, or an open model on Together AI) "
                    "reads each function's code and config to set concurrency, resources, scaling and secrets, and reports "
                    "what must change. A cut-over runbook moves each trigger from the Lambda to EKS with a rollback.",
    )
    sub = p.add_subparsers(dest="source", required=True)
    t = sub.add_parser("from-template", help="read functions from a SAM or CloudFormation template")
    t.add_argument("template", type=Path)
    t.add_argument("--function", action="append", dest="functions", metavar="NAME",
                   help="only this function (logical id or FunctionName); repeatable")
    a = sub.add_parser("from-aws", help="read deployed functions from AWS (read-only) and download their code")
    a.add_argument("functions", nargs="+", metavar="NAME")
    a.add_argument("--profile")

    from . import ddb2pg
    ddb2pg.add_parser(sub)

    for s in (t, a):
        s.add_argument("--out", type=Path, default=Path("out"), help="output directory (default: out)")
        s.add_argument("--region", help="AWS region for the workloads (default: the function's region)")
        s.add_argument("--registry", default="ACCOUNT_ID.dkr.ecr.REGION.amazonaws.com",
                       help="container registry prefix for the images")
        s.add_argument("--namespace", default="lambda-migration", help="Kubernetes namespace (default: lambda-migration)")
        s.add_argument("--iam", choices=["pod-identity", "irsa"], default="pod-identity",
                       help="how pods get the execution role (default: pod-identity)")
        s.add_argument("--handler", help="handler for container-image functions")
        s.add_argument("--runtime", help="runtime for container-image functions, e.g. python3.12 or nodejs20.x")
        s.add_argument("--format", choices=["kustomize", "helm", "both"], default="kustomize",
                       help="manifests as a kustomize directory (k8s/), a Helm chart (helm/), or both (default: kustomize)")
        s.add_argument("--certificate-arn", help="ACM certificate for HTTPS on the ALB (needed for the weighted DNS cut-over)")
        s.add_argument("--live", action="store_true",
                       help="start queue/stream workers and CronJobs active instead of paused for the cut-over "
                            "(only for functions whose Lambda triggers are already off)")
        s.add_argument("--no-ai", action="store_true", help="skip the AI review and use rule-based settings only")
        s.add_argument("--provider", choices=["auto", "anthropic", "together"], default="auto",
                       help="AI provider: auto uses Anthropic if ANTHROPIC_API_KEY is set, else Together if TOGETHER_API_KEY is set")
        s.add_argument("--model", help=f"model for the review (default: {ai.DEFAULT_MODELS['anthropic']} on Anthropic, "
                                       f"{ai.DEFAULT_MODELS['together']} on Together)")
        s.add_argument("--effort", default="high", choices=["low", "medium", "high", "xhigh", "max"],
                       help="review effort, Claude only (default: high)")
        s.add_argument("--jobs", type=int, default=4, help="functions reviewed in parallel (default: 4)")
    return p.parse_args(argv)


def load(args: argparse.Namespace) -> list[FunctionSpec]:
    if args.source == "from-template":
        from .loaders.template import load_template
        specs = load_template(args.template, args.functions)
        if args.functions and not specs:
            raise SystemExit(f"no function named {', '.join(args.functions)} in {args.template}")
    else:
        from .loaders.aws import load_from_aws
        specs = load_from_aws(args.functions, args.out, region=args.region, profile=args.profile)
    for s in specs:
        if s.image_uri:
            s.handler = args.handler or s.handler
            s.runtime = args.runtime or s.runtime
    return specs


def review_one(plan: Plan, args: argparse.Namespace) -> tuple[dict | None, str | None]:
    if args.no_ai or not plan.supported:
        return None, None
    try:
        result = ai.review(plan, model=args.model, effort=args.effort, provider=args.provider)
    except ai.AIReviewError as e:
        return None, str(e)
    ai.apply_review(plan, result)
    return result, None


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.source == "ddb2pg":
        return args.func(args)
    specs = load(args)
    if not specs:
        print("no Lambda functions found", file=sys.stderr)
        return 1
    plans = [baseline_plan(s, args.registry) for s in specs]

    if not args.no_ai:
        try:
            args.provider = ai.resolve_provider(args.provider)
        except ai.AIReviewError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        args.model = args.model or ai.DEFAULT_MODELS[args.provider]
        print(f"Reviewing {len(plans)} function(s) with {args.provider}:{args.model}...", file=sys.stderr)
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, args.jobs)) as pool:
        outcomes = list(pool.map(lambda p: review_one(p, args), plans))

    failed = 0
    rows = []
    for plan, (result, error) in zip(plans, outcomes):
        if error:
            failed += 1
            plan.ai_notes.insert(0, f"AI REVIEW FAILED: {error}. The settings are rule-based; re-run, or pass --no-ai to accept that.")
        write_plan(plan, args.out / plan.k8s_name, args.namespace, args.region, args.iam, result,
                   fmt=args.format, live=args.live, certificate_arn=args.certificate_arn)
        blockers = sum(f.get("severity") == "blocker" for f in plan.findings)
        status = ("not generated: " + plan.unsupported_reason if not plan.supported else
                  "AI review failed" if error else
                  plan.ai_summary.split("]")[0].strip("[") if plan.ai_used else "rule-based only")
        rows.append((plan, status, blockers, error))
        print(f"  {plan.spec.name:40} {status}" + (f", {blockers} blocker(s)" if blockers else "") +
              (f" — {error}" if error else ""), file=sys.stderr)

    summary = ["# lambda2eks summary", "", "| Function | Status | Workloads | Blockers | Report |", "|---|---|---|---|---|"]
    for plan, status, blockers, _ in rows:
        kinds = ", ".join(w.kind for w in plan.workloads) if plan.supported else "—"
        summary.append(f"| {plan.spec.name} | {status} | {kinds} | {blockers} | [{plan.k8s_name}/MIGRATION.md]({plan.k8s_name}/MIGRATION.md) |")
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "SUMMARY.md").write_text("\n".join(summary) + "\n")
    print(f"Wrote {args.out}/SUMMARY.md", file=sys.stderr)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
