#!/usr/bin/env python3
"""End-to-end run of `lambda2eks ddb2pg` on Rancher Desktop.

LocalStack stands in for DynamoDB, a throwaway PostgreSQL 17 runs beside it. The run:
  1. seeds a single-table design (users, orders, a GSI, a stream, one attribute of mixed type)
  2. plan -> apply -> backfill
  3. changes the table after the backfill (insert, update, delete)
  4. sync until idle, so the stream carries those changes across
  5. verify (counts and a sample), plus SQL checks of each change and the entity views

Rancher Desktop is started if it is not running and shut down at the end (--keep-running to leave it).

    .venv/bin/python examples/ddb2pg/run.py
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import random
import subprocess
import sys
import urllib.request
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUT = HERE / "out"
_spec = importlib.util.spec_from_file_location("rancher_run", HERE.parent / "test-lambdas" / "rancher" / "run.py")
rr = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rr)

TABLE = "l2e-ddb2pg-app"
PG_URL = "postgresql://postgres:ddb2pg@localhost:15432/app"
CLI = [sys.executable, "-m", "lambda2eks.cli", "ddb2pg"]
DDB = ["--region", rr.REGION, "--endpoint-url", rr.ENDPOINT_LOCAL]


def lambda2eks(*args: str) -> None:
    rr.log("lambda2eks ddb2pg " + " ".join(args))
    env = {"AWS_ACCESS_KEY_ID": "test", "AWS_SECRET_ACCESS_KEY": "test", "PATH": "/usr/bin:/bin"}
    subprocess.run([*CLI, *args], check=True, env=env, cwd=HERE.parent.parent)


def platform() -> None:
    rr.start_rancher()
    rr.kubectl("apply", "-f", str(HERE.parent / "test-lambdas" / "rancher" / "platform.yaml"))
    # A namespace still terminating from the last run refuses new objects.
    rr.kubectl("wait", "--for=delete", "namespace/ddb2pg", "--timeout=180s", check=False, capture=True)
    rr.kubectl("apply", "-f", str(HERE / "postgres.yaml"))
    rr.kubectl("-n", "localstack", "rollout", "status", "deploy/localstack", "--timeout=300s")
    rr.kubectl("-n", "ddb2pg", "rollout", "status", "deploy/postgres", "--timeout=300s")
    rr.wait_until("LocalStack", lambda: json.load(urllib.request.urlopen(f"{rr.ENDPOINT_LOCAL}/_localstack/health", timeout=3)), 120)
    import psycopg
    rr.wait_until("PostgreSQL on localhost:15432", lambda: psycopg.connect(PG_URL, connect_timeout=3).close() or True, 120)


def seed(ddb) -> int:
    try:
        ddb.delete_table(TableName=TABLE)
        ddb.get_waiter("table_not_exists").wait(TableName=TABLE)
    except ddb.exceptions.ResourceNotFoundException:
        pass
    ddb.create_table(
        TableName=TABLE, BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[{"AttributeName": a, "AttributeType": t} for a, t in
                              [("PK", "S"), ("SK", "S"), ("GSI1PK", "S"), ("GSI1SK", "S")]],
        KeySchema=[{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
        GlobalSecondaryIndexes=[{"IndexName": "GSI1", "Projection": {"ProjectionType": "ALL"},
                                 "KeySchema": [{"AttributeName": "GSI1PK", "KeyType": "HASH"},
                                               {"AttributeName": "GSI1SK", "KeyType": "RANGE"}]}],
        StreamSpecification={"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"})
    ddb.get_waiter("table_exists").wait(TableName=TABLE)
    rnd, n = random.Random(7), 0
    for u in range(60):
        ddb.put_item(TableName=TABLE, Item={
            "PK": {"S": f"USER#{u}"}, "SK": {"S": "PROFILE"}, "name": {"S": f"user {u}"},
            "email": {"S": f"u{u}@example.test"}, "tags": {"SS": rnd.sample(["vip", "beta", "eu", "za"], 2)},
            # Mixed type on purpose: inference must leave it in doc only.
            "tier": {"N": str(u % 3)} if u % 2 else {"S": ["gold", "silver"][u % 2]}})
        n += 1
        for o in range(rnd.randint(1, 6)):
            ddb.put_item(TableName=TABLE, Item={
                "PK": {"S": f"USER#{u}"}, "SK": {"S": f"ORDER#{o:04d}"},
                "GSI1PK": {"S": f"STATUS#{rnd.choice(['open', 'paid', 'shipped'])}"}, "GSI1SK": {"S": f"2026-11-{o + 1:02d}"},
                "total": {"N": f"{rnd.uniform(5, 500):.2f}"}, "paid": {"BOOL": rnd.random() > 0.5},
                "lines": {"L": [{"M": {"sku": {"S": f"SKU{rnd.randint(1, 99)}"}, "qty": {"N": str(rnd.randint(1, 4))}}}]}})
            n += 1
    return n


def mutate(ddb) -> None:
    ddb.put_item(TableName=TABLE, Item={"PK": {"S": "USER#999"}, "SK": {"S": "PROFILE"}, "name": {"S": "late arrival"},
                                        "email": {"S": "late@example.test"}, "tags": {"SS": ["new"]}})
    ddb.update_item(TableName=TABLE, Key={"PK": {"S": "USER#1"}, "SK": {"S": "PROFILE"}},
                    UpdateExpression="SET #n = :n", ExpressionAttributeNames={"#n": "name"},
                    ExpressionAttributeValues={":n": {"S": "renamed after backfill"}})
    ddb.delete_item(TableName=TABLE, Key={"PK": {"S": "USER#2"}, "SK": {"S": "PROFILE"}})


def checks(ddb_count: int) -> list[tuple[str, bool, str]]:
    import psycopg
    out = []
    with psycopg.connect(PG_URL) as conn, conn.cursor() as cur:
        def one(sql: str):
            cur.execute(sql)
            return cur.fetchone()
        t = "l2e_ddb2pg_app"
        n = one(f"SELECT count(*) FROM {t}")[0]
        out.append(("row count matches DynamoDB", n == ddb_count, f"{n} rows, DynamoDB {ddb_count}"))
        r = one(f"SELECT name FROM {t} WHERE pk = 'USER#999' AND sk = 'PROFILE'")
        out.append(("insert after backfill arrived by stream", r == ("late arrival",), repr(r)))
        r = one(f"SELECT name, doc->>'name' FROM {t} WHERE pk = 'USER#1' AND sk = 'PROFILE'")
        out.append(("update after backfill arrived by stream", r == ("renamed after backfill",) * 2, repr(r)))
        r = one(f"SELECT count(*) FROM {t} WHERE pk = 'USER#2' AND sk = 'PROFILE'")
        out.append(("delete after backfill arrived by stream", r == (0,), repr(r)))
        cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (t,))
        cols = dict(cur.fetchall())
        out.append(("typed columns inferred", cols.get("total") == "numeric" and cols.get("paid") == "boolean"
                    and cols.get("tags") == "ARRAY" and cols.get("lines") == "jsonb", json.dumps(cols, sort_keys=True)))
        out.append(("mixed-type attribute kept in doc only", "tier" not in cols
                    and one(f"SELECT count(*) FROM {t} WHERE doc ? 'tier'")[0] > 0, "tier"))
        p, o = one(f"SELECT (SELECT count(*) FROM {t}_profile), (SELECT count(*) FROM {t}_order)")
        out.append(("entity views from the sort key (profile, order)", p == 60 and p + o == n, f"{p} profiles, {o} orders"))
        r = one(f"SELECT count(*) FROM {t} WHERE gsi1_pk = 'STATUS#paid'")[0]
        out.append(("GSI1 columns queryable", r > 0, f"{r} paid orders"))
    return out


def run_all(args: argparse.Namespace) -> int:
    platform()
    ddb = rr.aws("dynamodb")
    n = seed(ddb)
    rr.log(f"seeded {n} items into {TABLE}")
    schema = str(OUT / "l2e_ddb2pg_app.schema.json")
    lambda2eks("plan", TABLE, *DDB, "--out", str(OUT))
    lambda2eks("apply", *DDB, "--database-url", PG_URL, "--schema", schema)
    lambda2eks("backfill", *DDB, "--database-url", PG_URL, "--schema", schema)
    mutate(ddb)
    lambda2eks("sync", *DDB, "--database-url", PG_URL, "--schema", schema, "--until-idle", "10")
    lambda2eks("verify", *DDB, "--database-url", PG_URL, "--schema", schema)
    ddb_count = ddb.scan(TableName=TABLE, Select="COUNT")["Count"]
    results = checks(ddb_count)
    lines = ["# ddb2pg end-to-end", "", "| check | result | detail |", "|---|---|---|"]
    lines += [f"| {name} | {'PASS' if ok else 'FAIL'} | `{detail[:120]}` |" for name, ok, detail in results]
    (OUT / "REPORT.md").write_text("\n".join(lines) + "\n")
    for name, ok, detail in results:
        print(f"{'PASS' if ok else 'FAIL'}  {name}: {detail}")
    passed = sum(ok for _, ok, _ in results)
    print(f"\n{passed}/{len(results)} checks passed; report: {OUT / 'REPORT.md'}")
    return 0 if passed == len(results) else 1


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--keep-running", action="store_true", help="leave Rancher Desktop and the containers up")
    args = p.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    try:
        return run_all(args)
    finally:
        if not args.keep_running:
            rr.kubectl("delete", "namespace", "ddb2pg", "--ignore-not-found", "--timeout=120s", check=False)
            rr.stop_rancher()


if __name__ == "__main__":
    sys.exit(main())
