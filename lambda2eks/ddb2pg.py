"""DynamoDB to PostgreSQL: schema from the table and a sample of items, backfill, stream sync, verify, code patches.

The order that keeps no change behind:

    plan      read the table (keys, GSIs, LSIs, TTL, stream) and sample items -> schema.json + schema.sql
    apply     create the PostgreSQL table, indexes and per-entity views
    backfill  parallel Scan -> upsert. Must finish inside the stream's 24 hour retention.
    sync      read the DynamoDB stream from TRIM_HORIZON (or the saved checkpoint) and apply every change.
              Replaying changes older than the backfill is harmless: each record is a full image applied
              in order, so every key ends at its latest state. Run it until the cut-over.
    verify    item count and a random sample of items, compared field by field
    rewrite   AI patches that replace the DynamoDB calls in a function's code with SQL

Each row keeps the whole item in `doc` (jsonb), so nothing is lost, and also has a typed column for
every key attribute and every attribute the sample showed with one consistent type. An item whose
attribute has another type writes NULL to that column and is counted; `doc` still has the value.
"""

from __future__ import annotations

import argparse
import base64
import json
import random
import re
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any

PG_TYPES = {"S": "text", "N": "numeric", "B": "bytea", "BOOL": "boolean", "M": "jsonb", "L": "jsonb",
            "SS": "text[]", "NS": "numeric[]", "BS": "bytea[]"}
ENTITY_PREFIX = re.compile(r"^([A-Za-z][A-Za-z0-9_]*)#")
ENTITY_WORD = re.compile(r"^([A-Z][A-Z0-9_]*)$")  # a whole sort key like PROFILE or META
SYSTEM_COLUMNS = ("doc", "_synced_at")


# ------------------------------------------------------------------ conversions

def from_ddb(av: dict[str, Any]) -> Any:
    """A DynamoDB AttributeValue -> a plain Python value (Decimal for numbers, bytes for binary)."""
    (t, v), = av.items()
    if t == "S":
        return v
    if t == "N":
        return Decimal(v)
    if t == "B":
        return v if isinstance(v, bytes) else base64.b64decode(v)
    if t == "BOOL":
        return bool(v)
    if t == "NULL":
        return None
    if t == "M":
        return {k: from_ddb(x) for k, x in v.items()}
    if t == "L":
        return [from_ddb(x) for x in v]
    if t == "SS":
        return sorted(v)
    if t == "NS":
        return sorted(Decimal(x) for x in v)
    if t == "BS":
        return sorted(x if isinstance(x, bytes) else base64.b64decode(x) for x in v)
    raise ValueError(f"unknown DynamoDB type {t}")


def ddb_type(av: dict[str, Any]) -> str:
    return next(iter(av))


class _JSON(json.JSONEncoder):
    def default(self, o: Any) -> Any:
        if isinstance(o, Decimal):
            return int(o) if o == o.to_integral_value() and abs(o) < 2**63 else float(o) if o.is_finite() else str(o)
        if isinstance(o, bytes):
            return base64.b64encode(o).decode()
        return super().default(o)


def to_json(item: dict[str, Any]) -> str:
    """A DynamoDB item -> the JSON stored in `doc`. Binary becomes base64, sets become sorted arrays."""
    return json.dumps({k: from_ddb(v) for k, v in item.items()}, cls=_JSON, sort_keys=True, separators=(",", ":"))


def snake(name: str) -> str:
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name)
    s = re.sub(r"[^A-Za-z0-9]+", "_", s).strip("_").lower() or "col"
    return s if not s[0].isdigit() else f"c_{s}"


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


# ------------------------------------------------------------------ schema

@dataclass
class Column:
    attribute: str       # DynamoDB attribute name
    name: str            # PostgreSQL column name
    ddb_type: str        # S, N, B, BOOL, M, L, SS, NS, BS
    pg_type: str
    key: bool = False
    seen_in: float = 1.0  # share of sampled items that had it


@dataclass
class Schema:
    table: str                       # DynamoDB table
    pg_table: str
    partition_key: str               # attribute names
    sort_key: str | None
    columns: list[Column]
    indexes: list[dict[str, Any]] = field(default_factory=list)  # {"name", "kind": GSI|LSI, "columns": [...]}
    ttl_attribute: str | None = None
    stream_view: str | None = None   # NEW_IMAGE, NEW_AND_OLD_IMAGES, KEYS_ONLY, OLD_IMAGE or None
    entities: dict[str, int] = field(default_factory=dict)  # single-table design: key prefix -> sampled count
    entity_key: str | None = None    # the key attribute the entity prefixes come from (partition key if None)
    sampled: int = 0
    item_count_estimate: int = 0
    notes: list[str] = field(default_factory=list)

    def column(self, attribute: str) -> Column | None:
        return next((c for c in self.columns if c.attribute == attribute), None)

    @property
    def key_columns(self) -> list[Column]:
        return [c for c in (self.column(self.partition_key), self.column(self.sort_key) if self.sort_key else None) if c]

    def to_json(self) -> str:
        return json.dumps(asdict(self), indent=2)

    @classmethod
    def from_json(cls, text: str) -> Schema:
        d = json.loads(text)
        d["columns"] = [Column(**c) for c in d["columns"]]
        return cls(**d)


def infer_schema(desc: dict[str, Any], items: list[dict[str, Any]], ttl_attribute: str | None = None,
                 min_share: float = 0.0) -> Schema:
    """desc is DescribeTable's Table; items are sampled raw DynamoDB items."""
    attr_types = {a["AttributeName"]: a["AttributeType"] for a in desc.get("AttributeDefinitions", [])}
    keys = {k["KeyType"]: k["AttributeName"] for k in desc["KeySchema"]}
    pk, sk = keys["HASH"], keys.get("RANGE")
    notes: list[str] = []
    seen: dict[str, Counter] = {}
    for item in items:
        for k, v in item.items():
            seen.setdefault(k, Counter())[ddb_type(v)] += 1
    taken: set[str] = set(SYSTEM_COLUMNS)

    def colname(attr: str) -> str:
        base = snake(attr)
        n, i = base, 2
        while n in taken:
            n, i = f"{base}_{i}", i + 1
        taken.add(n)
        return n

    columns: list[Column] = []
    key_attrs = [pk] + ([sk] if sk else [])
    index_attrs = [k["AttributeName"] for ix in desc.get("GlobalSecondaryIndexes", []) + desc.get("LocalSecondaryIndexes", [])
                   for k in ix["KeySchema"]]
    for attr in dict.fromkeys(key_attrs + index_attrs):
        t = attr_types[attr]
        columns.append(Column(attr, colname(attr), t, PG_TYPES[t], key=attr in key_attrs,
                              seen_in=round(sum(seen.get(attr, Counter()).values()) / max(1, len(items)), 3)))
    for attr in sorted(seen):
        if any(c.attribute == attr for c in columns):
            continue
        types = seen[attr] - Counter({"NULL": seen[attr]["NULL"]})
        share = sum(seen[attr].values()) / max(1, len(items))
        if len(types) != 1:
            notes.append(f"attribute {attr!r} has types {dict(seen[attr])} in the sample; it stays in doc only")
            continue
        if share < min_share:
            notes.append(f"attribute {attr!r} is in only {share:.0%} of sampled items; it stays in doc only")
            continue
        t = next(iter(types))
        columns.append(Column(attr, colname(attr), t, PG_TYPES[t], seen_in=round(share, 3)))

    indexes = []
    for kind, key in (("GSI", "GlobalSecondaryIndexes"), ("LSI", "LocalSecondaryIndexes")):
        for ix in desc.get(key, []):
            attrs = [k["AttributeName"] for k in sorted(ix["KeySchema"], key=lambda k: k["KeyType"] != "HASH")]
            indexes.append({"name": ix["IndexName"], "kind": kind, "attributes": attrs,
                            "projection": ix.get("Projection", {}).get("ProjectionType", "ALL")})

    # Single-table design: entity types show as key prefixes (USER#1, ORDER#9). Users and their orders
    # often share a partition (PK=USER#1, SK=PROFILE / ORDER#0001), so the sort key is tried too.
    entities, entity_key = Counter(), None
    for key, patterns in ((pk, (ENTITY_PREFIX,)), (sk, (ENTITY_PREFIX, ENTITY_WORD))):
        if not key:
            continue
        found = Counter()
        for item in items:
            v = item.get(key, {}).get("S") or ""
            m = next((m for p in patterns if (m := p.match(v))), None)
            if m:
                found[m.group(1)] += 1
        if len(found) > 1 and sum(found.values()) >= 0.8 * len(items):
            entities, entity_key = found, key
            notes.append(f"single-table design: {len(found)} entity prefixes in {key}; a view is created per entity")
            break
    stream = desc.get("StreamSpecification") or {}
    view = stream.get("StreamViewType") if stream.get("StreamEnabled") else None
    if not view:
        notes.append("no DynamoDB stream: enable one (NEW_IMAGE) before the backfill, or `sync` has nothing to read")
    elif view == "KEYS_ONLY":
        notes.append("stream is KEYS_ONLY: sync reads each changed item back with a consistent GetItem")
    elif view == "OLD_IMAGE":
        notes.append("stream is OLD_IMAGE: sync reads each changed item back with a consistent GetItem")
    if ttl_attribute:
        notes.append(f"TTL on {ttl_attribute!r}: DynamoDB deletes expired items itself; on PostgreSQL schedule "
                     f"`DELETE ... WHERE {snake(ttl_attribute)} < extract(epoch from now())` (pg_cron, or a CronJob)")
    if any(ix["projection"] != "ALL" for ix in indexes):
        notes.append("some indexes project only some attributes; PostgreSQL indexes always reach the whole row")
    return Schema(table=desc["TableName"], pg_table=snake(desc["TableName"]), partition_key=pk, sort_key=sk,
                  columns=columns, indexes=indexes, ttl_attribute=ttl_attribute, stream_view=view,
                  entities=dict(entities.most_common()), entity_key=entity_key, sampled=len(items),
                  item_count_estimate=desc.get("ItemCount", 0), notes=notes)


def ddl(schema: Schema) -> str:
    t = ident(schema.pg_table)
    cols = [f"  {ident(c.name)} {c.pg_type}{' NOT NULL' if c.key else ''}" for c in schema.columns]
    cols += ["  doc jsonb NOT NULL", "  _synced_at timestamptz NOT NULL DEFAULT now()"]
    pk = ", ".join(ident(c.name) for c in schema.key_columns)
    out = [f"-- Generated by lambda2eks ddb2pg from DynamoDB table {schema.table} ({schema.sampled} items sampled).",
           f"-- doc holds every item whole; the columns are typed copies for querying.",
           f"CREATE TABLE IF NOT EXISTS {t} (", ",\n".join(cols + [f"  PRIMARY KEY ({pk})"]), ");"]
    for ix in schema.indexes:
        names = [schema.column(a).name for a in ix["attributes"]]
        out.append(f"CREATE INDEX IF NOT EXISTS {ident(snake(schema.pg_table + '_' + ix['name']))} ON {t} "
                   f"({', '.join(ident(n) for n in names)});  -- {ix['kind']} {ix['name']}")
    if schema.ttl_attribute and schema.column(schema.ttl_attribute):
        out.append(f"CREATE INDEX IF NOT EXISTS {ident(schema.pg_table + '_ttl')} ON {t} "
                   f"({ident(schema.column(schema.ttl_attribute).name)});  -- TTL sweeps")
    ecol = ident(schema.column(schema.entity_key or schema.partition_key).name)
    for entity in schema.entities:
        lit = entity.replace("'", "''")
        out.append(f"CREATE OR REPLACE VIEW {ident(schema.pg_table + '_' + snake(entity))} AS "
                   f"SELECT * FROM {t} WHERE {ecol} = '{lit}' OR {ecol} LIKE '{lit}#%';")
    out.append("CREATE TABLE IF NOT EXISTS ddb2pg_checkpoints (pg_table text NOT NULL, shard_id text NOT NULL, "
               "sequence_number text, finished boolean NOT NULL DEFAULT false, updated_at timestamptz NOT NULL DEFAULT now(), "
               "PRIMARY KEY (pg_table, shard_id));")
    return "\n".join(out) + "\n"


# ------------------------------------------------------------------ rows

class RowWriter:
    """Upserts and deletes against one PostgreSQL table, using the schema's column mapping."""

    def __init__(self, conn: Any, schema: Schema):
        self.conn, self.schema = conn, schema
        self.mismatches: Counter = Counter()
        self._lock = threading.Lock()
        names = [c.name for c in schema.columns] + ["doc"]
        keys = [c.name for c in schema.key_columns]
        updates = [n for n in names if n not in keys] + ["_synced_at"]
        self.upsert_sql = (
            f"INSERT INTO {ident(schema.pg_table)} ({', '.join(ident(n) for n in names)}) "
            f"VALUES ({', '.join(['%s'] * (len(names) - 1) + ['%s::jsonb'])}) "
            f"ON CONFLICT ({', '.join(ident(k) for k in keys)}) DO UPDATE SET "
            + ", ".join(f"{ident(n)} = now()" if n == "_synced_at" else f"{ident(n)} = EXCLUDED.{ident(n)}" for n in updates))
        self.delete_sql = (f"DELETE FROM {ident(schema.pg_table)} WHERE "
                           + " AND ".join(f"{ident(k)} = %s" for k in keys))

    def row(self, item: dict[str, Any]) -> list[Any]:
        values = []
        for c in self.schema.columns:
            av = item.get(c.attribute)
            if av is None or ddb_type(av) == "NULL":
                values.append(None)
            elif ddb_type(av) != c.ddb_type:
                with self._lock:
                    self.mismatches[c.attribute] += 1
                values.append(None)
            else:
                v = from_ddb(av)
                values.append(json.dumps(v, cls=_JSON) if c.pg_type == "jsonb" else v)
        return values + [to_json(item)]

    def key(self, keys: dict[str, Any]) -> list[Any]:
        return [from_ddb(keys[c.attribute]) for c in self.schema.key_columns]

    def upsert(self, items: list[dict[str, Any]]) -> None:
        with self.conn.cursor() as cur:
            cur.executemany(self.upsert_sql, [self.row(i) for i in items])

    def delete(self, keys: dict[str, Any]) -> None:
        with self.conn.cursor() as cur:
            cur.execute(self.delete_sql, self.key(keys))


# ------------------------------------------------------------------ backfill

def backfill(ddb: Any, connect: Any, schema: Schema, segments: int = 8, page_size: int = 500,
             progress: Any = None) -> tuple[int, Counter]:
    """Parallel Scan (one PostgreSQL connection per segment). Returns (items written, type mismatches)."""
    total = 0
    lock = threading.Lock()
    mismatches: Counter = Counter()

    def segment(n: int) -> None:
        nonlocal total
        conn = connect()
        writer = RowWriter(conn, schema)
        kwargs: dict[str, Any] = {"TableName": schema.table, "Segment": n, "TotalSegments": segments,
                                  "Limit": page_size, "ConsistentRead": True}
        try:
            while True:
                page = ddb.scan(**kwargs)
                if page["Items"]:
                    writer.upsert(page["Items"])
                    conn.commit()
                    with lock:
                        total += len(page["Items"])
                        if progress:
                            progress(total)
                if "LastEvaluatedKey" not in page:
                    break
                kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
        finally:
            with lock:
                mismatches.update(writer.mismatches)
            conn.close()

    with ThreadPoolExecutor(max_workers=segments) as pool:
        list(pool.map(segment, range(segments)))
    return total, mismatches


# ------------------------------------------------------------------ stream sync

def _lineage_order(shards: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Parents before children, so one pass can finish a parent and then read its children."""
    by_id = {s["ShardId"]: s for s in shards}

    def depth(s: dict[str, Any]) -> int:
        d = 0
        while s.get("ParentShardId") in by_id:
            s, d = by_id[s["ParentShardId"]], d + 1
        return d
    return sorted(shards, key=depth)


class StreamSync:
    """Applies a DynamoDB stream to PostgreSQL, shard by shard, parents before children, checkpointed in PostgreSQL."""

    def __init__(self, ddb: Any, streams: Any, conn: Any, schema: Schema, stream_arn: str):
        self.ddb, self.streams, self.conn, self.schema, self.arn = ddb, streams, conn, schema, stream_arn
        self.writer = RowWriter(conn, schema)
        self.applied = Counter()

    def _checkpoints(self) -> dict[str, tuple[str | None, bool]]:
        with self.conn.cursor() as cur:
            cur.execute("SELECT shard_id, sequence_number, finished FROM ddb2pg_checkpoints WHERE pg_table = %s",
                        (self.schema.pg_table,))
            return {r[0]: (r[1], r[2]) for r in cur.fetchall()}

    def _save(self, cur: Any, shard: str, seq: str | None, finished: bool) -> None:
        cur.execute("INSERT INTO ddb2pg_checkpoints (pg_table, shard_id, sequence_number, finished) VALUES (%s, %s, %s, %s) "
                    "ON CONFLICT (pg_table, shard_id) DO UPDATE SET sequence_number = COALESCE(EXCLUDED.sequence_number, "
                    "ddb2pg_checkpoints.sequence_number), finished = EXCLUDED.finished, updated_at = now()",
                    (self.schema.pg_table, shard, seq, finished))

    def shards(self) -> list[dict[str, Any]]:
        out, kwargs = [], {"StreamArn": self.arn}
        while True:
            d = self.streams.describe_stream(**kwargs)["StreamDescription"]
            out += d["Shards"]
            if not d.get("LastEvaluatedShardId"):
                return out
            kwargs["ExclusiveStartShardId"] = d["LastEvaluatedShardId"]

    def _image(self, record: dict[str, Any]) -> dict[str, Any] | None:
        d = record["dynamodb"]
        if self.schema.stream_view in ("NEW_IMAGE", "NEW_AND_OLD_IMAGES"):
            return d.get("NewImage")
        return self.ddb.get_item(TableName=self.schema.table, Key=d["Keys"], ConsistentRead=True).get("Item")

    def apply(self, records: list[dict[str, Any]]) -> None:
        for r in records:
            if r["eventName"] == "REMOVE":
                self.writer.delete(r["dynamodb"]["Keys"])
            else:
                image = self._image(r)
                if image is None:  # read back after a later delete: the REMOVE record follows
                    self.writer.delete(r["dynamodb"]["Keys"])
                else:
                    self.writer.upsert([image])
            self.applied[r["eventName"]] += 1

    def run_once(self) -> bool:
        """One pass over every readable shard. Returns True if any record was applied."""
        done = self._checkpoints()
        did = False
        all_shards = _lineage_order(self.shards())
        known = {s["ShardId"] for s in all_shards}
        for shard in all_shards:
            sid, parent = shard["ShardId"], shard.get("ParentShardId")
            if done.get(sid, (None, False))[1]:
                continue
            if parent and parent in known and not done.get(parent, (None, False))[1]:
                continue  # the parent's changes come first
            seq = done.get(sid, (None, False))[0]
            it_args = {"StreamArn": self.arn, "ShardId": sid}
            it_args.update({"ShardIteratorType": "AFTER_SEQUENCE_NUMBER", "SequenceNumber": seq} if seq
                           else {"ShardIteratorType": "TRIM_HORIZON"})
            it = self.streams.get_shard_iterator(**it_args)["ShardIterator"]
            while it:
                page = self.streams.get_records(ShardIterator=it, Limit=1000)
                records, it = page["Records"], page.get("NextShardIterator")
                with self.conn.cursor() as cur:
                    if records:
                        self.apply(records)
                        did = True
                    finished = it is None
                    if records or finished:
                        self._save(cur, sid, records[-1]["dynamodb"]["SequenceNumber"] if records else None, finished)
                self.conn.commit()
                if finished:
                    done[sid] = (None, True)
                if not records:
                    break  # caught up on an open shard
        return did

    def run(self, until_idle: float | None = None, poll: float = 2.0, stop: threading.Event | None = None) -> Counter:
        """Keep applying. Stops after `until_idle` seconds with nothing new, or when `stop` is set."""
        idle_since = time.time()
        while not (stop and stop.is_set()):
            if self.run_once():
                idle_since = time.time()
            elif until_idle is not None and time.time() - idle_since >= until_idle:
                break
            time.sleep(poll)
        return self.applied


# ------------------------------------------------------------------ verify

def verify(ddb: Any, conn: Any, schema: Schema, sample: int = 200) -> dict[str, Any]:
    count, kwargs = 0, {"TableName": schema.table, "Select": "COUNT", "ConsistentRead": True}
    while True:
        page = ddb.scan(**kwargs)
        count += page["Count"]
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {ident(schema.pg_table)}")
        pg_count = cur.fetchone()[0]
        keys = [ident(c.name) for c in schema.key_columns]
        cur.execute(f"SELECT {', '.join(keys)}, doc::text FROM {ident(schema.pg_table)} ORDER BY random() LIMIT %s", (sample,))
        rows = cur.fetchall()
    mismatched = []
    for row in rows:
        key = {}
        for c, v in zip(schema.key_columns, row):
            key[c.attribute] = {c.ddb_type: str(v) if c.ddb_type == "N" else v}
        item = ddb.get_item(TableName=schema.table, Key=key, ConsistentRead=True).get("Item")
        if item is None or json.loads(to_json(item)) != json.loads(row[-1]):
            mismatched.append({k: str(v) for k, v in zip([c.attribute for c in schema.key_columns], row)})
    return {"dynamodb_items": count, "postgres_rows": pg_count, "counts_match": count == pg_count,
            "sampled": len(rows), "sample_mismatches": mismatched, "ok": count == pg_count and not mismatched}


# ------------------------------------------------------------------ AI code rewrite

REWRITE_PROMPT = """You are moving one AWS Lambda function's data access from Amazon DynamoDB to PostgreSQL.
You get the PostgreSQL schema the data was migrated into (and how each DynamoDB attribute maps to a column),
and the function's source code. Return unified diffs that replace every DynamoDB call on the named tables with
SQL against that schema, in the function's own language: psycopg 3 for Python, `pg` (node-postgres) for Node.js.

Rules:
- Keep behaviour identical: conditional writes become INSERT ... ON CONFLICT or UPDATE ... WHERE with a row-count
  check; transactions become a database transaction; Query on a GSI becomes a WHERE on its columns with ORDER BY the
  sort key; pagination tokens become keyset pagination on the primary key.
- `doc` (jsonb) holds the whole item. Read attributes that have no column from doc. Write doc on every insert/update,
  keeping it consistent with the columns, so the DynamoDB-shaped item stays whole.
- The connection comes from the DATABASE_URL environment variable; create it once outside the handler (pool for Node).
- Use bind parameters only, never string formatting, for values.
- Add the driver to requirements.txt or package.json.
- Do not touch code unrelated to these tables. Diffs must apply with `git apply -p1` against paths as given.
Return JSON matching the schema."""

REWRITE_SCHEMA = {
    "type": "object", "additionalProperties": False, "required": ["summary", "patches", "risks"],
    "properties": {
        "summary": {"type": "string"},
        "patches": {"type": "array", "items": {"type": "object", "additionalProperties": False,
                    "required": ["file", "description", "diff"],
                    "properties": {"file": {"type": "string"}, "description": {"type": "string"}, "diff": {"type": "string"}}}},
        "risks": {"type": "array", "items": {"type": "string"}},
    },
}


def rewrite(code_dir: Path, schemas: list[Schema], model: str | None = None, effort: str = "high",
            provider: str = "auto", client: Any = None) -> dict[str, Any]:
    from . import ai
    files, omitted = ai.collect_sources(code_dir)
    mapping = [{"dynamodb_table": s.table, "postgres_table": s.pg_table, "primary_key": [c.name for c in s.key_columns],
                "columns": {c.attribute: {"column": c.name, "type": c.pg_type} for c in s.columns},
                "indexes": s.indexes, "entity_views": list(s.entities), "notes": s.notes} for s in schemas]
    user = "\n\n".join([
        "## PostgreSQL schema\n```sql\n" + "".join(ddl(s) for s in schemas) + "```",
        "## Attribute mapping\n```json\n" + json.dumps(mapping, indent=2) + "\n```",
        *(f"## {rel}\n```\n{text}\n```" for rel, text in files),
        *(["Not sent: " + "; ".join(omitted)] if omitted else []),
    ])
    provider = ai.resolve_provider(provider)
    model = model or ai.DEFAULT_MODELS[provider]
    text = ai.complete_json(REWRITE_PROMPT, user, REWRITE_SCHEMA, model=model, effort=effort, provider=provider,
                            label="the DynamoDB rewrite", client=client)
    result = json.loads(ai._json_text(text))
    sent = {rel for rel, _ in files}
    for p in result["patches"]:
        if p["file"] not in sent:
            p["description"] = "[UNVERIFIED: file was not sent] " + p["description"]
    return result


# ------------------------------------------------------------------ CLI

def _aws(args: argparse.Namespace, service: str) -> Any:
    import boto3
    return boto3.Session(region_name=args.region, profile_name=args.profile).client(
        service, **({"endpoint_url": args.endpoint_url} if args.endpoint_url else {}))


def _connect(url: str):
    try:
        import psycopg
    except ImportError as e:  # pragma: no cover - depends on environment
        raise SystemExit("ddb2pg needs psycopg: pip install 'lambda2eks[postgres]'") from e
    return lambda: psycopg.connect(url)


def sample_items(ddb: Any, table: str, n: int) -> list[dict[str, Any]]:
    """Up to n items, read from random Scan segments so one hot partition does not dominate."""
    segments = max(1, min(16, n // 50))
    items: list[dict[str, Any]] = []
    for seg in random.sample(range(segments), segments):
        kwargs = {"TableName": table, "Segment": seg, "TotalSegments": segments, "Limit": max(1, n // segments)}
        page = ddb.scan(**kwargs)
        items += page["Items"]
        if len(items) >= n:
            break
    return items[:n]


def cmd_plan(args: argparse.Namespace) -> int:
    ddb = _aws(args, "dynamodb")
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for table in args.tables:
        desc = ddb.describe_table(TableName=table)["Table"]
        try:
            ttl = ddb.describe_time_to_live(TableName=table)["TimeToLiveDescription"]
            ttl_attr = ttl.get("AttributeName") if ttl.get("TimeToLiveStatus") in ("ENABLED", "ENABLING") else None
        except Exception:  # noqa: BLE001 - not every emulator implements TTL
            ttl_attr = None
        schema = infer_schema(desc, sample_items(ddb, table, args.sample), ttl_attr, args.min_share)
        (out / f"{schema.pg_table}.schema.json").write_text(schema.to_json())
        (out / f"{schema.pg_table}.sql").write_text(ddl(schema))
        print(f"{table} -> {schema.pg_table}: {len(schema.columns)} columns, {len(schema.indexes)} indexes, "
              f"{len(schema.entities)} entity views, {schema.sampled} items sampled", file=sys.stderr)
        for n in schema.notes:
            print(f"  note: {n}", file=sys.stderr)
    return 0


def _schemas(args: argparse.Namespace) -> list[Schema]:
    return [Schema.from_json(Path(p).read_text()) for p in args.schema]


def cmd_apply(args: argparse.Namespace) -> int:
    conn = _connect(args.database_url)()
    for s in _schemas(args):
        with conn.cursor() as cur:
            cur.execute(ddl(s))
        conn.commit()
        print(f"created {s.pg_table}", file=sys.stderr)
    return 0


def cmd_backfill(args: argparse.Namespace) -> int:
    ddb = _aws(args, "dynamodb")
    for s in _schemas(args):
        t0 = time.time()
        n, mismatches = backfill(ddb, _connect(args.database_url), s, segments=args.segments)
        print(f"{s.table} -> {s.pg_table}: {n} items in {time.time() - t0:.1f}s", file=sys.stderr)
        for attr, c in mismatches.items():
            print(f"  {c} item(s) had another type for {attr!r}: column NULL, value kept in doc", file=sys.stderr)
    return 0


def cmd_sync(args: argparse.Namespace) -> int:
    ddb, streams = _aws(args, "dynamodb"), _aws(args, "dynamodbstreams")
    conn = _connect(args.database_url)()
    for s in _schemas(args):
        arn = ddb.describe_table(TableName=s.table)["Table"].get("LatestStreamArn")
        if not arn:
            raise SystemExit(f"{s.table} has no stream; enable one (NEW_IMAGE) and re-run")
        applied = StreamSync(ddb, streams, conn, s, arn).run(until_idle=args.until_idle)
        print(f"{s.table} -> {s.pg_table}: applied {dict(applied)}", file=sys.stderr)
    return 0


def cmd_verify(args: argparse.Namespace) -> int:
    ddb = _aws(args, "dynamodb")
    conn = _connect(args.database_url)()
    ok = True
    for s in _schemas(args):
        r = verify(ddb, conn, s, args.sample)
        ok &= r["ok"]
        print(json.dumps({"table": s.table, **r}), flush=True)
    return 0 if ok else 1


def cmd_rewrite(args: argparse.Namespace) -> int:
    from . import ai
    try:
        result = rewrite(Path(args.code_dir), _schemas(args), args.model, args.effort, args.provider)
    except ai.AIReviewError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    for i, p in enumerate(result["patches"], 1):
        (out / f"{i:02d}-{snake(p['file'])}.diff").write_text(p["diff"])
    (out / "rewrite.json").write_text(json.dumps(result, indent=2))
    print(f"{len(result['patches'])} patch(es) in {out}; nothing was applied. {result['summary']}", file=sys.stderr)
    for r in result["risks"]:
        print(f"  risk: {r}", file=sys.stderr)
    return 0


def add_parser(sub: Any) -> None:
    p = sub.add_parser("ddb2pg", help="migrate DynamoDB tables to PostgreSQL (schema, backfill, stream sync, verify, code)")
    steps = p.add_subparsers(dest="step", required=True)

    def common(s: Any, db: bool = True, schema: bool = True) -> Any:
        s.add_argument("--region")
        s.add_argument("--profile")
        s.add_argument("--endpoint-url", help="DynamoDB endpoint, e.g. LocalStack")
        if db:
            s.add_argument("--database-url", required=True, help="postgresql://user:pass@host:5432/db")
        if schema:
            s.add_argument("--schema", action="append", required=True, metavar="FILE", help="<table>.schema.json from plan; repeatable")
        return s

    s = common(steps.add_parser("plan", help="infer the schema from the table and sampled items"), db=False, schema=False)
    s.add_argument("tables", nargs="+")
    s.add_argument("--out", default="ddb2pg")
    s.add_argument("--sample", type=int, default=1000, help="items to sample (default 1000)")
    s.add_argument("--min-share", type=float, default=0.0, help="only give a column to attributes in at least this share of items")
    s.set_defaults(func=cmd_plan)
    common(steps.add_parser("apply", help="create the tables, indexes and views")).set_defaults(func=cmd_apply)
    s = common(steps.add_parser("backfill", help="parallel Scan into PostgreSQL"))
    s.add_argument("--segments", type=int, default=8)
    s.set_defaults(func=cmd_backfill)
    s = common(steps.add_parser("sync", help="apply the DynamoDB stream until the cut-over"))
    s.add_argument("--until-idle", type=float, help="stop after this many seconds with no new changes (default: run forever)")
    s.set_defaults(func=cmd_sync)
    s = common(steps.add_parser("verify", help="compare counts and a sample of items"))
    s.add_argument("--sample", type=int, default=200)
    s.set_defaults(func=cmd_verify)
    s = common(steps.add_parser("rewrite", help="AI patches replacing DynamoDB calls with SQL"), db=False)
    s.add_argument("code_dir")
    s.add_argument("--out", default="ddb2pg/patches")
    s.add_argument("--provider", choices=["auto", "anthropic", "together"], default="auto")
    s.add_argument("--model")
    s.add_argument("--effort", default="high", choices=["low", "medium", "high", "xhigh", "max"])
    s.set_defaults(func=cmd_rewrite)
