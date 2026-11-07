import json
from decimal import Decimal

from lambda2eks import ai, ddb2pg
from lambda2eks.ddb2pg import RowWriter, Schema, StreamSync, ddl, infer_schema, to_json

DESC = {
    "TableName": "OrdersTable",
    "KeySchema": [{"AttributeName": "PK", "KeyType": "HASH"}, {"AttributeName": "SK", "KeyType": "RANGE"}],
    "AttributeDefinitions": [{"AttributeName": "PK", "AttributeType": "S"}, {"AttributeName": "SK", "AttributeType": "S"},
                             {"AttributeName": "GSI1PK", "AttributeType": "S"}, {"AttributeName": "createdAt", "AttributeType": "N"}],
    "GlobalSecondaryIndexes": [{"IndexName": "GSI1", "Projection": {"ProjectionType": "ALL"},
                                "KeySchema": [{"AttributeName": "createdAt", "KeyType": "RANGE"},
                                              {"AttributeName": "GSI1PK", "KeyType": "HASH"}]}],
    "StreamSpecification": {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"},
    "ItemCount": 3,
}
ITEMS = [
    {"PK": {"S": "USER#1"}, "SK": {"S": "PROFILE"}, "name": {"S": "Ada"}, "tags": {"SS": ["b", "a"]}, "score": {"N": "1"}},
    {"PK": {"S": "USER#1"}, "SK": {"S": "ORDER#9"}, "GSI1PK": {"S": "ORDER"}, "createdAt": {"N": "1700000000"},
     "total": {"N": "12.50"}, "lines": {"L": [{"M": {"sku": {"S": "x"}}}]}, "score": {"S": "high"}},
    {"PK": {"S": "ORDER#9"}, "SK": {"S": "META"}, "paid": {"BOOL": True}, "blob": {"B": b"\x00\x01"}},
]


def schema():
    return infer_schema(DESC, ITEMS, ttl_attribute="expiresAt")


def test_infer_schema_types_keys_indexes_and_entities():
    s = schema()
    cols = {c.attribute: (c.name, c.pg_type, c.key) for c in s.columns}
    assert cols["PK"] == ("pk", "text", True) and cols["SK"] == ("sk", "text", True)
    assert cols["GSI1PK"] == ("gsi1_pk", "text", False) and cols["createdAt"] == ("created_at", "numeric", False)
    assert cols["tags"][1] == "text[]" and cols["lines"][1] == "jsonb" and cols["paid"][1] == "boolean"
    assert cols["blob"][1] == "bytea" and cols["total"][1] == "numeric"
    assert "score" not in cols  # N in one item, S in another: doc only
    assert any("score" in n for n in s.notes)
    assert s.indexes == [{"name": "GSI1", "kind": "GSI", "attributes": ["GSI1PK", "createdAt"], "projection": "ALL"}]
    assert s.entities == {"USER": 2, "ORDER": 1}
    assert s.pg_table == "orders_table" and s.stream_view == "NEW_AND_OLD_IMAGES"


def test_ddl_has_primary_key_index_views_and_checkpoints():
    sql = ddl(schema())
    assert 'CREATE TABLE IF NOT EXISTS "orders_table"' in sql and 'PRIMARY KEY ("pk", "sk")' in sql
    assert 'ON "orders_table" ("gsi1_pk", "created_at");  -- GSI GSI1' in sql
    assert ("CREATE OR REPLACE VIEW \"orders_table_user\" AS SELECT * FROM \"orders_table\" "
            "WHERE \"pk\" = 'USER' OR \"pk\" LIKE 'USER#%';") in sql
    assert "ddb2pg_checkpoints" in sql and "doc jsonb NOT NULL" in sql


def test_entities_from_the_sort_key_when_the_partition_key_has_one_prefix():
    items = [{"PK": {"S": f"USER#{i}"}, "SK": {"S": "PROFILE" if i % 3 == 0 else f"ORDER#{i}"}} for i in range(9)]
    s = infer_schema(DESC, items)
    assert s.entity_key == "SK" and s.entities == {"ORDER": 6, "PROFILE": 3}
    assert "WHERE \"sk\" = 'PROFILE' OR \"sk\" LIKE 'PROFILE#%';" in ddl(s)


def test_doc_is_lossless_json():
    doc = json.loads(to_json(ITEMS[1]))
    assert doc["total"] == 12.5 and doc["createdAt"] == 1700000000 and doc["lines"] == [{"sku": "x"}]
    assert json.loads(to_json(ITEMS[0]))["tags"] == ["a", "b"]
    assert json.loads(to_json(ITEMS[2]))["blob"] == "AAE="


def test_schema_round_trips_through_json():
    s = schema()
    assert Schema.from_json(s.to_json()) == s


class FakeCursor:
    def __init__(self, db):
        self.db = db

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def executemany(self, sql, rows):
        for r in rows:
            self.execute(sql, r)

    def execute(self, sql, params=()):
        self.db.log.append((sql.split()[0], params))
        if sql.startswith("INSERT INTO \"orders_table\""):
            self.db.rows[(params[0], params[1])] = params
        elif sql.startswith("DELETE FROM \"orders_table\""):
            self.db.rows.pop(tuple(params), None)
        elif sql.startswith("SELECT shard_id"):
            self._result = [(k, v[0], v[1]) for k, v in self.db.checkpoints.items()]
        elif sql.startswith("INSERT INTO ddb2pg_checkpoints"):
            _, shard, seq, fin = params
            old = self.db.checkpoints.get(shard, (None, False))
            self.db.checkpoints[shard] = (seq or old[0], fin)

    def fetchall(self):
        return self._result


class FakeConn:
    def __init__(self):
        self.rows, self.checkpoints, self.log = {}, {}, []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        pass


def test_row_writer_maps_columns_and_nulls_type_mismatches():
    s = schema()
    w = RowWriter(FakeConn(), s)
    row = w.row({"PK": {"S": "X#1"}, "SK": {"S": "A"}, "createdAt": {"S": "yesterday"}, "total": {"N": "3"}})
    names = [c.name for c in s.columns]
    assert row[names.index("created_at")] is None and w.mismatches["createdAt"] == 1
    assert row[names.index("total")] == Decimal("3") and json.loads(row[-1])["createdAt"] == "yesterday"
    assert "ON CONFLICT (\"pk\", \"sk\") DO UPDATE" in w.upsert_sql


class FakeStreams:
    """Parent shard (closed) and child shard (open). The child's records must not apply before the parent's."""

    def __init__(self):
        rec = lambda name, seq, sk, image=True: {  # noqa: E731
            "eventName": name, "dynamodb": {"SequenceNumber": seq, "Keys": {"PK": {"S": "U#1"}, "SK": {"S": sk}},
                                            **({"NewImage": {"PK": {"S": "U#1"}, "SK": {"S": sk}, "v": {"N": seq}}} if image else {})}}
        self.records = {"parent": [rec("INSERT", "1", "a"), rec("MODIFY", "2", "a")],
                        "child": [rec("REMOVE", "3", "a", image=False), rec("INSERT", "4", "b")]}

    def describe_stream(self, **kw):
        return {"StreamDescription": {"Shards": [{"ShardId": "child", "ParentShardId": "parent"}, {"ShardId": "parent"}]}}

    def get_shard_iterator(self, StreamArn, ShardId, ShardIteratorType, SequenceNumber=None):
        recs = self.records[ShardId]
        start = 0 if ShardIteratorType == "TRIM_HORIZON" else next(i + 1 for i, r in enumerate(recs) if r["dynamodb"]["SequenceNumber"] == SequenceNumber)
        return {"ShardIterator": f"{ShardId}:{start}"}

    def get_records(self, ShardIterator, Limit):
        shard, start = ShardIterator.split(":")
        recs = self.records[shard][int(start):]
        closed = shard == "parent"
        return {"Records": recs, "NextShardIterator": None if closed else f"{shard}:{int(start) + len(recs)}"}


def test_stream_sync_orders_parent_before_child_and_resumes():
    s = schema()
    conn, streams = FakeConn(), FakeStreams()
    sync = StreamSync(None, streams, conn, s, "arn")
    assert sync.run_once()  # parent applied and finished; child applied in the same pass
    assert set(conn.rows) == {("U#1", "b")}  # a: inserted, modified, removed; b: inserted
    assert conn.checkpoints == {"parent": ("2", True), "child": ("4", False)}
    assert sync.applied == {"INSERT": 2, "MODIFY": 1, "REMOVE": 1}
    assert not StreamSync(None, streams, conn, s, "arn").run_once()  # resumes after seq 4: nothing new


def test_rewrite_sends_schema_and_code_and_flags_unknown_files(tmp_path, monkeypatch):
    (tmp_path / "app.py").write_text("import boto3\ntable = boto3.resource('dynamodb').Table('OrdersTable')\n")
    seen = {}

    def fake(system, user, schema_, model, effort, provider, label, client):
        seen.update(system=system, user=user, schema=schema_)
        return json.dumps({"summary": "s", "risks": [], "patches": [
            {"file": "app.py", "description": "use psycopg", "diff": "--- a/app.py\n+++ b/app.py\n"},
            {"file": "other.py", "description": "x", "diff": ""}]})
    monkeypatch.setattr(ai, "complete_json", fake)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "x")
    result = ddb2pg.rewrite(tmp_path, [schema()], provider="anthropic")
    assert "CREATE TABLE IF NOT EXISTS \"orders_table\"" in seen["user"] and "Table('OrdersTable')" in seen["user"]
    assert seen["schema"] is ddb2pg.REWRITE_SCHEMA
    assert result["patches"][1]["description"].startswith("[UNVERIFIED")
