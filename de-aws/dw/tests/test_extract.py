"""Pruebas sin AWS de la extracción por lotes: formato CSV, manifiestos y SQL de carga."""

import csv
import gzip
import io
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import boto3
import pytest
from moto import mock_aws

from extract import s3_batches
from extract.entities import ALL_ENTITIES, ENTITIES_BY_LABEL
from extract.extractors import CsvGzipWriter, flatten_document, format_value
from extract.redshift import _copy_sql, raw_table_ddl

CUSTOMERS = ENTITIES_BY_LABEL["mysql.customers"]
REVIEWS = ENTITIES_BY_LABEL["documentdb.reviews"]


def test_format_value():
    assert format_value(None, "varchar(10)") == ""
    assert format_value(1, "boolean") == "true" and format_value(0, "boolean") == "false"
    assert format_value(Decimal("10.50"), "numeric(12,2)") == "10.50"
    assert format_value(date(2025, 1, 2), "date") == "2025-01-02"
    bogota = timezone(timedelta(hours=-5))
    assert format_value(datetime(2025, 1, 1, 20, 0, tzinfo=bogota), "timestamptz") == "2025-01-02 01:00:00"


def test_csv_writer_tracks_rows_and_watermark():
    writer = CsvGzipWriter(CUSTOMERS)
    metadata = ("b1", "full", "2026-10-21 10:00:00", "mysql.customers")
    for day in (3, 1, 2):
        writer.write({"customer_id": day, "name": 'Ana, "la" de\nBogotá', "updated_at": datetime(2025, 1, day)}, metadata)
    rows = list(csv.reader(io.StringIO(gzip.decompress(writer.close()).decode("utf-8"))))
    assert rows[0] == CUSTOMERS.file_columns
    assert len(rows) == 4 and writer.rows == 3
    assert rows[1][1] == 'Ana, "la" de\nBogotá'  # comas, comillas y saltos sobreviven
    assert writer.max_watermark == datetime(2025, 1, 3)


def test_flatten_review_keeps_full_document():
    document = {"_id": "review-1", "review_id": 1, "rating": 5, "customer": {"customer_id": 7},
                "product": {"product_id": 9}, "reviewed_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
                "verification": {"purchase_confirmed": True}}
    record = flatten_document(REVIEWS, document)
    assert record["customer_id"] == 7 and record["product_id"] == 9
    assert '"purchase_confirmed": true' in record["document_json"]


def test_raw_ddl_and_copy_sql():
    for entity in ALL_ENTITIES:
        ddl = raw_table_ddl(entity)
        assert ddl.startswith('create table if not exists "raw".') and "_batch_id varchar(64)" in ddl
    sql = _copy_sql(CUSTOMERS, "bucket", "b1", "arn:aws:iam::1:role/r")
    assert "from 's3://bucket/raw/mysql/customers/batch_id=b1/part-'" in sql
    assert "iam_role 'arn:aws:iam::1:role/r'" in sql and "gzip" in sql
    assert "_loaded_at" not in sql  # lo pone Redshift con DEFAULT sysdate


@pytest.mark.parametrize("batch_id", ["../x", "", "a b", "x" * 65])
def test_invalid_batch_ids(batch_id):
    with pytest.raises(ValueError):
        s3_batches.validate_batch_id(batch_id)


def test_published_batch_is_never_overwritten(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="dw-test-bucket")
        manifest = s3_batches.publish_batch_entity("dw-test-bucket", CUSTOMERS, "b1", b"data", {"rows": 1}, client)
        assert manifest["data_key"] == "raw/mysql/customers/batch_id=b1/part-00000.csv.gz"
        assert s3_batches.read_manifest("dw-test-bucket", CUSTOMERS, "b1", client)["rows"] == 1
        with pytest.raises(s3_batches.BatchAlreadyPublished):
            s3_batches.publish_batch_entity("dw-test-bucket", CUSTOMERS, "b1", b"other", {"rows": 2}, client)
        body = client.get_object(Bucket="dw-test-bucket", Key=manifest["data_key"])["Body"].read()
        assert body == b"data"


def test_source_generation_marker_roundtrip(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "x")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(Bucket="dw-test-bucket")
        assert s3_batches.read_source_generation("dw-test-bucket", client) is None
        s3_batches.write_source_generation("dw-test-bucket", {"generated_at": "2026-10-21T10:00:00", "seed": 42}, client)
        assert s3_batches.read_source_generation("dw-test-bucket", client)["seed"] == 42


class FakeOrdersCursor:
    """Responde las dos consultas de ensure_batch_order con los order_id indicados."""

    def __init__(self, order_ids):
        self.order_ids = order_ids
        self.result = None

    def execute(self, sql, params):
        if "COUNT(*)" in sql:
            low, high = params
            self.result = (sum(low <= order_id <= high for order_id in self.order_ids),)
        else:
            self.result = (max((order_id for order_id in self.order_ids if order_id >= params[0]), default=None),)

    def fetchone(self):
        return self.result


def test_append_batch_must_follow_watermark_order():
    from extract.sources.append_batch import BASE_ID, ensure_batch_order

    batch_2 = [BASE_ID + 20, BASE_ID + 21, BASE_ID + 22]
    ensure_batch_order(FakeOrdersCursor([1, 2]), 2)  # sin lotes previos
    ensure_batch_order(FakeOrdersCursor(batch_2), 3)  # lote posterior
    ensure_batch_order(FakeOrdersCursor(batch_2), 2)  # repetir el mismo lote
    with pytest.raises(ValueError, match="older than the last appended batch"):
        ensure_batch_order(FakeOrdersCursor(batch_2), 1)


def _airflow_tasks(monkeypatch):
    """Importa airflow_tasks sin Airflow instalado: basta con AirflowFailException."""
    import sys
    import types

    if "airflow" not in sys.modules:
        airflow = types.ModuleType("airflow")
        airflow.exceptions = types.ModuleType("airflow.exceptions")
        airflow.exceptions.AirflowFailException = type("AirflowFailException", (Exception,), {})
        monkeypatch.setitem(sys.modules, "airflow", airflow)
        monkeypatch.setitem(sys.modules, "airflow.exceptions", airflow.exceptions)
    from extract import airflow_tasks

    return airflow_tasks


class FakeTI:
    def __init__(self, task_id, state=None, batch=None):
        self.task_id, self.state, self.batch = task_id, state, batch

    def xcom_pull(self, task_ids):
        return self.batch


class FakeDagRun:
    run_id = "manual__1"

    def __init__(self, states):
        self.states = states

    def get_task_instances(self):
        return [FakeTI(task_id, state) for task_id, state in self.states.items()]


def test_record_result_registers_batch_that_failed_before_resolve(monkeypatch):
    tasks = _airflow_tasks(monkeypatch)
    calls = []
    monkeypatch.setattr(tasks.redshift, "record_failed_batch", lambda *args: calls.append(("failed", args)))
    monkeypatch.setattr(tasks.redshift, "set_status", lambda *args: calls.append(("status", args)))
    context = {
        "dag_run": FakeDagRun({"validate_connections": "failed", "resolve_batch": "upstream_failed"}),
        "ti": FakeTI("record_result"),
        "params": {"batch_id": "b1", "mode": "auto"},
    }
    with pytest.raises(tasks.AirflowFailException):
        tasks.record_result(**context)
    assert calls[0][0] == "failed" and calls[0][1][:3] == ("b1", "manual__1", "auto")
    assert "validate_connections" in calls[0][1][3]

    calls.clear()
    context["ti"] = FakeTI("record_result", batch={"batch_id": "b1", "mode": "full"})
    context["dag_run"] = FakeDagRun({"validate_connections": "success", "reconcile": "success"})
    tasks.record_result(**context)
    assert calls == [("status", ("b1", "success", "Pipeline completo"))]


class FakeBatchRunsCursor:
    def __init__(self, existing):
        self.existing, self.statements, self.result = existing, [], None

    def execute(self, sql, params=None):
        self.statements.append(sql)
        self.result = (1,) if sql.startswith("select 1") and self.existing else None

    def fetchone(self):
        return self.result


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor

    def commit(self):
        pass

    def close(self):
        pass


@pytest.mark.parametrize("existing", [False, True])
def test_record_failed_batch_never_overwrites_a_registered_batch(monkeypatch, existing):
    from extract import redshift

    cursor = FakeBatchRunsCursor(existing)
    monkeypatch.setattr(redshift, "redshift_connection", lambda: FakeConnection(cursor))
    redshift.record_failed_batch("b1", "manual__1", "auto", "Tareas fallidas antes de registrar el lote: x")
    inserts = [sql for sql in cursor.statements if sql.startswith("insert into ops.batch_runs")]
    assert len(inserts) == (0 if existing else 1)
    assert not any(sql.startswith("update ops.batch_runs") for sql in cursor.statements)


def test_failure_injection_logs_batch_and_task(monkeypatch, caplog):
    tasks = _airflow_tasks(monkeypatch)
    monkeypatch.setattr(tasks, "bucket_name", lambda: "bucket")
    monkeypatch.setattr(tasks, "put_marker_once", lambda *args: True)
    with caplog.at_level("ERROR"), pytest.raises(tasks.AirflowFailException):
        tasks.failure_injection({"batch_id": "b7"}, params={"simulate_failure": True})
    assert "[batch=b7 task=failure_injection] Fallo controlado del lote b7" in caplog.text
