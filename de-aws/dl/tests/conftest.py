"""Pruebas locales del data lake: S3 y Glue simulados con moto, Comprehend y HTTP falsos.

fixtures/glue_tables.json se genera desde Terraform para usar exactamente las mismas tablas:
    cd dl/terraform && echo 'jsonencode(local.glue_tables)' | terraform console
"""

import json
import sys
import types
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIXTURES = Path(__file__).resolve().parent / "fixtures"
sys.path.insert(0, str(ROOT / "src"))


# Airflow no está instalado en el entorno de pruebas: se simulan los dos objetos que usa el código.
class AirflowFailException(Exception):
    pass


class _Pool:
    pools = {}

    @classmethod
    def get_pool(cls, name):
        return cls.pools.get(name)

    @classmethod
    def create_or_update_pool(cls, name, slots, description, include_deferred):
        cls.pools[name] = slots


airflow = types.ModuleType("airflow")
airflow.exceptions = types.ModuleType("airflow.exceptions")
airflow.exceptions.AirflowFailException = AirflowFailException
airflow.models = types.ModuleType("airflow.models")
airflow.models.pool = types.ModuleType("airflow.models.pool")
airflow.models.pool.Pool = _Pool
sys.modules.update({
    "airflow": airflow,
    "airflow.exceptions": airflow.exceptions,
    "airflow.models": airflow.models,
    "airflow.models.pool": airflow.models.pool,
})

BUCKET = "test-lake"
DATABASE = "news_lake"
FORMATS = {
    "parquet": ("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
    "json": ("org.apache.hadoop.mapred.TextInputFormat",
             "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
             "org.openx.data.jsonserde.JsonSerDe"),
}


@pytest.fixture
def aws(monkeypatch):
    from moto import mock_aws

    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("LAKE_BUCKET", BUCKET)
    monkeypatch.setenv("GLUE_DATABASE", DATABASE)
    monkeypatch.setenv("ATHENA_WORKGROUP", "test")
    with mock_aws():
        import boto3

        boto3.client("s3").create_bucket(Bucket=BUCKET)
        glue = boto3.client("glue")
        glue.create_database(DatabaseInput={"Name": DATABASE})
        for name, table in json.loads((FIXTURES / "glue_tables.json").read_text()).items():
            input_format, output_format, serde = FORMATS[table["format"]]
            glue.create_table(DatabaseName=DATABASE, TableInput={
                "Name": name,
                "TableType": "EXTERNAL_TABLE",
                "StorageDescriptor": {
                    "Columns": [{"Name": c[0], "Type": c[1], "Comment": c[2]} for c in table["columns"]],
                    "Location": f"s3://{BUCKET}/{table['location']}",
                    "InputFormat": input_format,
                    "OutputFormat": output_format,
                    "SerdeInfo": {"SerializationLibrary": serde},
                },
                "PartitionKeys": [{"Name": p[0], "Type": p[1]} for p in table["partitions"]],
            })
        from newslake import storage

        storage._TABLE_CACHE.clear()
        yield


class FakeComprehend:
    """Devuelve como entidades las palabras con mayúscula inicial; cuenta las llamadas."""

    def __init__(self):
        self.calls = 0

    def detect_entities(self, Text, LanguageCode):
        self.calls += 1
        entities, offset = [], 0
        for index, word in enumerate(Text.split()):
            clean = word.strip(".,:;\"'()¿?¡!")
            if clean[:1].isupper() and len(clean) > 3:
                kind = ("PERSON", "ORGANIZATION", "LOCATION")[index % 3]
                entities.append({"Type": kind, "Text": clean, "Score": 0.9, "BeginOffset": offset})
            offset += len(word) + 1
        return {"Entities": entities}

    def detect_key_phrases(self, Text, LanguageCode):
        words = [w.strip(".,:;") for w in Text.split() if len(w) > 6][:5]
        return {"KeyPhrases": [{"Text": w, "Score": 0.8, "BeginOffset": i} for i, w in enumerate(words)]}

    def detect_dominant_language(self, Text):
        return {"Languages": [{"LanguageCode": "es", "Score": 0.99}]}


@pytest.fixture
def comprehend(monkeypatch):
    import boto3

    fake = FakeComprehend()
    original = boto3.client

    def client(service, *args, **kwargs):
        return fake if service == "comprehend" else original(service, *args, **kwargs)

    monkeypatch.setattr(boto3, "client", client)
    return fake


@pytest.fixture
def feeds(monkeypatch):
    """Sirve los RSS de fixtures/ en lugar de consultar El Tiempo."""
    import requests

    files = {
        "https://www.eltiempo.com/rss/economia.xml": "economia.xml",
        "https://www.eltiempo.com/rss/politica.xml": "politica.xml",
        "https://www.eltiempo.com/rss/deportes.xml": "deportes.xml",
    }

    class Response:
        def __init__(self, status, content):
            self.status_code = status
            self.content = content
            self.text = content.decode("utf-8", errors="ignore")

    def get(url, timeout, headers):
        if url in files:
            return Response(200, (FIXTURES / files[url]).read_bytes())
        return Response(404, b"Not found")

    monkeypatch.setattr(requests, "get", get)
    return files
