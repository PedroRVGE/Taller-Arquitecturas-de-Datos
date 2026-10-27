"""Configuración: variables de entorno (infraestructura) y feeds.yml (fuentes)."""

import os
from functools import lru_cache
from pathlib import Path

import yaml

SRC_DIR = Path(__file__).resolve().parent.parent
FEEDS_FILE = Path(os.getenv("NEWSLAKE_FEEDS_FILE", str(SRC_DIR / "feeds.yml")))
SQL_DIR = Path(os.getenv("NEWSLAKE_SQL_DIR", str(SRC_DIR.parent / "sql" / "athena")))

SILVER_TABLES = ("news", "news_entities", "news_key_phrases")
GOLD_TABLES = ("gold_news_daily", "gold_entity_daily", "gold_key_phrases_daily")
RUN_TABLES = ("news_quarantine", "comprehend_results")  # una partición por corrida
JSON_TABLES = ("bronze_ingestions", "pipeline_runs")
ALL_TABLES = SILVER_TABLES + GOLD_TABLES + RUN_TABLES + JSON_TABLES
UNKNOWN_DATE = "unknown"
WRITER_POOL = "newslake_writer"


def required(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta la variable de entorno {name}")
    return value


def bucket():
    return required("LAKE_BUCKET")


def glue_database():
    return os.getenv("GLUE_DATABASE", "news_lake")


def athena_workgroup():
    return required("ATHENA_WORKGROUP")


def region():
    return os.getenv("AWS_DEFAULT_REGION", "us-east-1")


@lru_cache(maxsize=1)
def feeds_config():
    with FEEDS_FILE.open(encoding="utf-8") as stream:
        config = yaml.safe_load(stream)
    sources = []
    seen = set()
    for source in config["sources"]:
        if source["id"] in seen:
            raise ValueError(f"Fuente repetida en feeds.yml: {source['id']}")
        if not source["url"].endswith(".xml"):
            raise ValueError(f"{source['id']}: la URL debe ser el feed XML, no la página del directorio")
        seen.add(source["id"])
        sources.append({
            "id": source["id"],
            "media": source.get("media", config["media"]),
            "section": source["section"],
            "url": source["url"],
            "language": source.get("language", config["language"]),
        })
    return {**config, "sources": sources}


def sources():
    return feeds_config()["sources"]


def source_by_id(source_id):
    for source in sources():
        if source["id"] == source_id:
            return source
    raise KeyError(source_id)
