"""Ingesta a Bronze: consulta los feeds y guarda la respuesta original.

    bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/feed.xml
    bronze/source=<fuente>/ingestion_date=AAAA-MM-DD/run_id=<id>/_metadata.json
    control/bronze_log/run_id=<id>__source=<fuente>.json   (también para fallos)

Una corrida que ya tiene registro para una fuente no vuelve a consultar el RSS: se
reprocesa desde Bronze. Las escrituras son condicionales, así un reproceso nunca crea
otra copia lógica del mismo lote.
"""

import hashlib
import logging
from datetime import datetime, timezone
from xml.etree import ElementTree

import requests
from defusedxml import ElementTree as SafeElementTree

from . import rss, storage
from .settings import feeds_config

LOGGER = logging.getLogger(__name__)
TRANSIENT_HTTP = {408, 425, 429, 500, 502, 503, 504}


class TransientSourceError(RuntimeError):
    """Error que justifica un reintento de Airflow (timeout, 5xx, 429)."""


class PermanentSourceError(RuntimeError):
    """Error que no se corrige reintentando (404, XML inválido, respuesta vacía)."""


def log_key(run_id, source_id):
    return f"control/bronze_log/run_id={run_id}__source={source_id}.json"


def bronze_prefix(source_id, ingested_at, run_id):
    return f"bronze/source={source_id}/ingestion_date={ingested_at:%Y-%m-%d}/run_id={run_id}/"


def existing_record(run_id, source_id):
    return storage.get_json(log_key(run_id, source_id))


def _record(run_id, source, ingested_at, **fields):
    record = {
        "run_id": run_id,
        "source_id": source["id"],
        "media": source["media"],
        "section": source["section"],
        "url": source["url"],
        "ingested_at_utc": ingested_at.isoformat(),
        "http_status": None,
        "status": None,
        "content_sha256": None,
        "bytes": None,
        "item_count": None,
        "bronze_key": None,
        "origin": "http",
        "error": None,
    }
    record.update(fields)
    return record


def store_valid_response(run_id, source, ingested_at, content, http_status, origin, log=LOGGER):
    """Valida el XML y guarda el original. Devuelve el registro de la bitácora."""
    feed = rss.parse_feed(content)  # FeedError si está vacío o malformado
    prefix = bronze_prefix(source["id"], ingested_at, run_id)
    key = prefix + "feed.xml"
    if not storage.put_if_absent(key, content, "application/rss+xml"):
        log.info("%s: %s ya existía; se conserva el original", source["id"], key)
        # Hash, tamaño e ítems deben describir el archivo guardado, no la respuesta descartada.
        content = storage.get(key)
        feed = rss.parse_feed(content)
    record = _record(
        run_id, source, ingested_at,
        http_status=http_status,
        status="ok" if feed["items"] else "no_items",
        content_sha256=hashlib.sha256(content).hexdigest(),
        bytes=len(content),
        item_count=len(feed["items"]),
        bronze_key=key,
        origin=origin,
    )
    storage.put_json(prefix + "_metadata.json", record)
    storage.put_json(log_key(run_id, source["id"]), record)
    return record


def fetch(source, run_id, log=LOGGER, url_override=None):
    """Descarga un feed y lo guarda en Bronze. Los errores quedan en la bitácora."""
    config = feeds_config()
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    url = url_override or source["url"]
    try:
        response = requests.get(
            url, timeout=config["request_timeout_seconds"], headers={"User-Agent": config["user_agent"]}
        )
    except (requests.Timeout, requests.ConnectionError) as error:
        storage.put_json(log_key(run_id, source["id"]), _record(
            run_id, source, ingested_at, status="network_error", url=url, error=str(error)[:500]))
        raise TransientSourceError(f"{source['id']}: {error}") from error

    if response.status_code != 200:
        storage.put_json(log_key(run_id, source["id"]), _record(
            run_id, source, ingested_at, status="http_error", url=url, http_status=response.status_code,
            error=response.text[:300]))
        error_class = TransientSourceError if response.status_code in TRANSIENT_HTTP else PermanentSourceError
        raise error_class(f"{source['id']}: HTTP {response.status_code} en {url}")

    try:
        record = store_valid_response(run_id, source, ingested_at, response.content, 200, "http", log)
    except rss.FeedError as error:
        storage.put_json(log_key(run_id, source["id"]), _record(
            run_id, source, ingested_at, status=error.status, url=url, http_status=200,
            bytes=len(response.content), content_sha256=hashlib.sha256(response.content).hexdigest(),
            error=str(error)[:500]))
        raise PermanentSourceError(f"{source['id']}: {error}") from error
    log.info("%s: HTTP 200, %s ítems, %s bytes -> s3://%s", source["id"], record["item_count"], record["bytes"],
             record["bronze_key"])
    return record


def make_fixture(source, run_id, from_run_id, log=LOGGER):
    """Copia controlada de un RSS de Bronze con la primera noticia modificada.

    Sirve para demostrar una noticia con contenido cambiado sin depender de que el medio
    publique una actualización durante la demostración.
    """
    original = existing_record(from_run_id, source["id"])
    if not original or not original.get("bronze_key"):
        raise PermanentSourceError(f"{source['id']}: la corrida {from_run_id} no tiene Bronze")
    root = SafeElementTree.fromstring(storage.get(original["bronze_key"]))
    first = root.find("channel/item")
    if first is None or first.find("title") is None:
        raise PermanentSourceError(f"{source['id']}: la corrida {from_run_id} no tiene ítems para modificar")
    title = first.find("title")
    old_title = (title.text or "").strip()
    title.text = f"{old_title} (actualizada)"
    content = ElementTree.tostring(root, encoding="utf-8", xml_declaration=True)
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    log.info("%s: fixture desde %s; título modificado: %s", source["id"], from_run_id, old_title[:80])
    return store_valid_response(run_id, source, ingested_at, content, 200, "fixture", log)


def runs_in_bronze():
    """Registros válidos de Bronze de todas las corridas, en orden cronológico (para rebuild)."""
    records = [storage.get_json(key) for key in storage.list_keys("control/bronze_log/") if key.endswith(".json")]
    valid = [r for r in records if r and r.get("status") in ("ok", "no_items") and r.get("bronze_key")]
    return sorted(valid, key=lambda r: (r["ingested_at_utc"], r["run_id"], r["source_id"]))
