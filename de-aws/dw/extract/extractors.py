"""Extracción completa o incremental de RDS for MySQL y DocumentDB a CSV comprimido en S3.

Formato elegido: CSV con gzip.
- Es el mismo formato de la versión local y se audita con cualquier herramienta.
- COPY de Redshift lo carga de forma nativa (FORMAT AS CSV GZIP) y en paralelo.
- Los tipos se fijan en las tablas raw y en staging, no en el archivo; Parquet exigiría
  que el esquema del archivo coincida exactamente con la tabla destino.
- Los documentos de DocumentDB viajan completos en la columna document_json.
"""

import csv
import gzip
import io
import logging
from datetime import date, datetime, timezone
from decimal import Decimal

from bson import json_util
from pymysql.cursors import SSCursor

from .connections import mongo_client, mongo_database, mysql_connection
from .entities import DOCUMENTDB_ENTITIES, MYSQL_ENTITIES
from .s3_batches import publish_batch_entity, read_manifest

FETCH_SIZE = 1000
LOGGER = logging.getLogger(__name__)


def format_value(value, redshift_type):
    """Convierte un valor de la fuente al texto que COPY espera."""
    if value is None:
        return ""
    if redshift_type == "boolean":
        return "true" if value in (True, 1, "1", "true", "True") else "false"
    if isinstance(value, datetime):
        if value.tzinfo is not None:  # DocumentDB guarda UTC: se escribe en UTC sin zona
            value = value.astimezone(timezone.utc).replace(tzinfo=None)
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    return str(value)


def watermark_to_text(entity, value):
    if value is None:
        return None
    if entity.watermark_kind == "timestamp":
        return format_value(value, "timestamp")
    return str(int(value))


def watermark_from_text(entity, text):
    if text is None:
        return None
    if entity.watermark_kind == "timestamp":
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return int(text)


class CsvGzipWriter:
    """Acumula filas en memoria (volumen del curso) y devuelve bytes gzip."""

    def __init__(self, entity):
        self.entity = entity
        self.buffer = io.BytesIO()
        self.gzip = gzip.GzipFile(fileobj=self.buffer, mode="wb", mtime=0)
        self.text = io.TextIOWrapper(self.gzip, encoding="utf-8", newline="")
        self.writer = csv.writer(self.text)
        self.writer.writerow(entity.file_columns)
        self.rows = 0
        self.max_watermark = None
        self.types = dict(entity.columns)

    def write(self, record, metadata):
        values = [format_value(record.get(name), self.types[name]) for name in self.entity.column_names]
        self.writer.writerow(values + list(metadata))
        self.rows += 1
        current = record.get(self.entity.watermark_column)
        if current is not None and (self.max_watermark is None or current > self.max_watermark):
            self.max_watermark = current

    def close(self):
        self.text.flush()
        self.text.detach()
        self.gzip.close()
        return self.buffer.getvalue()


def _mysql_rows(connection, entity, watermark):
    columns = ", ".join(f"`{name}`" for name in entity.column_names)
    sql = f"SELECT {columns} FROM `{entity.name}`"
    params = ()
    if watermark is not None:
        sql += f" WHERE `{entity.watermark_column}` > %s"
        params = (watermark,)
    sql += f" ORDER BY `{entity.watermark_column}`, `{entity.key}`"
    cursor = connection.cursor(SSCursor)
    try:
        cursor.execute(sql, params)
        while batch := cursor.fetchmany(FETCH_SIZE):
            for row in batch:
                yield dict(zip(entity.column_names, row))
    finally:
        cursor.close()


def flatten_document(entity, document):
    """Selecciona los campos de raw y guarda el documento completo como JSON."""
    customer = document.get("customer") or {}
    product = document.get("product") or {}
    record = {
        "customer_id": customer.get("customer_id"),
        "product_id": product.get("product_id"),
        "document_json": json_util.dumps(document, ensure_ascii=False),
    }
    if entity.name == "reviews":
        record.update(
            review_id=document.get("review_id"),
            rating=document.get("rating"),
            reviewed_at=document.get("reviewed_at"),
        )
    else:
        device = document.get("device") or {}
        record.update(
            event_id=document.get("event_id"),
            event_type=document.get("event_type"),
            occurred_at=document.get("occurred_at"),
            sales_channel=device.get("sales_channel"),
        )
    return record


def _mongo_rows(database, entity, watermark):
    query = {} if watermark is None else {entity.watermark_column: {"$gt": watermark}}
    cursor = database[entity.name].find(query).sort([(entity.watermark_column, 1), ("_id", 1)]).batch_size(FETCH_SIZE)
    for document in cursor:
        yield flatten_document(entity, document)


def _extract_entity(bucket, batch, entity, rows, log):
    mode = batch["mode"]
    watermark_text = batch["watermarks"].get(entity.label) if mode == "incremental" else None
    extracted_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    metadata = (batch["batch_id"], mode, extracted_at.strftime("%Y-%m-%d %H:%M:%S"), entity.label)

    writer = CsvGzipWriter(entity)
    for record in rows(watermark_from_text(entity, watermark_text)):
        writer.write(record, metadata)
    data = writer.close()

    manifest = publish_batch_entity(bucket, entity, batch["batch_id"], data, {
        "batch_id": batch["batch_id"],
        "source": entity.source,
        "entity": entity.name,
        "raw_table": entity.raw_table,
        "extraction_mode": mode,
        "extracted_at": extracted_at.isoformat(sep=" "),
        "rows": writer.rows,
        "format": "csv.gz",
        "columns": entity.file_columns,
        "watermark_column": entity.watermark_column,
        "watermark_from": watermark_text,
        # Sin filas nuevas, el avance se queda donde estaba.
        "watermark_to": watermark_to_text(entity, writer.max_watermark) or watermark_text,
    })
    log.info("%s: %s filas extraídas (%s, desde %s) -> s3://%s/%s",
             entity.label, writer.rows, mode, watermark_text, bucket, manifest["data_key"])
    return manifest


def extract_source(source, bucket, batch, log=LOGGER):
    """Extrae todas las entidades de una fuente; reutiliza las que ya tienen manifiesto."""
    entities = MYSQL_ENTITIES if source == "mysql" else DOCUMENTDB_ENTITIES
    pending = []
    summary = {}
    for entity in entities:
        existing = read_manifest(bucket, entity, batch["batch_id"])
        if existing is not None:
            log.info("%s: lote %s ya publicado (%s filas); se reutiliza sin consultar la fuente",
                     entity.label, batch["batch_id"], existing["rows"])
            summary[entity.label] = {"rows": existing["rows"], "reused": True}
        else:
            pending.append(entity)
    if not pending:
        return summary

    if source == "mysql":
        connection = mysql_connection()
        try:
            for entity in pending:
                manifest = _extract_entity(
                    bucket, batch, entity, lambda wm, e=entity: _mysql_rows(connection, e, wm), log)
                summary[entity.label] = {"rows": manifest["rows"], "reused": False}
        finally:
            connection.close()
    else:
        client = mongo_client()
        try:
            database = mongo_database(client)
            for entity in pending:
                manifest = _extract_entity(
                    bucket, batch, entity, lambda wm, e=entity: _mongo_rows(database, e, wm), log)
                summary[entity.label] = {"rows": manifest["rows"], "reused": False}
        finally:
            client.close()
    return summary
