"""S3 y Glue: lectura del estado publicado y publicación atómica por partición.

Cómo se publica sin dejar resultados parciales o duplicados en Athena:
1. Los archivos de una partición se escriben en una carpeta nueva por corrida:
       silver/news/published_date=2026-10-21/version=<run_id>/part-00000.parquet
   Athena todavía no los ve: la partición en Glue apunta a la carpeta anterior.
2. Solo cuando silver y gold están escritos, la tarea update_glue_catalog cambia la
   ubicación de cada partición (BatchCreatePartition / BatchUpdatePartition).
3. Repetir la corrida escribe el mismo contenido en la misma carpeta y deja el mismo
   puntero: no hay duplicados. Una partición cuyo contenido no cambió (mismo hash) no se
   reescribe.
"""

import io
import json
import logging
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

from .rss import sha256_json
from .settings import bucket, glue_database

LOGGER = logging.getLogger(__name__)
HIVE_TO_ARROW = {
    "string": pa.string(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("ms"),
    "array<string>": pa.list_(pa.string()),
}


def s3():
    return boto3.client("s3")


def glue():
    return boto3.client("glue")


# ------------------------------------------------------------------ S3 genérico
def put_if_absent(key, body, content_type="application/octet-stream"):
    """Escritura condicional (If-None-Match): False si el objeto ya existía."""
    try:
        s3().put_object(Bucket=bucket(), Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] in ("PreconditionFailed", "412"):
            return False
        raise


def put(key, body, content_type="application/octet-stream"):
    s3().put_object(Bucket=bucket(), Key=key, Body=body, ContentType=content_type)


def get(key):
    try:
        return s3().get_object(Bucket=bucket(), Key=key)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise


def list_keys(prefix):
    paginator = s3().get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket(), Prefix=prefix):
        for item in page.get("Contents", []):
            yield item["Key"]


def delete_prefix(prefix):
    keys = list(list_keys(prefix))
    for start in range(0, len(keys), 1000):
        s3().delete_objects(
            Bucket=bucket(), Delete={"Objects": [{"Key": key} for key in keys[start:start + 1000]], "Quiet": True}
        )


def _json_default(value):
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    raise TypeError(type(value))


def put_json(key, value):
    put(key, json.dumps(value, ensure_ascii=False, default=_json_default).encode("utf-8"), "application/json")


def get_json(key):
    body = get(key)
    return None if body is None else json.loads(body)


# ------------------------------------------------------------------ esquemas de Glue
_TABLE_CACHE = {}


def table_definition(table):
    """Columnas, particiones y ubicación definidas por Terraform en Glue."""
    if table not in _TABLE_CACHE:
        response = glue().get_table(DatabaseName=glue_database(), Name=table)["Table"]
        _TABLE_CACHE[table] = {
            "columns": [(c["Name"], c["Type"]) for c in response["StorageDescriptor"]["Columns"]],
            "partition_keys": [p["Name"] for p in response.get("PartitionKeys", [])],
            "location": response["StorageDescriptor"]["Location"].rstrip("/") + "/",
            "storage_descriptor": response["StorageDescriptor"],
        }
    return _TABLE_CACHE[table]


def arrow_schema(columns):
    return pa.schema([pa.field(name, HIVE_TO_ARROW[kind]) for name, kind in columns])


def rows_to_parquet(rows, columns):
    """Escribe filas con el esquema exacto de Glue (orden y tipos)."""
    names = [name for name, _ in columns]
    table = pa.Table.from_pylist([{name: row.get(name) for name in names} for row in rows], schema=arrow_schema(columns))
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def parquet_to_rows(body):
    return pq.read_table(io.BytesIO(body)).to_pylist()


def key_from_uri(uri):
    return uri.split(f"s3://{bucket()}/", 1)[1]


def content_hash(rows, columns):
    names = [name for name, _ in columns]
    canonical = sorted(json.dumps([row.get(name) for name in names], default=_json_default, ensure_ascii=False)
                       for row in rows)
    return sha256_json(canonical)


# ------------------------------------------------------------------ particiones publicadas
def published_partitions(table):
    """{(valores...): {"location", "parameters"}} de las particiones registradas en Glue."""
    partitions = {}
    paginator = glue().get_paginator("get_partitions")
    for page in paginator.paginate(DatabaseName=glue_database(), TableName=table):
        for partition in page["Partitions"]:
            partitions[tuple(partition["Values"])] = {
                "location": partition["StorageDescriptor"]["Location"],
                "parameters": partition.get("Parameters", {}),
            }
    return partitions


def read_published_rows(table):
    """Filas vigentes de una tabla (lo que Athena ve), con las columnas de partición."""
    definition = table_definition(table)
    rows = []
    for values, partition in published_partitions(table).items():
        prefix = key_from_uri(partition["location"]).rstrip("/") + "/"
        for key in list_keys(prefix):
            if not key.endswith(".parquet"):
                continue
            for row in parquet_to_rows(get(key)):
                row.update(dict(zip(definition["partition_keys"], values)))
                rows.append(row)
    return rows


def partition_path(table, values, run_id, versioned):
    definition = table_definition(table)
    location = definition["location"]
    parts = "/".join(f"{name}={value}" for name, value in zip(definition["partition_keys"], values))
    suffix = f"version={run_id}/" if versioned else ""
    return f"{location}{parts}/{suffix}"


def stage_table(table, rows, run_id, mode):
    """Escribe en S3 las particiones que cambiaron y devuelve el plan de publicación.

    mode="replace": rows es el estado completo; particiones ausentes se eliminan del catálogo.
    mode="append":  rows son solo las particiones de esta corrida (cuarentena, caché).
    """
    definition = table_definition(table)
    keys = definition["partition_keys"]
    grouped = {}
    for row in rows:
        grouped.setdefault(tuple(str(row[key]) for key in keys), []).append(row)

    published = published_partitions(table)
    plan = []
    for values, partition_rows in sorted(grouped.items()):
        digest = content_hash(partition_rows, definition["columns"])
        current = published.get(values)
        if current and current["parameters"].get("content_sha256") == digest:
            continue  # sin cambios: no se reescribe ni se vuelve a publicar
        location = partition_path(table, values, run_id, versioned=(mode == "replace"))
        put(key_from_uri(location) + "part-00000.parquet", rows_to_parquet(partition_rows, definition["columns"]))
        plan.append({
            "table": table, "values": list(values), "location": location, "action": "upsert",
            "content_sha256": digest, "row_count": len(partition_rows),
            "previous_location": current["location"] if current else None,
        })
    if mode == "replace":
        for values, current in published.items():
            if values not in grouped:
                plan.append({"table": table, "values": list(values), "action": "delete",
                             "previous_location": current["location"]})
    LOGGER.info("%s: %s particiones con cambios de %s", table, len(plan), len(grouped))
    return plan


class StalePlanError(RuntimeError):
    """El catálogo cambió después de calcular el plan (otra corrida publicó entre medias)."""


def stale_entries(plan):
    """Entradas del plan cuya partición ya no apunta a donde apuntaba al calcularlo.

    Una entrada ya aplicada por un intento anterior de esta misma corrida no cuenta como conflicto.
    """
    stale = []
    published_by_table = {}
    for entry in plan:
        if entry["table"] not in published_by_table:
            published_by_table[entry["table"]] = published_partitions(entry["table"])
        current = published_by_table[entry["table"]].get(tuple(entry["values"]))
        current_location = current["location"] if current else None
        target = entry.get("location") if entry["action"] == "upsert" else None
        if current_location not in (entry["previous_location"], target):
            stale.append({**entry, "current_location": current_location})
    return stale


def apply_plan(plan, run_id):
    """Cambia los punteros de Glue: aquí los datos nuevos se vuelven visibles en Athena.

    Antes de tocar el catálogo comprueba que ninguna partición cambió desde que se calculó el
    plan. Si cambió, aplicarlo pisaría lo que publicó otra corrida (pérdida de noticias): se
    rechaza completo y la corrida debe recalcularse desde Bronze (reprocess_run_id).
    """
    stale = stale_entries(plan)
    if stale:
        sample = [(e["table"], e["values"]) for e in stale[:3]]
        raise StalePlanError(
            f"{len(stale)} particiones cambiaron después de calcular el plan de {run_id} (p. ej. {sample})"
        )

    by_table = {}
    for entry in plan:
        by_table.setdefault(entry["table"], []).append(entry)

    applied = 0
    for table, entries in by_table.items():
        definition = table_definition(table)
        published = published_partitions(table)

        def partition_input(entry, definition=definition):
            descriptor = dict(definition["storage_descriptor"], Location=entry["location"])
            return {
                "Values": entry["values"],
                "StorageDescriptor": descriptor,
                "Parameters": {"content_sha256": entry["content_sha256"], "run_id": run_id,
                               "row_count": str(entry["row_count"])},
            }

        creates = [partition_input(e) for e in entries if e["action"] == "upsert" and tuple(e["values"]) not in published]
        updates = [e for e in entries if e["action"] == "upsert" and tuple(e["values"]) in published]
        deletes = [{"Values": e["values"]} for e in entries if e["action"] == "delete" and tuple(e["values"]) in published]

        for start in range(0, len(creates), 100):
            response = glue().batch_create_partition(
                DatabaseName=glue_database(), TableName=table, PartitionInputList=creates[start:start + 100])
            _raise_partition_errors(response, table, ignore=("AlreadyExistsException",))
        for start in range(0, len(updates), 100):
            response = glue().batch_update_partition(
                DatabaseName=glue_database(), TableName=table,
                Entries=[{"PartitionValueList": e["values"], "PartitionInput": partition_input(e)}
                         for e in updates[start:start + 100]])
            _raise_partition_errors(response, table)
        for start in range(0, len(deletes), 25):
            response = glue().batch_delete_partition(
                DatabaseName=glue_database(), TableName=table, PartitionsToDelete=deletes[start:start + 25])
            _raise_partition_errors(response, table, ignore=("EntityNotFoundException",))
        applied += len(creates) + len(updates) + len(deletes)
        LOGGER.info("Glue %s: %s creadas, %s actualizadas, %s eliminadas", table, len(creates), len(updates), len(deletes))
    return applied


def _raise_partition_errors(response, table, ignore=()):
    errors = [e for e in response.get("Errors", []) if e["ErrorDetail"]["ErrorCode"] not in ignore]
    if errors:
        raise RuntimeError(f"Glue rechazó particiones de {table}: {errors[:3]}")


def cleanup_old_versions(plan):
    """Borra carpetas de versiones que ya no apunta Glue, conservando la anterior inmediata
    (una consulta de Athena en curso pudo empezar a leerla)."""
    for entry in plan:
        if "/version=" not in entry.get("location", entry.get("previous_location") or ""):
            continue
        keep = {entry.get("location"), entry.get("previous_location")}
        partition_prefix = key_from_uri((entry.get("location") or entry["previous_location"]).split("version=")[0])
        versions = {key.split("version=")[1].split("/")[0] for key in list_keys(partition_prefix) if "version=" in key}
        for version in versions:
            uri = f"s3://{bucket()}/{partition_prefix}version={version}/"
            if uri not in keep:
                delete_prefix(f"{partition_prefix}version={version}/")
