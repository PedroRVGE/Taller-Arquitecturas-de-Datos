"""Organización de los lotes en S3.

    s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/part-00000.csv.gz
    s3://<bucket>/raw/<fuente>/<entidad>/batch_id=<id>/_manifest.json

El manifiesto es la marca de "lote completo": se escribe después de los datos. Si existe,
el lote se reutiliza (reproceso sin consultar la fuente) y nunca se sobrescribe. Las
escrituras usan If-None-Match: '*' (escritura condicional de S3), así una carrera entre dos
procesos no reemplaza en silencio un objeto ya publicado.
"""

import hashlib
import json
import re

import boto3
from botocore.exceptions import ClientError

BATCH_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
DATA_FILE = "part-00000.csv.gz"
MANIFEST_FILE = "_manifest.json"


class BatchAlreadyPublished(RuntimeError):
    """Se intentó escribir sobre un lote que ya tiene manifiesto."""


def validate_batch_id(batch_id):
    if not BATCH_ID_PATTERN.match(batch_id or ""):
        raise ValueError(f"batch_id inválido: {batch_id!r}. Use letras, números, '.', '_' o '-' (máximo 64).")
    return batch_id


def s3_client():
    return boto3.client("s3")


def batch_prefix(entity, batch_id):
    return f"raw/{entity.source}/{entity.name}/batch_id={batch_id}/"


def data_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + DATA_FILE


def manifest_key(entity, batch_id):
    return batch_prefix(entity, batch_id) + MANIFEST_FILE


def copy_prefix_uri(bucket, entity, batch_id):
    """Prefijo para COPY: solo los archivos part-*, nunca el manifiesto."""
    return f"s3://{bucket}/{batch_prefix(entity, batch_id)}part-"


def read_manifest(bucket, entity, batch_id, client=None):
    client = client or s3_client()
    try:
        body = client.get_object(Bucket=bucket, Key=manifest_key(entity, batch_id))["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def _put_if_absent(client, bucket, key, body, content_type):
    try:
        client.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type, IfNoneMatch="*")
        return True
    except ClientError as error:
        if error.response["Error"]["Code"] in ("PreconditionFailed", "412"):
            return False
        raise


def publish_batch_entity(bucket, entity, batch_id, data, manifest, client=None):
    """Escribe datos y manifiesto de una entidad dentro de un lote.

    - Con manifiesto previo: el lote está publicado y se rechaza la escritura.
    - Con datos pero sin manifiesto: es un intento interrumpido; se reemplaza (el
      versionado del bucket conserva la versión anterior).
    """
    client = client or s3_client()
    if read_manifest(bucket, entity, batch_id, client) is not None:
        raise BatchAlreadyPublished(f"{entity.label} ya tiene el lote {batch_id} publicado")

    key = data_key(entity, batch_id)
    if not _put_if_absent(client, bucket, key, data, "application/gzip"):
        client.delete_object(Bucket=bucket, Key=key)
        if not _put_if_absent(client, bucket, key, data, "application/gzip"):
            raise BatchAlreadyPublished(f"Otro proceso está escribiendo {key}")

    manifest = dict(manifest, data_key=key, bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
    body = json.dumps(manifest, ensure_ascii=False, indent=2, default=str).encode("utf-8")
    if not _put_if_absent(client, bucket, manifest_key(entity, batch_id), body, "application/json"):
        raise BatchAlreadyPublished(f"Otro proceso publicó el manifiesto de {entity.label} en {batch_id}")
    return manifest


SOURCE_GENERATION_KEY = "control/source_generation.json"


def write_source_generation(bucket, payload, client=None):
    """Registra que las fuentes se regeneraron (dw_source_ops generate). El bucket versionado guarda el historial."""
    client = client or s3_client()
    body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    client.put_object(Bucket=bucket, Key=SOURCE_GENERATION_KEY, Body=body, ContentType="application/json")


def read_source_generation(bucket, client=None):
    client = client or s3_client()
    try:
        body = client.get_object(Bucket=bucket, Key=SOURCE_GENERATION_KEY)["Body"].read()
    except ClientError as error:
        if error.response["Error"]["Code"] in ("NoSuchKey", "404"):
            return None
        raise
    return json.loads(body)


def put_marker_once(bucket, key, body=b"", client=None):
    """Crea un objeto marcador. Devuelve False si ya existía (útil para fallos de una sola vez)."""
    client = client or s3_client()
    return _put_if_absent(client, bucket, key, body, "text/plain")
