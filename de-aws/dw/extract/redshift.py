"""Carga de lotes a Redshift y tablas de control del pipeline (esquema ops).

Estados de un lote en ops.batch_runs:
    running -> loaded -> transformed -> success
                      \\-> failed (en cualquier punto, también antes de resolve_batch)

La marca de avance (watermark) de cada entidad está en ops.batch_entities y solo se usa
para la siguiente extracción incremental cuando el lote llegó al menos a "loaded", es
decir, cuando su COPY a raw se confirmó en una transacción.
"""

import logging
from contextlib import closing

from .connections import bucket_name, redshift_connection, redshift_iam_role
from .entities import ALL_ENTITIES, METADATA_COLUMNS
from .s3_batches import copy_prefix_uri, read_manifest

LOGGER = logging.getLogger(__name__)
LOADED_STATES = ("loaded", "transformed", "success")

# "raw" es palabra reservada en Redshift (codificación RAW): siempre va entre comillas.
RAW_SCHEMA = '"raw"'

OPS_DDL = (
    "create schema if not exists ops",
    """create table if not exists ops.batch_runs (
        batch_id varchar(64) not null,
        dag_run_id varchar(256),
        requested_mode varchar(16),
        mode varchar(16),
        status varchar(16),
        attempts integer,
        started_at timestamp,
        loaded_at timestamp,
        finished_at timestamp,
        message varchar(4000),
        primary key (batch_id)
    )""",
    """create table if not exists ops.batch_entities (
        batch_id varchar(64) not null,
        source varchar(32),
        entity varchar(64),
        extraction_mode varchar(16),
        extracted_rows bigint,
        loaded_rows bigint,
        watermark_column varchar(64),
        watermark_from varchar(64),
        watermark_to varchar(64),
        s3_prefix varchar(1024),
        extracted_at timestamp,
        loaded_at timestamp
    )""",
    """create table if not exists ops.reconciliation (
        batch_id varchar(64),
        check_name varchar(128),
        source_value numeric(20,2),
        warehouse_value numeric(20,2),
        passed boolean,
        checked_at timestamp default sysdate
    )""",
)


def raw_table_ddl(entity):
    columns = [f"{name} {kind}" for name, kind in entity.columns]
    columns += [f"{name} {kind}" for name, kind in METADATA_COLUMNS]
    columns.append("_loaded_at timestamp default sysdate")
    return (
        f"create table if not exists {RAW_SCHEMA}.{entity.raw_table} (\n    "
        + ",\n    ".join(columns)
        + f"\n) sortkey (_batch_id, {entity.key})"
    )


def bootstrap(cursor):
    """Crea esquemas y tablas si no existen (idempotente)."""
    cursor.execute(f"create schema if not exists {RAW_SCHEMA}")
    for statement in OPS_DDL:
        cursor.execute(statement)
    for entity in ALL_ENTITIES:
        cursor.execute(raw_table_ddl(entity))


def _fetchone(cursor, sql, params=None):
    cursor.execute(sql, params)
    return cursor.fetchone()


def register_batch(batch_id, dag_run_id, requested_mode, mode):
    """Inserta o reactiva el lote en ops.batch_runs. Devuelve el número de intento."""
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        bootstrap(cursor)
        existing = _fetchone(cursor, "select attempts, mode from ops.batch_runs where batch_id = %s", (batch_id,))
        if existing is None:
            cursor.execute(
                "insert into ops.batch_runs (batch_id, dag_run_id, requested_mode, mode, status, attempts, started_at)"
                " values (%s, %s, %s, %s, 'running', 1, sysdate)",
                (batch_id, dag_run_id, requested_mode, mode),
            )
            attempts = 1
        else:
            attempts = (existing[0] or 0) + 1
            cursor.execute(
                "update ops.batch_runs set dag_run_id = %s, requested_mode = %s, status = 'running',"
                " attempts = %s, finished_at = null, message = null where batch_id = %s",
                (dag_run_id, requested_mode, attempts, batch_id),
            )
        connection.commit()
        return attempts


def record_failed_batch(batch_id, dag_run_id, requested_mode, message):
    """Registra un lote que falló antes de resolve_batch (sin modo ni marca de avance).

    Si el lote ya existía, por ejemplo un reproceso de un lote exitoso, no se toca: cambiar su
    estado le quitaría la marca de avance que ya aportó.
    """
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        bootstrap(cursor)
        if _fetchone(cursor, "select 1 from ops.batch_runs where batch_id = %s", (batch_id,)) is None:
            cursor.execute(
                "insert into ops.batch_runs (batch_id, dag_run_id, requested_mode, status, attempts, started_at,"
                " finished_at, message) values (%s, %s, %s, 'failed', 1, sysdate, sysdate, %s)",
                (batch_id, dag_run_id, requested_mode, message[:4000]),
            )
        connection.commit()


def previous_batch(batch_id):
    """Modo con el que se registró el lote antes (para mantenerlo en un reintento)."""
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        bootstrap(cursor)
        connection.commit()
        row = _fetchone(cursor, "select mode, status from ops.batch_runs where batch_id = %s", (batch_id,))
        return {"mode": row[0], "status": row[1]} if row else None


def has_loaded_batches(exclude_batch_id):
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        row = _fetchone(
            cursor,
            f"select count(*) from ops.batch_runs where status in {LOADED_STATES} and batch_id <> %s",
            (exclude_batch_id,),
        )
        return row[0] > 0


def last_loaded_at(exclude_batch_id):
    """Fecha (UTC) de la última carga confirmada en raw, sin contar el lote indicado."""
    with closing(redshift_connection()) as connection:
        row = _fetchone(
            connection.cursor(),
            f"select max(loaded_at) from ops.batch_runs where status in {LOADED_STATES} and batch_id <> %s",
            (exclude_batch_id,),
        )
        return row[0]


def latest_watermarks(exclude_batch_id):
    """Último watermark confirmado por entidad, tomado de lotes cargados."""
    sql = f"""
        select source || '.' || entity, watermark_to
        from (
            select e.source, e.entity, e.watermark_to,
                   row_number() over (partition by e.source, e.entity order by e.extracted_at desc) as rn
            from ops.batch_entities as e
            join ops.batch_runs as r on r.batch_id = e.batch_id
            where r.status in {LOADED_STATES}
              and e.loaded_at is not null
              and e.batch_id <> %s
        ) as ranked
        where rn = 1
    """
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        cursor.execute(sql, (exclude_batch_id,))
        return {label: watermark for label, watermark in cursor.fetchall()}


def set_status(batch_id, status, message=None):
    timestamp_column = {"loaded": "loaded_at", "success": "finished_at", "failed": "finished_at"}.get(status)
    extra = f", {timestamp_column} = sysdate" if timestamp_column else ""
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"update ops.batch_runs set status = %s, message = %s{extra} where batch_id = %s",
            (status, (message or "")[:4000] or None, batch_id),
        )
        connection.commit()


def _copy_sql(entity, bucket, batch_id, iam_role):
    columns = ", ".join(entity.file_columns)
    return (
        f"copy {RAW_SCHEMA}.{entity.raw_table} ({columns})\n"
        f"from '{copy_prefix_uri(bucket, entity, batch_id)}'\n"
        f"iam_role '{iam_role}'\n"
        "format as csv ignoreheader 1 gzip emptyasnull\n"
        "dateformat 'auto' timeformat 'auto' acceptinvchars"
    )


def _log_load_errors(cursor, log):
    """Muestra el motivo de un COPY fallido (vista de sistema de Redshift Serverless)."""
    try:
        cursor.execute(
            "select table_id, column_name, error_code, error_message, raw_line"
            " from sys_load_error_detail order by start_time desc limit 5"
        )
        for row in cursor.fetchall():
            log.error("Error de carga: %s", row)
    except Exception:  # noqa: BLE001 - diagnóstico opcional
        log.exception("No fue posible leer sys_load_error_detail")


def load_batch(batch_id, log=LOGGER):
    """Carga las 6 entidades del lote en una sola transacción.

    Para cada tabla: DELETE de las filas del lote + COPY desde S3. Si algo falla, ROLLBACK
    y raw queda como estaba. Repetir el lote reemplaza sus filas: no duplica.
    """
    bucket = bucket_name()
    iam_role = redshift_iam_role()
    manifests = {}
    for entity in ALL_ENTITIES:
        manifest = read_manifest(bucket, entity, batch_id)
        if manifest is None:
            raise RuntimeError(f"Falta el manifiesto de {entity.label} para el lote {batch_id}")
        manifests[entity.label] = manifest

    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        try:
            bootstrap(cursor)
            loaded = {}
            for entity in ALL_ENTITIES:
                manifest = manifests[entity.label]
                cursor.execute(f"delete from {RAW_SCHEMA}.{entity.raw_table} where _batch_id = %s", (batch_id,))
                cursor.execute(_copy_sql(entity, bucket, batch_id, iam_role))
                count = _fetchone(
                    cursor, f"select count(*) from {RAW_SCHEMA}.{entity.raw_table} where _batch_id = %s", (batch_id,)
                )[0]
                if count != manifest["rows"]:
                    raise RuntimeError(
                        f"{entity.label}: el manifiesto declara {manifest['rows']} filas y Redshift cargó {count}"
                    )
                loaded[entity.label] = count
                log.info("%s -> %s.%s: %s filas", entity.label, RAW_SCHEMA, entity.raw_table, count)

            cursor.execute("delete from ops.batch_entities where batch_id = %s", (batch_id,))
            for entity in ALL_ENTITIES:
                manifest = manifests[entity.label]
                cursor.execute(
                    "insert into ops.batch_entities (batch_id, source, entity, extraction_mode, extracted_rows,"
                    " loaded_rows, watermark_column, watermark_from, watermark_to, s3_prefix, extracted_at, loaded_at)"
                    " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, sysdate)",
                    (
                        batch_id, entity.source, entity.name, manifest["extraction_mode"], manifest["rows"],
                        loaded[entity.label], manifest["watermark_column"], manifest["watermark_from"],
                        manifest["watermark_to"], copy_prefix_uri(bucket, entity, batch_id), manifest["extracted_at"],
                    ),
                )
            cursor.execute(
                "update ops.batch_runs set status = 'loaded', loaded_at = sysdate where batch_id = %s", (batch_id,)
            )
            connection.commit()
            return loaded
        except Exception:
            connection.rollback()
            _log_load_errors(cursor, log)
            raise


def query_one(sql, params=None):
    with closing(redshift_connection()) as connection:
        return _fetchone(connection.cursor(), sql, params)


def record_reconciliation(batch_id, checks):
    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        cursor.execute("delete from ops.reconciliation where batch_id = %s", (batch_id,))
        for name, source_value, warehouse_value, passed in checks:
            cursor.execute(
                "insert into ops.reconciliation (batch_id, check_name, source_value, warehouse_value, passed)"
                " values (%s, %s, %s, %s, %s)",
                (batch_id, name, source_value, warehouse_value, passed),
            )
        connection.commit()
