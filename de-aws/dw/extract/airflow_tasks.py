"""Funciones que ejecutan las tareas del DAG dw_pipeline.

Los archivos de DAG solo declaran tareas y dependencias; la lógica vive aquí para que el
análisis periódico de DAGs no abra conexiones ni ejecute trabajo.
"""

import logging
import socket
from contextlib import closing
from datetime import datetime

from airflow.exceptions import AirflowFailException

from . import redshift
from .connections import bucket_name, mongo_client, mysql_connection, redshift_connection
from .dbt_runner import run_dbt
from .entities import ALL_ENTITIES
from .extractors import extract_source
from .s3_batches import put_marker_once, read_manifest, read_source_generation, s3_client, validate_batch_id

LOGGER = logging.getLogger(__name__)
VALID_MODES = ("auto", "full", "incremental", "reprocess")


def batch_log(batch_id, task):
    """Logger con el lote y la tarea en cada línea: facilita ubicar fallos en los logs."""
    return _PrefixedAdapter(LOGGER, f"[batch={batch_id} task={task}]")


class _PrefixedAdapter(logging.LoggerAdapter):
    def __init__(self, logger, prefix):
        super().__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        return f"{self.prefix} {msg}", kwargs


def default_batch_id(context):
    """Un lote por corrida: se deriva de la fecha lógica, estable entre reintentos."""
    return "b" + context["logical_date"].in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")


# ------------------------------------------------------------------ 1. validación
def validate_connections(**context):
    params = context["params"]
    mode = params.get("mode", "auto")
    if mode not in VALID_MODES:
        raise AirflowFailException(f"mode debe ser uno de {VALID_MODES}, no {mode!r}")
    if params.get("batch_id"):
        validate_batch_id(params["batch_id"])

    log = batch_log(params.get("batch_id") or default_batch_id(context), "validate_connections")
    bucket = bucket_name()
    s3_client().head_bucket(Bucket=bucket)
    log.info("S3 accesible: %s", bucket)

    with closing(mysql_connection()) as connection, connection.cursor() as cursor:
        cursor.execute("select count(*) from information_schema.tables where table_schema = database()")
        log.info("RDS MySQL accesible (%s tablas)", cursor.fetchone()[0])

    client = mongo_client()
    try:
        client.admin.command("ping")
        log.info("DocumentDB accesible")
    finally:
        client.close()

    with closing(redshift_connection()) as connection:
        cursor = connection.cursor()
        cursor.execute("select current_database(), current_user")
        log.info("Redshift accesible: %s", cursor.fetchone())
    log.info("Host de ejecución: %s", socket.gethostname())


# ------------------------------------------------------------------ 2. lote
def resolve_batch(**context):
    """Decide el identificador y el modo del lote, y lo registra en ops.batch_runs.

    - Si las 6 entidades ya tienen manifiesto en S3, el lote se reprocesa desde S3.
    - Un reintento de un lote conserva el modo con el que empezó.
    - auto = full si nunca se cargó un lote o si las fuentes se regeneraron (dw_source_ops
      generate) después de la última carga; incremental en otro caso.
    """
    params = context["params"]
    batch_id = validate_batch_id(params.get("batch_id") or default_batch_id(context))
    requested = params.get("mode", "auto")
    log = batch_log(batch_id, "resolve_batch")
    bucket = bucket_name()

    published = [entity.label for entity in ALL_ENTITIES if read_manifest(bucket, entity, batch_id) is not None]
    previous = redshift.previous_batch(batch_id)

    if len(published) == len(ALL_ENTITIES):
        mode = "reprocess"
    elif requested == "reprocess":
        raise AirflowFailException(
            f"No se puede reprocesar {batch_id}: solo hay manifiesto para {published or 'ninguna entidad'}"
        )
    elif previous and previous["mode"] in ("full", "incremental"):
        mode = previous["mode"]
    elif requested == "auto":
        mode = "incremental" if redshift.has_loaded_batches(batch_id) else "full"
        if mode == "incremental" and sources_regenerated_after_last_load(bucket, batch_id, log):
            mode = "full"
    elif requested == "incremental" and not redshift.has_loaded_batches(batch_id):
        log.warning("No hay lotes cargados previos: la extracción incremental se ejecuta como full")
        mode = "full"
    else:
        mode = requested

    # En reproceso el modo original queda en ops.batch_runs (columna mode) y en los manifiestos.
    stored_mode = previous["mode"] if (mode == "reprocess" and previous) else mode
    watermarks = redshift.latest_watermarks(batch_id) if mode == "incremental" else {}
    attempts = redshift.register_batch(batch_id, context["run_id"], requested, stored_mode)
    log.info("Lote resuelto: modo=%s solicitado=%s intento=%s entidades publicadas=%s watermarks=%s",
             mode, requested, attempts, published, watermarks)
    return {"batch_id": batch_id, "mode": mode, "watermarks": watermarks}


def sources_regenerated_after_last_load(bucket, batch_id, log):
    """True si generate reemplazó las fuentes después de la última carga confirmada.

    Equivale a run_pipeline.sh sin --existing-sources en DE-DW: regenerar borra los lotes
    agregados, así que el siguiente lote debe ser full para que stg_* deje de verlos.
    """
    generation = read_source_generation(bucket)
    loaded_at = redshift.last_loaded_at(batch_id)
    if not generation or loaded_at is None:
        return False
    generated_at = datetime.fromisoformat(generation["generated_at"])
    if generated_at > loaded_at:
        log.info("Fuentes regeneradas el %s (después de la última carga, %s): el lote será full", generated_at, loaded_at)
        return True
    return False


# ------------------------------------------------------------------ 3. extracción
def extract_task(source, batch):
    log = batch_log(batch["batch_id"], f"extract_{source}")
    if batch["mode"] == "reprocess":
        log.info("Reproceso: no se consulta %s, se usan los archivos del lote en S3", source)
    return extract_source(source, bucket_name(), batch, log)


# ------------------------------------------------------------------ 4. carga
def load_task(batch):
    log = batch_log(batch["batch_id"], "load_redshift")
    loaded = redshift.load_batch(batch["batch_id"], log)
    log.info("Carga confirmada en Redshift: %s", loaded)
    return loaded


def failure_injection(batch, **context):
    """Fallo controlado para la demostración: falla una sola vez por lote.

    La primera ejecución crea un marcador en S3 y falla. Al limpiar la tarea en Airflow
    (Clear) o al disparar de nuevo el mismo lote, el marcador existe y la tarea continúa.
    """
    if not context["params"].get("simulate_failure"):
        return "sin fallo simulado"
    key = f"control/failure_injection/batch_id={batch['batch_id']}"
    if put_marker_once(bucket_name(), key, b"fallo controlado"):
        raise AirflowFailException(
            f"Fallo controlado del lote {batch['batch_id']} después de cargar raw. Limpie esta tarea para reanudar."
        )
    return "fallo ya simulado para este lote; se continúa"


# ------------------------------------------------------------------ 5. dbt
def dbt_task(command, selector, batch):
    run_dbt(command, selector, batch["batch_id"])
    if command == "run" and selector == "path:models/marts":
        redshift.set_status(batch["batch_id"], "transformed")


# ------------------------------------------------------------------ 6. conciliación
RECONCILIATION_QUERIES = {
    "sales_subtotal": (
        "select coalesce(sum(i.subtotal), 0) from intermediate.int_mysql__order_items__valid i"
        " join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id",
        "select coalesce(sum(subtotal), 0) from marts.fct_sales",
    ),
    "sales_lines": (
        "select count(*) from intermediate.int_mysql__order_items__valid i"
        " join intermediate.int_mysql__orders__valid o on o.order_id = i.order_id",
        "select count(*) from marts.fct_sales",
    ),
    "reviews": (
        "select count(*) from intermediate.int_mongodb__reviews__valid",
        "select count(*) from marts.fct_reviews",
    ),
    "navigation_events": (
        "select count(*) from intermediate.int_mongodb__navigation_events__valid",
        "select count(*) from marts.fct_navigation_events",
    ),
}


def reconcile_task(batch):
    """Concilia indicadores agregados entre la capa válida y los marts."""
    log = batch_log(batch["batch_id"], "reconcile")
    checks = []
    for name, (source_sql, warehouse_sql) in RECONCILIATION_QUERIES.items():
        source_value = redshift.query_one(source_sql)[0]
        warehouse_value = redshift.query_one(warehouse_sql)[0]
        passed = source_value == warehouse_value
        checks.append((name, source_value, warehouse_value, passed))
        log.info("%s: válidos=%s warehouse=%s %s", name, source_value, warehouse_value, "OK" if passed else "DIFERENCIA")
    redshift.record_reconciliation(batch["batch_id"], checks)
    failed = [name for name, *_, passed in checks if not passed]
    if failed:
        raise AirflowFailException(f"La conciliación falló para {failed}")
    return {name: str(warehouse_value) for name, _, warehouse_value, _ in checks}


# ------------------------------------------------------------------ 7. resultado
def record_result(**context):
    """Se ejecuta siempre (trigger_rule=all_done) y deja el estado final del lote."""
    dag_run = context["dag_run"]
    batch = context["ti"].xcom_pull(task_ids="resolve_batch")
    batch_id = (batch or {}).get("batch_id") or context["params"].get("batch_id") or default_batch_id(context)
    log = batch_log(batch_id, "record_result")

    failed = [
        ti.task_id for ti in dag_run.get_task_instances()
        if ti.task_id != context["ti"].task_id and ti.state in ("failed", "upstream_failed")
    ]
    if batch:
        status = "failed" if failed else "success"
        message = f"Tareas fallidas: {', '.join(failed)}" if failed else "Pipeline completo"
        redshift.set_status(batch_id, status, message)
        log.info("Lote %s -> %s. %s", batch_id, status, message)
    if failed:
        raise AirflowFailException(f"El lote {batch_id} terminó con tareas fallidas: {failed}")
