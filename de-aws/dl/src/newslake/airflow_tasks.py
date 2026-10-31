"""Tareas del DAG newslake_pipeline. El DAG solo declara dependencias; la lógica vive aquí.

Entre tareas viajan archivos pequeños en control/staging/run_id=<id>/ (no visibles para
Athena); XCom solo transporta metadatos.
"""

import json
import logging
import re
from collections import Counter
from datetime import datetime, timezone

import boto3
from airflow.exceptions import AirflowFailException
from botocore.config import Config

from . import athena, bronze, enrich, gold, rss, silver, storage
from .settings import ALL_TABLES, WRITER_POOL, bucket, feeds_config, glue_database, source_by_id, sources

LOGGER = logging.getLogger(__name__)
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
CORE_TASKS = ("validate_access", "transform_silver", "enrich_comprehend", "publish_silver", "build_gold",
              "failure_injection", "update_glue_catalog", "athena_validations")
TIMESTAMP_FIELDS = ("published_at_utc", "analyzed_at", "ingested_at_utc", "detected_at")


class _Prefixed(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{self.extra['prefix']} {msg}", kwargs


def task_log(run, task, source_id=None):
    """Cada línea de log lleva corrida, tarea y fuente."""
    source = f" source={source_id}" if source_id else ""
    return _Prefixed(LOGGER, {"prefix": f"[run={run['run_id']} mode={run['mode']} task={task}{source}]"})


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)


def run_info(context):
    """Identificador y modo de la corrida, deterministas para todas las tareas y reintentos.

    new        consulta los RSS (o reutiliza Bronze si la corrida ya lo tiene)
    reprocess  procesa una corrida existente solo desde Bronze
    rebuild    reconstruye Silver y Gold desde todas las corridas de Bronze
    """
    params = context["params"]
    stamp = context["logical_date"].in_timezone("UTC").strftime("%Y%m%dT%H%M%SZ")
    if params.get("reprocess_run_id"):
        run = {"run_id": params["reprocess_run_id"], "mode": "reprocess"}
    elif params.get("rebuild_from_bronze"):
        run = {"run_id": params.get("run_id") or f"rebuild{stamp}", "mode": "rebuild"}
    else:
        run = {"run_id": params.get("run_id") or f"r{stamp}", "mode": "new"}
    if not RUN_ID_PATTERN.match(run["run_id"]):
        raise AirflowFailException(f"run_id inválido: {run['run_id']!r}")
    return run


# ------------------------------------------------------------------ staging entre tareas
def _staging_key(run_id, name):
    return f"control/staging/run_id={run_id}/{name}.json"


def stage(run_id, name, value):
    storage.put_json(_staging_key(run_id, name), value)


def unstage(run_id, name, default=None):
    value = storage.get_json(_staging_key(run_id, name))
    if value is None:
        if default is not None:
            return default
        raise AirflowFailException(f"Falta el archivo intermedio {name} de la corrida {run_id}")
    if isinstance(value, list):
        for row in value:
            for field in TIMESTAMP_FIELDS:
                if isinstance(row.get(field), str):
                    row[field] = datetime.fromisoformat(row[field])
    return value


# ------------------------------------------------------------------ 1. validación
def ensure_writer_pool():
    from airflow.models.pool import Pool

    if Pool.get_pool(WRITER_POOL) is None:
        Pool.create_or_update_pool(WRITER_POOL, slots=1, description="Escrituras al data lake", include_deferred=False)


def validate_access(**context):
    run = run_info(context)
    log = task_log(run, "validate_access")
    config = feeds_config()
    log.info("Fuentes configuradas: %s", [s["id"] for s in config["sources"]])
    ensure_writer_pool()

    boto3.client("s3").head_bucket(Bucket=bucket())
    glue_client = boto3.client("glue")
    for table in ALL_TABLES:
        glue_client.get_table(DatabaseName=glue_database(), Name=table)
    boto3.client("athena").get_work_group(WorkGroup=athena.athena_workgroup())
    language = boto3.client("comprehend").detect_dominant_language(Text="Prueba de acceso al servicio de noticias")
    log.info("S3, Glue (%s tablas), Athena y Comprehend accesibles (%s)", len(ALL_TABLES),
             language["Languages"][0]["LanguageCode"])
    for name in ("unavailable_source",):
        value = context["params"].get(name)
        if value and value not in [s["id"] for s in config["sources"]]:
            raise AirflowFailException(f"{name}={value!r} no es una fuente de feeds.yml")


# ------------------------------------------------------------------ 2. bronze
def ingest_source(source_id, **context):
    run = run_info(context)
    params = context["params"]
    log = task_log(run, "ingest_bronze", source_id)
    source = source_by_id(source_id)

    if run["mode"] == "rebuild":
        log.info("Reconstrucción: no se consulta el RSS")
        return None
    existing = bronze.existing_record(run["run_id"], source_id)
    if existing and existing.get("status") in ("ok", "no_items"):
        log.info("La corrida ya tiene Bronze (%s); se reprocesa sin consultar el RSS", existing["bronze_key"])
        # Marca para la bitácora: en esta ejecución del DAG la fuente se reprocesó, no se extrajo.
        stage(run["run_id"], f"reused_{source_id}", {"dag_run_id": context["dag_run"].run_id})
        return existing
    if run["mode"] == "reprocess":
        raise AirflowFailException(f"La corrida {run['run_id']} no tiene Bronze válido para {source_id}")

    if params.get("simulate_change_from_run"):
        try:
            return bronze.make_fixture(source, run["run_id"], params["simulate_change_from_run"], log)
        except bronze.PermanentSourceError as error:
            raise AirflowFailException(str(error)) from error
    if params.get("simulate_defects_from_run"):
        try:
            return bronze.make_defects_fixture(source, run["run_id"], params["simulate_defects_from_run"], log)
        except bronze.PermanentSourceError as error:
            raise AirflowFailException(str(error)) from error

    url = None
    if params.get("unavailable_source") == source_id:
        url = source["url"].replace(".xml", "-no-disponible.xml")
        log.warning("Fuente no disponible simulada: %s", url)
    try:
        return bronze.fetch(source, run["run_id"], log, url_override=url)
    except bronze.PermanentSourceError as error:
        raise AirflowFailException(str(error)) from error  # sin reintentos: no es transitorio


# ------------------------------------------------------------------ 3. silver
def _source_for(record):
    try:
        return source_by_id(record["source_id"])
    except KeyError:  # una fuente retirada de feeds.yml sigue siendo reprocesable
        return {"id": record["source_id"], "media": record["media"], "section": record["section"],
                "url": record["url"], "language": feeds_config()["language"]}


def _parsed_sources(records):
    order = {source["id"]: index for index, source in enumerate(sources())}
    parsed = []
    for record in sorted(records, key=lambda r: order.get(r["source_id"], len(order))):
        parsed.append({
            "source": _source_for(record),
            "bronze_key": record["bronze_key"],
            "ingested_at": datetime.fromisoformat(record["ingested_at_utc"]),
            "feed": rss.parse_feed(storage.get(record["bronze_key"])),
        })
    return parsed


def require_validation(context):
    """transform_silver corre con ALL_DONE para tolerar una fuente caída, pero no sin validación."""
    states = {ti.task_id: ti.state for ti in context["dag_run"].get_task_instances()}
    if states.get("validate_access") != "success":
        raise AirflowFailException(
            f"validate_access terminó en {states.get('validate_access')!r}: no se procesa sin validar el acceso"
        )


def transform_silver(**context):
    run = run_info(context)
    log = task_log(run, "transform_silver")
    require_validation(context)
    now = utcnow()

    if run["mode"] == "rebuild":
        state = {}
        records = bronze.runs_in_bronze()
    else:
        state = {row["news_id"]: row for row in storage.read_published_rows("news")}
        records = [bronze.existing_record(run["run_id"], s["id"]) for s in sources()]
        records = [r for r in records if r and r.get("status") in ("ok", "no_items")]
    if not records:
        raise AirflowFailException("Ninguna fuente tiene una respuesta válida en Bronze: no hay nada que procesar")

    runs = {}
    for record in records:
        runs.setdefault(record["run_id"], []).append(record)
    quarantine, counts = [], Counter()
    for source_run_id, run_records in runs.items():
        rows, run_counts = silver.merge_run(state, {"run_id": source_run_id}, _parsed_sources(run_records), now)
        quarantine += rows
        counts += run_counts
        log.info("Corrida %s fusionada: %s", source_run_id, dict(run_counts))

    stage(run["run_id"], "news_merged", list(state.values()))
    stage(run["run_id"], "quarantine", quarantine)
    stage(run["run_id"], "counts", {**{k: counts.get(k, 0) for k in
                                       ("received", "rejected", "duplicates_in_batch", "new", "updated", "repeated")},
                                    "unique_news_current": len(state),
                                    "news_without_valid_date": sum(1 for r in state.values()
                                                                   if r["published_date"] == "unknown")})
    log.info("Silver vigente: %s noticias; conteos de la corrida: %s; cuarentena: %s", len(state), dict(counts),
             len(quarantine))


# ------------------------------------------------------------------ 4. comprehend
def enrich_comprehend(**context):
    run = run_info(context)
    log = task_log(run, "enrich_comprehend")
    config = feeds_config()["comprehend"]
    now = utcnow()
    state = {row["news_id"]: row for row in unstage(run["run_id"], "news_merged")}

    cache = {}
    for record in sorted(storage.read_published_rows("comprehend_results"), key=lambda r: r["analyzed_at"]):
        cache[record["text_hash"]] = record
    previous_attempt = unstage(run["run_id"], "comprehend_new", default=[])
    for record in previous_attempt:  # un reintento no vuelve a pagar análisis ya hechos
        cache[record["text_hash"]] = record

    client = boto3.client("comprehend", config=Config(retries={"max_attempts": 10, "mode": "adaptive"}))
    new_records, stats, errors = enrich.enrich_state(state, cache, client, config, now)
    for record in new_records:
        record.update(analyzed_date=record["analyzed_at"].date().isoformat(), run_id=run["run_id"])
    run_records = previous_attempt + new_records

    entities, phrases = enrich.apply_enrichment(state, cache, set(config["entity_types"]))
    stage(run["run_id"], "comprehend_new", run_records)
    stage(run["run_id"], "news", list(state.values()))
    stage(run["run_id"], "entities", entities)
    stage(run["run_id"], "key_phrases", phrases)
    stage(run["run_id"], "enrich_stats", {**stats, "errors": errors})
    log.info("Comprehend: %s llamadas nuevas, %s textos desde caché, %s errores; %s entidades, %s frases",
             stats["comprehend_calls"], stats["comprehend_cached"], stats["comprehend_errors"], len(entities),
             len(phrases))


# ------------------------------------------------------------------ 5-6. silver y gold
def publish_silver(**context):
    """Escribe en S3 las particiones de Silver que cambiaron (todavía invisibles en Athena)."""
    run = run_info(context)
    log = task_log(run, "publish_silver")
    run_id = run["run_id"]
    plan = []
    plan += storage.stage_table("news", unstage(run_id, "news"), run_id, "replace")
    plan += storage.stage_table("news_entities", unstage(run_id, "entities"), run_id, "replace")
    plan += storage.stage_table("news_key_phrases", unstage(run_id, "key_phrases"), run_id, "replace")
    plan += storage.stage_table("news_quarantine", unstage(run_id, "quarantine"), run_id, "append")
    plan += storage.stage_table("comprehend_results", unstage(run_id, "comprehend_new"), run_id, "append")
    stage(run_id, "plan_silver", plan)
    log.info("Silver escrito: %s particiones por publicar", len(plan))


def build_gold(**context):
    run = run_info(context)
    log = task_log(run, "build_gold")
    run_id = run["run_id"]
    tables = gold.build(unstage(run_id, "news"), unstage(run_id, "entities"), unstage(run_id, "key_phrases"))
    plan = []
    for table, rows in tables.items():
        plan += storage.stage_table(table, rows, run_id, "replace")
        log.info("%s: %s filas", table, len(rows))
    stage(run_id, "plan_gold", plan)
    log.info("Gold escrito: %s particiones por publicar", len(plan))


def failure_injection(**context):
    """Fallo controlado entre la escritura de archivos y la publicación en Glue.

    Demuestra que los archivos nuevos no son visibles en Athena hasta cambiar los punteros.
    Falla una sola vez por corrida; al limpiar la tarea (Clear) el pipeline continúa.
    """
    if not context["params"].get("simulate_failure"):
        return "sin fallo simulado"
    run = run_info(context)
    log = task_log(run, "failure_injection")
    if storage.put_if_absent(f"control/failure_injection/run_id={run['run_id']}", b"fallo controlado", "text/plain"):
        message = f"Fallo controlado en {run['run_id']}: Silver y Gold están escritos pero no publicados en Glue"
        log.error(message)
        raise AirflowFailException(message)
    log.info("Fallo ya simulado para esta corrida; se continúa")
    return "fallo ya simulado; se continúa"


# ------------------------------------------------------------------ 7. glue
def update_glue_catalog(**context):
    run = run_info(context)
    log = task_log(run, "update_glue_catalog")
    plan = unstage(run["run_id"], "plan_silver") + unstage(run["run_id"], "plan_gold")
    try:
        applied = storage.apply_plan(plan, run["run_id"])
    except storage.StalePlanError as error:
        log.error("%s", error)
        raise AirflowFailException(
            f"{error}. Otra corrida publicó mientras esta estaba detenida; aplicar este plan perdería sus noticias. "
            f'Recupere con {{"reprocess_run_id": "{run["run_id"]}"}}, que recalcula desde Bronze sobre el estado actual.'
        ) from error
    storage.cleanup_old_versions(plan)
    stage(run["run_id"], "glue", {"partitions_published": applied})
    log.info("Glue actualizado: %s particiones creadas, actualizadas o eliminadas", applied)


# ------------------------------------------------------------------ 8. athena
def athena_validations(**context):
    run = run_info(context)
    log = task_log(run, "athena_validations")
    results = athena.run_validations(log)
    dates = sorted({row["published_date"] for row in unstage(run["run_id"], "news") if row["published_date"] != "unknown"})
    with_filter = without_filter = None
    if dates:
        with_filter, without_filter = athena.partition_pruning_comparison(dates[-1], log)
    stage(run["run_id"], "validations", {"results": results, "bytes_scanned_with_filter": with_filter,
                                         "bytes_scanned_without_filter": without_filter})
    failed = {name: count for name, count in results.items() if count}
    if failed:
        raise AirflowFailException(f"Validaciones con fallas: {failed}")


# ------------------------------------------------------------------ 9. estado
def record_run_status(**context):
    """Siempre se ejecuta. Clasifica la corrida en complete, partial o failed.

    Cada procesamiento deja su propio archivo en control/run_log/: repetir o recuperar una
    corrida no borra los conteos con los que se procesó la primera vez. mode distingue una
    extracción nueva (new) de un reproceso desde Bronze (reprocess, también cuando se repite
    un run_id que ya tenía Bronze) y de una reconstrucción (rebuild).
    """
    run = run_info(context)
    log = task_log(run, "record_run_status")
    run_id = run["run_id"]
    task_instances = context["dag_run"].get_task_instances()
    failed_tasks = sorted(ti.task_id for ti in task_instances
                          if ti.task_id != context["ti"].task_id and ti.state in ("failed", "upstream_failed"))

    sources_ok, sources_failed = [], []
    if run["mode"] != "rebuild":
        for source in sources():
            record = bronze.existing_record(run_id, source["id"])
            (sources_ok if record and record.get("status") in ("ok", "no_items") else sources_failed).append(source["id"])

    reused = [source_id for source_id in sources_ok
              if (storage.get_json(_staging_key(run_id, f"reused_{source_id}")) or {}).get("dag_run_id")
              == context["dag_run"].run_id]
    mode = "reprocess" if run["mode"] == "new" and sources_ok and reused == sources_ok else run["mode"]

    core_failed = [task for task in failed_tasks if task.split(".")[-1] in CORE_TASKS]
    if core_failed or (run["mode"] != "rebuild" and not sources_ok):
        status = "failed"
    elif sources_failed:
        status = "partial"
    else:
        status = "complete"

    counts = unstage(run_id, "counts", default={})
    enrich_stats = unstage(run_id, "enrich_stats", default={})
    validations = unstage(run_id, "validations", default={})
    record = {
        "run_id": run_id,
        "mode": mode,
        "status": status,
        "started_at": context["dag_run"].start_date.astimezone(timezone.utc).replace(tzinfo=None).isoformat(),
        "finished_at": utcnow().isoformat(),
        "sources_ok": sources_ok,
        "sources_failed": sources_failed,
        "sources_reused": reused,
        "failed_tasks": failed_tasks,
        **{key: counts.get(key) for key in ("received", "rejected", "duplicates_in_batch", "new", "updated",
                                            "repeated", "unique_news_current", "news_without_valid_date")},
        "comprehend_calls": enrich_stats.get("comprehend_calls"),
        "comprehend_cached": enrich_stats.get("comprehend_cached"),
        "comprehend_errors": enrich_stats.get("comprehend_errors"),
        "partitions_published": unstage(run_id, "glue", default={}).get("partitions_published"),
        "bytes_scanned_with_filter": validations.get("bytes_scanned_with_filter"),
        "bytes_scanned_without_filter": validations.get("bytes_scanned_without_filter"),
        "message": f"Fuentes fallidas: {sources_failed}" if sources_failed else "Sin fuentes fallidas",
    }
    processed = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    storage.put(f"control/run_log/{run_id}__{processed}.json",
                (json.dumps(record, ensure_ascii=False) + "\n").encode("utf-8"), "application/json")
    log.info("Corrida %s: %s", status, record)
    if status != "complete":
        raise AirflowFailException(f"Corrida {status}. Fuentes fallidas: {sources_failed}; tareas: {failed_tasks}")
