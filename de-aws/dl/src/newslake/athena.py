"""Ejecución de consultas en Athena (workgroup del data lake) y validaciones finales."""

import logging
import time

import boto3

from .settings import SQL_DIR, athena_workgroup, glue_database

LOGGER = logging.getLogger(__name__)


def run_query(sql, timeout_seconds=300):
    client = boto3.client("athena")
    execution_id = client.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": glue_database()},
        WorkGroup=athena_workgroup(),
    )["QueryExecutionId"]

    deadline = time.monotonic() + timeout_seconds
    while True:
        execution = client.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if time.monotonic() > deadline:
            client.stop_query_execution(QueryExecutionId=execution_id)
            raise TimeoutError(f"La consulta {execution_id} superó {timeout_seconds} s")
        time.sleep(1)
    if state != "SUCCEEDED":
        reason = execution["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena {state}: {reason}\n{sql}")

    rows = []
    paginator = client.get_paginator("get_query_results")
    header = None
    for page in paginator.paginate(QueryExecutionId=execution_id):
        for record in page["ResultSet"]["Rows"]:
            values = [cell.get("VarCharValue") for cell in record["Data"]]
            if header is None:
                header = values
                continue
            rows.append(dict(zip(header, values)))
    return {
        "id": execution_id,
        "rows": rows,
        "bytes_scanned": execution["Statistics"].get("DataScannedInBytes", 0),
    }


def validation_queries():
    """Archivos sql/athena/validations/*.sql. Cada uno devuelve una fila con la columna failures."""
    folder = SQL_DIR / "validations"
    return {path.stem: path.read_text(encoding="utf-8") for path in sorted(folder.glob("*.sql"))}


def run_validations(log=LOGGER):
    results = {}
    for name, sql in validation_queries().items():
        result = run_query(sql)
        failures = int(result["rows"][0]["failures"]) if result["rows"] else 0
        results[name] = failures
        log.info("Validación %s: %s fallas (%s bytes escaneados)", name, failures, result["bytes_scanned"])
    return results


def partition_pruning_comparison(published_date, log=LOGGER):
    """Bytes escaneados con y sin filtro de partición sobre la misma tabla."""
    with_filter = run_query(
        f"select count(*) as news, count(distinct section) as sections from news where published_date = '{published_date}'"
    )
    without_filter = run_query(
        f"select count(*) as news, count(distinct section) as sections from news "
        f"where date(published_at_utc) = date '{published_date}'"
    )
    log.info("Con filtro de partición: %s bytes; sin filtro: %s bytes",
             with_filter["bytes_scanned"], without_filter["bytes_scanned"])
    return with_filter["bytes_scanned"], without_filter["bytes_scanned"]
