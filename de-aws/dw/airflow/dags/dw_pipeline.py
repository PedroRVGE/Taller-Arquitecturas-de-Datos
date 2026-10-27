"""Pipeline del data warehouse en AWS: RDS + DocumentDB -> S3 -> Redshift -> dbt -> pruebas.

Parámetros al disparar (Trigger DAG w/ config):
    batch_id          lote a procesar; vacío = se deriva de la fecha lógica de la corrida
    mode              auto | full | incremental | reprocess
    simulate_failure  true = falla una sola vez después de cargar raw (demostración)

Recuperación: limpie (Clear) la tarea fallida con sus dependientes, o dispare de nuevo
con el mismo batch_id y mode=reprocess. El lote se lee de S3 sin consultar las fuentes.
"""

from datetime import timedelta

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from extract.airflow_tasks import (
    dbt_task,
    extract_task,
    failure_injection,
    load_task,
    reconcile_task,
    record_result,
    resolve_batch,
    validate_connections,
)

with DAG(
    dag_id="dw_pipeline",
    description="Extrae RDS y DocumentDB a S3, carga Redshift con COPY y transforma con dbt",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Bogota"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,  # dos lotes nunca escriben a la vez en raw ni en los marts
    default_args={
        "retries": 2,  # reintentos acotados: hasta 3 intentos por tarea
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
    },
    params={
        "batch_id": Param("", type="string", description="Vacío = lote nuevo derivado de la fecha lógica"),
        "mode": Param("auto", enum=["auto", "full", "incremental", "reprocess"]),
        "simulate_failure": Param(False, type="boolean", description="Falla una vez tras cargar raw"),
    },
    tags=["dw", "redshift", "dbt"],
) as dag:
    validate = PythonOperator(task_id="validate_connections", python_callable=validate_connections)
    batch = PythonOperator(task_id="resolve_batch", python_callable=resolve_batch)

    with TaskGroup(group_id="extract_to_s3", tooltip="Fuentes -> S3 raw por lote"):
        extract_mysql = PythonOperator(
            task_id="rds_mysql", python_callable=extract_task, op_args=["mysql", batch.output]
        )
        extract_docdb = PythonOperator(
            task_id="documentdb", python_callable=extract_task, op_args=["documentdb", batch.output]
        )

    load = PythonOperator(task_id="load_redshift", python_callable=load_task, op_args=[batch.output])
    inject = PythonOperator(
        task_id="failure_injection", python_callable=failure_injection, op_args=[batch.output], retries=0
    )

    with TaskGroup(group_id="dbt", tooltip="Transformaciones ejecutadas en Redshift"):
        steps = [
            ("deps", "deps", None),
            ("seed", "seed", None),
            ("run_intermediate", "run", "path:models/intermediate"),
            ("run_quarantine", "run", "path:models/quarantine"),
            ("snapshot", "snapshot", None),
            ("run_marts", "run", "path:models/marts"),
            ("test", "test", None),
        ]
        dbt_tasks = [
            PythonOperator(
                task_id=task_id,
                python_callable=dbt_task,
                op_args=[command, selector, batch.output],
                retries=0 if command == "test" else 2,  # una prueba fallida no es transitoria
            )
            for task_id, command, selector in steps
        ]
        for upstream, downstream in zip(dbt_tasks, dbt_tasks[1:]):
            upstream >> downstream

    reconcile = PythonOperator(
        task_id="reconcile", python_callable=reconcile_task, op_args=[batch.output], retries=0
    )
    result = PythonOperator(
        task_id="record_result", python_callable=record_result, trigger_rule=TriggerRule.ALL_DONE, retries=0
    )

    validate >> batch >> [extract_mysql, extract_docdb] >> load >> inject >> dbt_tasks[0]
    dbt_tasks[-1] >> reconcile >> result
