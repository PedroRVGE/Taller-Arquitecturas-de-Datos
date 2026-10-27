"""Data lake de noticias: RSS -> Bronze -> Silver (+ Comprehend) -> Gold -> Glue -> Athena.

Parámetros (Trigger DAG w/ config):
    run_id                    vacío = se deriva de la fecha lógica
    reprocess_run_id          procesa una corrida existente solo desde Bronze
    rebuild_from_bronze       reconstruye Silver y Gold desde todas las corridas de Bronze
    simulate_change_from_run  copia controlada de un RSS de Bronze con una noticia modificada
    unavailable_source        id de una fuente que se simula caída (corrida parcial)
    simulate_failure          falla una vez entre la escritura de archivos y Glue
"""

import os
from datetime import timedelta

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from airflow import DAG
from newslake import airflow_tasks as tasks
from newslake.settings import WRITER_POOL, sources

with DAG(
    dag_id="newslake_pipeline",
    description="Noticias RSS de El Tiempo en un data lake medallion con Comprehend, Glue y Athena",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Bogota"),
    schedule=os.getenv("PIPELINE_SCHEDULE", "*/30 * * * *"),
    catchup=False,
    max_active_runs=1,  # control de concurrencia: una corrida a la vez escribe el lago
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=5),
    },
    params={
        "run_id": Param("", type="string"),
        "reprocess_run_id": Param("", type="string"),
        "rebuild_from_bronze": Param(False, type="boolean"),
        "simulate_change_from_run": Param("", type="string"),
        "unavailable_source": Param("", type="string"),
        "simulate_failure": Param(False, type="boolean"),
    },
    tags=["dl", "rss", "athena"],
) as dag:
    validate = PythonOperator(task_id="validate_access", python_callable=tasks.validate_access)

    with TaskGroup(group_id="ingest_bronze", tooltip="Una tarea por feed RSS"):
        ingest = [
            PythonOperator(
                task_id=source["id"],
                python_callable=tasks.ingest_source,
                op_args=[source["id"]],
                retries=3,  # solo para errores transitorios; un 404 o XML inválido no se reintenta
            )
            for source in sources()
        ]

    transform = PythonOperator(
        task_id="transform_silver",
        python_callable=tasks.transform_silver,
        # Corre aunque falle una fuente: la corrida queda parcial, no se pierde lo disponible.
        trigger_rule=TriggerRule.ALL_DONE,
    )
    enrich = PythonOperator(task_id="enrich_comprehend", python_callable=tasks.enrich_comprehend, pool=WRITER_POOL)
    publish_silver = PythonOperator(task_id="publish_silver", python_callable=tasks.publish_silver, pool=WRITER_POOL)
    build_gold = PythonOperator(task_id="build_gold", python_callable=tasks.build_gold, pool=WRITER_POOL)
    inject = PythonOperator(task_id="failure_injection", python_callable=tasks.failure_injection, retries=0)
    update_glue = PythonOperator(
        task_id="update_glue_catalog", python_callable=tasks.update_glue_catalog, pool=WRITER_POOL
    )
    validations = PythonOperator(task_id="athena_validations", python_callable=tasks.athena_validations, retries=0)
    record = PythonOperator(
        task_id="record_run_status",
        python_callable=tasks.record_run_status,
        trigger_rule=TriggerRule.ALL_DONE,
        retries=0,
    )

    validate >> ingest >> transform >> enrich >> publish_silver >> build_gold >> inject >> update_glue
    update_glue >> validations >> record
