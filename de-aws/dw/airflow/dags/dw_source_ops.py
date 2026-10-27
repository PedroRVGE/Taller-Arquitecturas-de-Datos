"""Operaciones sobre las fuentes (solo manual). RDS y DocumentDB son privados: estas
acciones se ejecutan desde la EC2, que es la única con acceso de red a ellos.

    generate         crea el esquema y regenera los datos sintéticos (borra lo anterior);
                     el siguiente dw_pipeline en modo auto será full
    append_batch     agrega un lote diario de pedidos, reseñas y eventos (batch_number 1..365)
    simulate_changes cambia ciudad de un cliente y categoría de un producto (SCD tipo 2)
"""

import subprocess
import sys
from datetime import datetime, timezone

import pendulum
from airflow.models.param import Param
from airflow.operators.python import PythonOperator

from airflow import DAG
from extract.connections import bucket_name
from extract.s3_batches import write_source_generation

MODULES = {
    "generate": "extract.sources.generate_data",
    "append_batch": "extract.sources.append_batch",
    "simulate_changes": "extract.sources.simulate_changes",
}


def run_source_action(**context):
    params = context["params"]
    action = params["action"]
    arguments = []
    if action == "generate":
        arguments = ["--seed", str(params["seed"]), "--bad-records", str(params["bad_records"])]
    elif action == "append_batch":
        arguments = ["--batch-id", str(params["batch_number"])]
    elif action == "simulate_changes":
        arguments = ["--changed-at", params["changed_at"], "--city", params["city"],
                     "--department", params["department"], "--category", params["category"]]
    command = [sys.executable, "-m", MODULES[action], *arguments]
    print("Ejecutando:", " ".join(command))
    completed = subprocess.run(command, capture_output=True, text=True, check=False)
    print(completed.stdout)
    if completed.returncode != 0:
        print(completed.stderr)
        raise RuntimeError(f"{action} terminó con código {completed.returncode}")
    if action == "generate":
        # dw_pipeline en modo auto lee esta marca y hace un lote full (ver resolve_batch).
        generated_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
        write_source_generation(bucket_name(), {
            "generated_at": generated_at.isoformat(), "seed": params["seed"], "bad_records": params["bad_records"],
        })
        print(f"Fuentes regeneradas a las {generated_at} UTC: el próximo dw_pipeline en modo auto será full")


with DAG(
    dag_id="dw_source_ops",
    description="Prepara y modifica las fuentes RDS y DocumentDB para las demostraciones",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Bogota"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "action": Param("generate", enum=list(MODULES)),
        "seed": Param(42, type="integer"),
        "bad_records": Param(2, type="integer", minimum=0),
        "batch_number": Param(1, type="integer", minimum=1, maximum=365),
        "changed_at": Param("2025-07-01T00:00:00", type="string"),
        "city": Param("Medellín", type="string"),
        "department": Param("Antioquia", type="string"),
        "category": Param("Hogar", type="string"),
    },
    tags=["dw", "fuentes"],
) as dag:
    PythonOperator(task_id="run_source_action", python_callable=run_source_action)
