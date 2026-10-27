"""Ejecuta dbt desde Airflow. El SQL que dbt genera se ejecuta en Redshift, no en la EC2."""

import json
import logging
import os
import subprocess

DBT_EXECUTABLE = os.getenv("DBT_EXECUTABLE", "/home/airflow/dbt-venv/bin/dbt")
DBT_PROJECT = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/src/dbt")
LOGGER = logging.getLogger(__name__)


def run_dbt(command, selector=None, batch_id=None, extra_args=()):
    arguments = [DBT_EXECUTABLE, command, "--project-dir", DBT_PROJECT, "--profiles-dir", DBT_PROJECT]
    if selector:
        arguments += ["--select", selector]
    if batch_id and command != "deps":
        arguments += ["--vars", json.dumps({"batch_id": batch_id})]
    arguments += list(extra_args)

    LOGGER.info("[batch=%s] dbt: %s", batch_id, " ".join(arguments))
    completed = subprocess.run(arguments, capture_output=True, text=True, check=False)
    for line in completed.stdout.splitlines():
        LOGGER.info("[batch=%s] %s", batch_id, line)
    for line in completed.stderr.splitlines():
        LOGGER.warning("[batch=%s] %s", batch_id, line)
    if completed.returncode != 0:
        raise RuntimeError(f"dbt {command} falló con código {completed.returncode} (lote {batch_id})")
