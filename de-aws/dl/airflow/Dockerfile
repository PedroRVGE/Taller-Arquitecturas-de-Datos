FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

# Dependencias del pipeline: lectura segura de XML (defusedxml), Parquet (pyarrow) y AWS (boto3).
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "pyarrow" \
    "defusedxml" \
    "requests" \
    "PyYAML" \
    "boto3" \
    && pip check
