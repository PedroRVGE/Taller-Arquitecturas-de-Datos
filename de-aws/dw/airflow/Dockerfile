FROM apache/airflow:2.11.2-python3.11

ARG AIRFLOW_VERSION=2.11.2
ARG PYTHON_VERSION=3.11

# Certificado de AWS para TLS con DocumentDB (y RDS si se exige TLS).
USER root
RUN mkdir -p /opt/certs \
    && curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem -o /opt/certs/global-bundle.pem \
    && chmod 644 /opt/certs/global-bundle.pem
USER airflow

# Dependencias del pipeline, compatibles con las restricciones de Airflow.
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "apache-airflow==${AIRFLOW_VERSION}" \
    "Faker==37.0.0" \
    "PyMySQL==1.1.2" \
    "pymongo==4.16.0" \
    "redshift-connector" \
    "boto3" \
    && pip check

# dbt en un entorno virtual propio: sus dependencias no chocan con las de Airflow.
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
      "dbt-core==1.9.4" \
      "dbt-redshift==1.9.5" \
    && /home/airflow/dbt-venv/bin/pip check \
    && ln -s /home/airflow/dbt-venv/bin/dbt /home/airflow/.local/bin/dbt
