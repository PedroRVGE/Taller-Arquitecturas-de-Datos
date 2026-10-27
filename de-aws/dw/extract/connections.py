"""Conexiones a las fuentes y al warehouse. Toda la configuración llega por variables de entorno."""

import logging
import os
from pathlib import Path

import pymysql
import redshift_connector
from pymongo import MongoClient

AWS_CA_BUNDLE = Path("/opt/certs/global-bundle.pem")
LOGGER = logging.getLogger(__name__)


def required(name):
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta la variable de entorno {name}")
    return value


def mysql_connection(cursorclass=None, autocommit=False):
    """RDS for MySQL con TLS verificado contra el bundle de certificados de AWS."""
    options = {
        "host": required("MYSQL_HOST"),
        "port": int(os.getenv("MYSQL_PORT", "3306")),
        "user": required("MYSQL_USER"),
        "password": required("MYSQL_PASSWORD"),
        "database": os.getenv("MYSQL_DATABASE", "ecommerce"),
        "charset": "utf8mb4",
        "autocommit": autocommit,
        "connect_timeout": 15,
    }
    if cursorclass is not None:
        options["cursorclass"] = cursorclass
    if AWS_CA_BUNDLE.exists():
        options["ssl"] = {"ca": str(AWS_CA_BUNDLE)}
    return pymysql.connect(**options)


def mongo_client():
    """DocumentDB: el URI ya incluye tls=true, replicaSet=rs0 y retryWrites=false."""
    return MongoClient(required("MONGODB_URI"), tz_aware=True, serverSelectionTimeoutMS=15000)


def mongo_database(client):
    return client[os.getenv("MONGO_DATABASE", "ecommerce")]


def redshift_connection():
    connection = redshift_connector.connect(
        host=required("REDSHIFT_HOST"),
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        database=required("REDSHIFT_DATABASE"),
        user=required("REDSHIFT_USER"),
        password=required("REDSHIFT_PASSWORD"),
        ssl=True,
        timeout=60,
    )
    connection.autocommit = False
    return connection


def bucket_name():
    return required("DW_BUCKET")


def redshift_iam_role():
    return required("REDSHIFT_IAM_ROLE_ARN")


def aws_region():
    return os.getenv("AWS_DEFAULT_REGION", "us-east-1")
