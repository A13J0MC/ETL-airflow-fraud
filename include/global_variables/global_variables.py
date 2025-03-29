# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow import Dataset
import logging
import os
from minio import Minio
from pendulum import duration
import json

# -------------------- #
# Enter your own info! #
# -------------------- #

MY_NAME = "alejandro.mendoza"

# ----------------------- #
# Configuration variables #
# ----------------------- #

# MinIO connection config
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_IP = "host.docker.internal:9000"
CLIENT_BUCKET_NAME = 'client'
TRANS_BUCKET_NAME = 'transaction'
REDE_BUCKET_NAME = 'redemption'
FRAUD_BUCKET_NAME = 'fraud'
ARCHIVE_BUCKET_NAME = "archive"

# Source file path climate data
DATA_GLOBAL_PATH = f"{os.environ['AIRFLOW_HOME']}/include/loyalty_data"

# Datasets
DS_CLIENT_DATA_MINIO = Dataset(f"minio://{CLIENT_BUCKET_NAME}")
DS_TRANS_DATA_MINIO = Dataset(f"minio://{TRANS_BUCKET_NAME}")
DS_REDE_DATA_MINIO = Dataset(f"minio://{REDE_BUCKET_NAME}")
DS_FRAUD_DATA_MINIO = Dataset(f"minio://{FRAUD_BUCKET_NAME}")
DS_DUCKDB_IN_WEATHER = Dataset("duckdb://in_weather")
DS_DUCKDB_IN_CLIMATE = Dataset("duckdb://in_climate")
DS_DUCKDB_IN_CLIENT = Dataset("duckdb://in_client")
DS_DUCKDB_IN_TRANS = Dataset("duckdb://in_transaction")
DS_DUCKDB_IN_REDE = Dataset("duckdb://in_redemption")
DS_DUCKDB_IN_FRAUD = Dataset("duckdb://in_fraud")
DS_DUCKDB_REPORTING = Dataset("duckdb://reporting")
DS_START = Dataset("start")

# DuckDB config
DUCKDB_INSTANCE_NAME = json.loads(os.environ["AIRFLOW_CONN_DUCKDB_DEFAULT"])["host"]
CONN_ID_DUCKDB = "duckdb_default"

# get Airflow task logger
task_log = logging.getLogger("airflow.task")

# DAG default arguments
default_args = {
    "owner": MY_NAME,
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": duration(minutes=5),
}


# utility functions
def get_minio_client():
    client = Minio(MINIO_IP, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, secure=False)

    return client


# command to run streamlit app within codespaces/docker
# modifications are necessary to support double-port-forwarding
STREAMLIT_COMMAND = "streamlit run loyalty_app.py --server.enableWebsocketCompression=false --server.enableCORS=false"
