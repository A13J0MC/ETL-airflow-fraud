"""DAG that ingests from local csv files into MinIO."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from pendulum import datetime
import io

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import LocalFilesystemToMinIOOperator

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the "start" Dataset has been produced to
    schedule=[gv.DS_START],
    catchup=False,
    default_args=gv.default_args,
    description="Ingests loyalty data from provided csv files to MinIO.",
    tags=["ingestion", "minio"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def in_loyalty_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_client = CreateBucket(
        task_id="create_client_bucket", bucket_name=gv.CLIENT_BUCKET_NAME
    )

    # use the custom LocalCSVToMinIOOperator to read the contents in /include/climate
    # into MinIO. This task uses dynamic task allowing you to add additional files to
    # the folder and reading them in without changing any DAG code
    ingest_client_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_client_data",
        bucket_name=gv.CLIENT_BUCKET_NAME,
        outlets=[gv.DS_CLIENT_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.DATA_GLOBAL_PATH + '/clientes.csv',
                "object_name": 'clientes.csv',
            },
        ]
    )

    create_bucket_transactional = CreateBucket(
        task_id="create_transactional_bucket", bucket_name=gv.TRANS_BUCKET_NAME
    )

    ingest_transactional_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_transactional_data",
        bucket_name=gv.TRANS_BUCKET_NAME,
        outlets=[gv.DS_TRANS_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.DATA_GLOBAL_PATH + '/transacciones.csv',
                "object_name": 'transacciones.csv',
            },
        ]
    )

    create_bucket_redemption = CreateBucket(
        task_id="create_redemption_bucket", bucket_name=gv.REDE_BUCKET_NAME
    )

    ingest_redemption_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_redemption_data",
        bucket_name=gv.REDE_BUCKET_NAME,
        outlets=[gv.DS_REDE_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.DATA_GLOBAL_PATH + '/redenciones.csv',
                "object_name": 'redenciones.csv',
            },
        ]
    )

    create_bucket_fraud = CreateBucket(
        task_id="create_fraud_bucket", bucket_name=gv.FRAUD_BUCKET_NAME
    )

    ingest_fraud_data = LocalFilesystemToMinIOOperator.partial(
        task_id="ingest_fraud_data",
        bucket_name=gv.FRAUD_BUCKET_NAME,
        outlets=[gv.DS_FRAUD_DATA_MINIO],
    ).expand_kwargs(
        [
            {
                "local_file_path": gv.DATA_GLOBAL_PATH + '/eventos_fraude.csv',
                "object_name": 'eventos_fraude.csv',
            },
        ]
    )

    # set dependencies
    create_bucket_client >> ingest_client_data
    create_bucket_transactional >> ingest_transactional_data
    create_bucket_redemption >> ingest_redemption_data
    create_bucket_fraud >> ingest_fraud_data


in_loyalty_data()
