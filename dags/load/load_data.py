"""DAG that loads loyalty data from MinIO to DuckDB."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag, task
from pendulum import datetime, parse
import duckdb
import os
import json

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv
from include.custom_task_groups.create_bucket import CreateBucket
from include.custom_operators.minio import (
    MinIOListOperator,
    MinIOCopyObjectOperator,
    MinIODeleteObjectsOperator,
)

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    # this DAG runs as soon as the climate and weather data is ready in MinIO
    schedule=[gv.DS_CLIENT_DATA_MINIO, gv.DS_TRANS_DATA_MINIO, gv.DS_REDE_DATA_MINIO, gv.DS_FRAUD_DATA_MINIO],
    catchup=False,
    default_args=gv.default_args,
    description="Loads loyalty data from MinIO to DuckDB.",
    tags=["load", "minio", "duckdb"],
    # render Jinja templates as native objects (e.g. dictionary) instead of strings
    render_template_as_native_obj=True,
)
def load_loyalty_data():

    # create an instance of the CreateBucket task group consisting of 5 tasks
    create_bucket_tg = CreateBucket(
        task_id="create_archive_bucket", bucket_name=gv.ARCHIVE_BUCKET_NAME
    )

    list_files_client_bucket = MinIOListOperator(
        task_id="list_files_client_bucket", bucket_name=gv.CLIENT_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_CLIENT], pool="duckdb")
    def load_client_data(obj):
        """Loads content of one fileobject in the MinIO bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.CLIENT_BUCKET_NAME, obj, file_path=obj)

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)

    list_files_trans_bucket = MinIOListOperator(
        task_id="list_files_trans_bucket", bucket_name=gv.TRANS_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_TRANS], pool="duckdb")
    def load_trans_data(obj):
        """Loads content of one fileobject in the MinIO bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.TRANS_BUCKET_NAME, obj, file_path=obj)

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)

    list_files_rede_bucket = MinIOListOperator(
        task_id="list_files_rede_bucket", bucket_name=gv.REDE_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_REDE], pool="duckdb")
    def load_rede_data(obj):
        """Loads content of one fileobject in the MinIO bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.REDE_BUCKET_NAME, obj, file_path=obj)

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)

    list_files_fraud_bucket = MinIOListOperator(
        task_id="list_files_fraud_bucket", bucket_name=gv.FRAUD_BUCKET_NAME
    )

    @task(outlets=[gv.DS_DUCKDB_IN_FRAUD], pool="duckdb")
    def load_fraud_data(obj):
        """Loads content of one fileobject in the MinIO bucket
        to DuckDB."""

        # get the object from MinIO and save as a local tmp csv file
        minio_client = gv.get_minio_client()
        minio_client.fget_object(gv.FRAUD_BUCKET_NAME, obj, file_path=obj)

        # derive table name from object name
        table_name = obj.split(".")[0] + "_table"

        # use read_csv_auto to load data to duckdb
        cursor = duckdb.connect(gv.DUCKDB_INSTANCE_NAME)
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS {table_name} AS
            SELECT * FROM read_csv_auto('{obj}');"""
        )
        cursor.commit()
        cursor.close()

        # delete local tmp csv file
        os.remove(obj)

    @task
    def get_copy_args(obj_list_client, obj_list_trans, obj_list_rede, obj_list_fraud):
        """Return tuples with bucket names and bucket contents."""

        return [
            {
                "source_bucket_name": gv.CLIENT_BUCKET_NAME,
                "source_object_names": obj_list_client,
                "dest_object_names": obj_list_client,
            },
            {
                "source_bucket_name": gv.TRANS_BUCKET_NAME,
                "source_object_names": obj_list_trans,
                "dest_object_names": obj_list_trans,
            },
            {
                "source_bucket_name": gv.REDE_BUCKET_NAME,
                "source_object_names": obj_list_rede,
                "dest_object_names": obj_list_rede,
            },
            {
                "source_bucket_name": gv.FRAUD_BUCKET_NAME,
                "source_object_names": obj_list_fraud,
                "dest_object_names": obj_list_fraud,
            },
        ]

    copy_objects_to_archive = MinIOCopyObjectOperator.partial(
        task_id="copy_objects_to_archive",
        dest_bucket_name=gv.ARCHIVE_BUCKET_NAME,
    ).expand_kwargs(
        get_copy_args(
            list_files_client_bucket.output, list_files_trans_bucket.output,
            list_files_rede_bucket.output, list_files_fraud_bucket.output
        )
    )

    @task
    def get_deletion_args(obj_list_client, obj_list_trans, obj_list_rede, obj_list_fraud):
        """Return tuples with bucket names and bucket contents."""

        return [
            {"bucket_name": gv.CLIENT_BUCKET_NAME, "object_names": obj_list_client},
            {"bucket_name": gv.TRANS_BUCKET_NAME, "object_names": obj_list_trans},
            {"bucket_name": gv.REDE_BUCKET_NAME, "object_names": obj_list_rede},
            {"bucket_name": gv.FRAUD_BUCKET_NAME, "object_names": obj_list_fraud},
        ]

    delete_objects = MinIODeleteObjectsOperator.partial(
        task_id="delete_objects",
    ).expand_kwargs(
        get_deletion_args(
            list_files_client_bucket.output, list_files_trans_bucket.output,
            list_files_rede_bucket.output, list_files_fraud_bucket.output
        )
    )

    # set dependencies

    client_data = load_client_data.expand(obj=list_files_client_bucket.output)
    trans_data = load_trans_data.expand(obj=list_files_trans_bucket.output)
    rede_data = load_rede_data.expand(obj=list_files_rede_bucket.output)
    fraud_data = load_fraud_data.expand(obj=list_files_fraud_bucket.output)

    archive_bucket = create_bucket_tg
    [client_data, trans_data, rede_data, fraud_data] >> archive_bucket
    (archive_bucket >> [copy_objects_to_archive] >> delete_objects)

    archive_bucket


load_loyalty_data()
