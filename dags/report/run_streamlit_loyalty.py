"""DAG that runs a streamlit app."""

# --------------- #
# PACKAGE IMPORTS #
# --------------- #

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration

# -------------------- #
# Local module imports #
# -------------------- #

from include.global_variables import global_variables as gv

# --- #
# DAG #
# --- #


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=[gv.DS_DUCKDB_IN_CLIENT, gv.DS_DUCKDB_IN_TRANS, gv.DS_DUCKDB_IN_REDE, gv.DS_DUCKDB_IN_FRAUD],
    catchup=False,
    default_args=gv.default_args,
    # this DAG will time out after one hour
    dagrun_timeout=duration(hours=1),
    description="Runs a streamlit app.",
    tags=["reporting", "streamlit"]
)
def run_streamlit_loyalty():

    # run the streamlit app contained in the include folder
    run_streamlit_script = BashOperator(
        task_id="run_streamlit_script",
        # retrieve the command from the global variables file
        bash_command=gv.STREAMLIT_COMMAND,
        # provide the directory to run the bash command in
        cwd="include/streamlit_loyalty_app",
    )

    run_streamlit_script


run_streamlit_loyalty()
