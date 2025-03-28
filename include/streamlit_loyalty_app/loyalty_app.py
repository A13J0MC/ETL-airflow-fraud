# --------------- #
# PACKAGE IMPORTS #
# --------------- #

import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import date
import altair as alt
import json

# --------- #
# VARIABLES #
# --------- #

duck_db_instance_name = (
    "dwh"  # when changing this value also change the db name in .env
)


# retrieving data
def client_data(db=f"/usr/local/airflow/{duck_db_instance_name}"):
    """Function to query a local DuckDB data processed through
    the data pipeline."""

    # Query duckdb database `dwh`
    cursor = duckdb.connect(db)

    # get local weather data
    client_data = cursor.execute(
        """SELECT *
        FROM clientes_table
        LIMIT 10;"""
    ).fetchall()

    client_data_col_names = cursor.execute(
        f"""SELECT column_name from information_schema.columns where table_name = 'clientes_table';"""
    ).fetchall()

    cursor.close()

    df = pd.DataFrame(
        client_data, columns=[x[0] for x in client_data_col_names]
    )

    return df


data = client_data()

st.write(data)

st.button("Re-run")
