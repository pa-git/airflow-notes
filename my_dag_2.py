#!/usr/bin/env python
"""
Performs the ELT sequence from local files to snowflake.

In the following order;
- Put the local file in Snowflake stage
- Copy it into the Table
- Check the integrity of the data
"""

from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

__author__ = "Paco Awissi"
__version__ = "1.0.0"
__status__ = "Prototype"

# Connect to source and generate CSV file
def _get_source_data(source_name, source_object):
    filename = source_object
    return filename

# Load source file into Snowflake table
def _load_source_file():
    #put
    #copy
    #check count
    count = 100
    return count

    # Test load failure
    # exit(1)

# Refresh DIM
def _do_dim():
    return True

# Refresh FACT
def _do_fact():
    return True


with DAG(
    dag_id = "elt_job",
    start_date = datetime(2022, 12, 27),
    schedule_interval = "@daily",
    catchup = False
) as dag:

    get_source_data = PythonOperator (
        task_id = "get_source_data",
        python_callable = _get_source_data,
        op_args = ["my_source_db", "my_source_object"]
    )

    load_source_file = PythonOperator(
        task_id = "load_source_file",
        python_callable = _load_source_file
    )

    do_dim = PythonOperator(
        task_id = "do_dim",
        python_callable = _do_dim
    )

    do_fact = PythonOperator(
        task_id = "do_fact",
        python_callable = _do_fact
    )

    get_source_data >> load_source_file >> do_dim >> do_fact