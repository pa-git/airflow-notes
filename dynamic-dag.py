#!/usr/bin/env python

"""
Performs the ELT workflow from local files to Snowflake
"""

from pendulum import datetime
import os
import json
import typing

from airflow import DAG, AirflowException
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task, task_group

# Operators
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

__author__   = "Paco Awissi"
__version__  = "1.0.0"
__status__   = "Prorotype"

# Setting
functional_area = "adbc"

#######################################################################
#No changes passed this line are required to add a new functional area
#######################################################################

# Get config for current funcitonal area from file
current_dir = os.path.dirname(os.path.abspath(__file__))
config_file_path = os.path.join(current_dir, f"{functional_area}.json")
with open(config_file_path) as json_file:
    config_dict = json.load(json_file)


# Verify source data
@task
def verify_source(source: str) -> str:

    success = True
    print(f"Verify source: {source}")

    if success == True:
        return "Success"
    else:
        raise ValueError(f"Verification for source {source} FAILED")


# Truncate target
@task
def truncate_target(payload: typing.List[str]) -> typing.List[str]:

    success = True
    target = payload[1]
    print(f"Truncate target: {target}")

    if success == True:
        return payload
    else:
        raise ValueError(f"Truncate of target {target} FAILED")


# Load source to target
@task
def load_target(payload: typing.List[str]) -> typing.List[str]:
    
    success = True
    source, target = payload[0], payload[1]
    print(f"Load source {source} into target: {target}")

    if success == True:
        return payload
    else:
        raise ValueError(f"Load of {source} into {target} FAILED")


# Verify target
@task
def verify_target(payload: typing.List[str]) -> typing.List[str]:

    success = True
    source, target = payload[0], payload[1]
    print(f"Verify target: {target}")

    if success == True:
        return payload
    else:
        raise ValueError(f"Verify target {target} FAILED")


# Create source to target sequence
@task_group
def load_sequence(payload: typing.List[str]) -> None:
    verify_target(load_target(truncate_target(payload)))


# Execute Tasks and TaskGroups
with DAG(
    dag_id=f"{functional_area}_workflow",
    tags = ["prototype"],
    start_date=datetime(2023, 1, 8, tz="UTC"),
    catchup=False
) as dag:

    # Execute all dims one after the other
    with TaskGroup(group_id="dim_sequence") as dim_sequence:
        a = []
        for i, v in enumerate(config_dict["dims"]):
            a.append(EmptyOperator(task_id=f"dim_{v}"))
            if i not in [0]:
                a[i-1] >> a[i]

    # Execute all facts in parallel
    with TaskGroup(group_id="fact_sequence") as fact_sequence:
        for v in config_dict["facts"]:
            EmptyOperator(task_id=f"fact_{v}")

    # Create empty operators for grouping
    start_task = EmptyOperator(task_id="Start")
    verify_sources = verify_source.expand(source = ["a","b","c"])
    snapshot_and_metadata = EmptyOperator(task_id="Snapshot_and_Metadata")
    load_end_task = EmptyOperator(task_id="Load_Done")
    dim_end_task = EmptyOperator(task_id="Dim_Done")
    fact_end_task = EmptyOperator(task_id="Fact_Done")

    # Create DAG
    for payload in config_dict["load_list"]:
        this_load_sequence = load_sequence(payload)
        start_task >> verify_sources >> this_load_sequence >> snapshot_and_metadata >> load_end_task >> dim_sequence >> dim_end_task >> fact_sequence >> fact_end_task
