from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# noinspection PyUnresolvedReferences
from jsontoclickhouse import JsonToClickhouseOperator

from datetime import datetime
from datetime import timedelta

import os
import re

file_names = {
    "head": "event-data-head.json",
    "full": "event-data.json"
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 4, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}


def clean(ds, **op_kwargs):
    os.remove(op_kwargs["jsonpath"] + op_kwargs["outfile"])
    # os.system(clean_command)
    return "Cleaned"


def create_dag(file_name, dag_id):
    jsonpath = "/files/"
    infile = str(file_name)
    outfile = "testfile.json"

    aggregate_command = "clickhouse-client --query=\"INSERT INTO default.aggjson select * from default.fromjson\"  --host clickhouse"
    clean_command = "clickhouse-client --query=\"truncate table default.fromjson\"  --host clickhouse"

    dag = DAG(
        dag_id=dag_id,
        description='generated_DAG_' + str(file_name),
        default_args=default_args)

    with dag:

        load_job = JsonToClickhouseOperator(
            task_id='load',
            params={
                "host": "clickhouse",
                "schema": "default",
                "table": "fromjson",
                "filepath": "/files",
                "jsonname": "event-data-head.json",
            },
            dag=dag
        )

        aggregate_job = BashOperator(
            task_id='aggregate',
            bash_command=aggregate_command,
            dag=dag)

        clean_data_job = PythonOperator(
            task_id='clean',
            python_callable=clean,
            provide_context=True,
            op_kwargs={"jsonpath": jsonpath, "outfile": outfile, "infile": infile},
            dag=dag
        )

        clean_table_job = BashOperator(
            task_id='clean_table',
            bash_command=clean_command,
            dag=dag)

    load_job >> aggregate_job >> clean_table_job >> clean_data_job

    return dag


for file_name in file_names.values():
    dag_id = 'my_generated_DAG-' + str(file_name)
    globals()[dag_id] = create_dag(file_name, dag_id)
