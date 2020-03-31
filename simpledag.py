from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime
from datetime import timedelta

import os 
import re

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2018, 9, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': timedelta(seconds=5),
}

dag = DAG(
  dag_id='my_dag', 
  description='Simple DAG',
  default_args=default_args)

jsonpath = "/storage/Study/myCode/airflow-test-task/airflow-test-task/"
infile = "event-data-head.json"
outfile = "testfile.json"

loadcommand = "cat " + jsonpath + outfile + " | docker run -i --rm --link my-clickhouse-server:clickhouse-server yandex/clickhouse-client -n clickhouse-client: clickhouse-client --input_format_skip_unknown_fields=1 --query=\"INSERT INTO default.fromjson FORMAT JSONEachRow\"  --host clickhouse-server"


print(loadcommand)

def filterJSON(line):
    re.sub(":([0-9]+?),", ":\"\1\",", line)
    re.sub(":([0-9\.]+?)}", ":\"\1\"}", line)
    return line

def prepare_data(ds, **kwargs):
  fi = open(jsonpath+infile,"r",encoding="utf8")
  with open(jsonpath+outfile, "w+") as fo:
    for line in map(filterJSON, fi.readlines()):
      fo.write(line)
    return "Prepared"

# def load_to_clickhouse(config, ds, **kwargs):
#     pass

def clean_data(ds, **kwargs):
    os.remove(jsonpath+outfile)
    pass


prepare_data_job = PythonOperator(
  task_id='prepare_data', 
  python_callable=prepare_data, 
#   op_kwargs = {'config' : config},
  provide_context=True,
  dag=dag
)

load_to_clickhouse_job = BashOperator(
  task_id='load_to_clickhouse',
  bash_command=loadcommand,
  dag = dag)


clean_data_job = PythonOperator(
  task_id='clean_data', 
  python_callable=clean_data, 
#   op_kwargs = {'config' : config},
  provide_context=True,
  dag=dag
)


prepare_data_job >> load_to_clickhouse_job >> clean_data_job










