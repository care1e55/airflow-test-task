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
    'start_date': datetime(2019, 4, 1),
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

jsonpath = "/files/"
infile = "event-data-head.json"
outfile = "testfile.json"

loadcommand = "cat " + jsonpath + outfile + " | clickhouse-client --input_format_skip_unknown_fields=1 --query=\"INSERT INTO default.fromjson FORMAT JSONEachRow\"  --host clickhouse"
aggregate_command = "clickhouse-client --query=\"INSERT INTO default.aggjson select * from default.fromjson\"  --host clickhouse"
clean_command = "clickhouse-client --query=\"truncate table default.fromjson\"  --host clickhouse"


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


def aggreagate(ds, **kwargs):
    return "Aggreagated"


def clean(ds, **kwargs):
    os.remove(jsonpath+outfile)
    # os.system(clean_command)
    return "Cleaned"


prepare_data_job = PythonOperator(
  task_id='prepare_data', 
  python_callable=prepare_data, 
  provide_context=True,
  dag=dag
)


load_job = BashOperator(
  task_id='load',
  bash_command=loadcommand,
  dag = dag)


aggregate_job = BashOperator(
  task_id='aggregate',
  bash_command=aggregate_command,
  dag=dag)
  
  
clean_data_job = PythonOperator(
  task_id='clean', 
  python_callable=clean, 
  provide_context=True,
  dag=dag
)

clean_table_job = BashOperator(
  task_id='clean_table',
  bash_command=clean_command,
  dag=dag)


prepare_data_job >> load_job >> aggregate_job >> clean_table_job >> clean_data_job











