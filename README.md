# airflow-test-task
Solution for test task for Data engineer.

Airflow ETL-pipeline form file to Clickhouse. 
Solution provided as docker-compose.

DAG:
Load json from mounted volume -> clean and prepare for parsing -> parse and load to clickhouse -> drop log table 

TODO:
 - [ ] use hooks
 - [ ] clickhouse aggregate operator with templating
 - [ ] load config and sql templating
 - [ ] add spark container for aggregating

