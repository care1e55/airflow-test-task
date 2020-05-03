from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from sqlalchemy import create_engine
import re

class JsonToClickhouse(BaseOperator):

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filterJSON(line):
        re.sub(":([0-9]+?),", ":\"\1\",", line)
        re.sub(":([0-9\.]+?)}", ":\"\1\"}", line)
        return line

    def execute(self, context):
        uri = 'clickhouse://default:@localhost:8123'
        engine = create_engine(uri)

        with open("./airflow/files/event-data-head.json", "r") as fi:
            for line in map(self.filterJSON, fi.readlines()):
                sql = r'INSERT INTO default.fromjson  FORMAT JSONEachRow ' + line
                engine.execute(sql)

