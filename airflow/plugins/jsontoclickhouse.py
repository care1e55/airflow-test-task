from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

from sqlalchemy import create_engine
import re

class JsonToClickhouseOperator(BaseOperator):

    template_fields = ['host', 'schema', 'table', 'filepath', 'jsonname']

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filterJSON(self, line):
        re.sub(":([0-9]+?),", ":\"\1\",", line)
        re.sub(":([0-9\.]+?)}", ":\"\1\"}", line)
        return line

    def execute(self, context):
        uri = 'clickhouse://default:@clickhouse:8123'
        engine = create_engine(uri)

        with open("/files/event-data-head.json", "r") as fi:
            for line in map(self.filterJSON, fi.readlines()):
                sql = r'INSERT INTO default.fromjson  FORMAT JSONEachRow ' + line
                engine.execute(sql)

