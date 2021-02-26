import psycopg2
from queries import create_lab_schema, create_blood_work_table


class DatabaseDriver:
    def __init__(self):
        self._conn = psycopg2.connect("host=127.0.0.1 dbname=postgres user=postgres password=postgres")
        self._cur = self._conn.cursor()

    def execute_query(self, query):
        self._cur.execute(query)

    def setup(self):
        self.execute_query(create_lab_schema)
        self.execute_query(create_blood_work_table)
        self._conn.commit()