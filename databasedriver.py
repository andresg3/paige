import psycopg2
import configparser
from pathlib import Path
from queries import *

config = configparser.ConfigParser()
config.read_file(open(f"{Path(__file__).parents[0]}/config.cfg"))


class DatabaseDriver:
    def __init__(self):
        self._conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DATABASE'].values()))
        self._cur = self._conn.cursor()

    def execute_query(self, query):
        try:
            self._cur.execute(query)
            self._conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self._conn.rollback()
            return 1

    def load_df(self, df):
        # Create a list of tuples from the dataframe values
        tuples = [tuple(x) for x in df.to_numpy()]
        # SQL query to execute
        values = [self._cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s)", tup).decode('utf8') for tup in tuples]
        query = insert_blood_work_table + ",".join(values)
        print('done')
        try:
            self._cur.execute(query, tuples)
            self._conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            self._conn.rollback()
            return 1

    def setup(self):
        self.execute_query(create_lab_schema)
        self.execute_query(create_blood_work_table)