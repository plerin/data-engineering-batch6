from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = "redshift_dev_db")
    return hook.get_conn().cursor()

def summary_nps(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    select_sql = context["params"]["sql"]

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = """DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """.format(schema=schema, table=table)
    sql += select_sql
    cur.execute(sql)

    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{table}".format(schema=schema, table=table))
    cnt = cur.fetchone()[0]
    if cnt == 0:
        raise ValueError("{schema}.{table} didn't have any recode".format(schema=schema, table=table))
    
    try:
        sql = """DROP TABLE IF EXISTS {schema}.{table}; ALTER TABLE {schema}.temp_{table} RENAME TO {table};""".format(schema= schema, table= table)
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("Failed to sql. Complete ROLLBACK!")
        raise AirflowException("")
        # raise

DAG_ID = "Assignment3_week6"
default_args = {
    "catchup" : False,
    "start_date" : datetime(2021, 11, 25),
    "schedule_interval" : "@once"
}

with DAG(DAG_ID, default_args = default_args, tags = ["assignment_week6"]) as dag:
    summary_nps = PythonOperator(
        task_id = "summary_nps",
        python_callable = summary_nps,
        params = {
            "schema" : "plerin152",
            "table" : "nps_summary",
            "sql" : """
                SELECT
                    TO_CHAR(created_at, 'YYYY-MM-DD') as day,
                    ROUND((SUM(CASE WHEN score BETWEEN 9 AND 10 THEN 1 ELSE 0 END) - SUM(CASE WHEN score < 7 THEN 1 ELSE 0 END))::decimal / SUM(1),2) as nps
                    FROM {schema}.{table}
                    GROUP BY day
                    ORDER BY 1;
            """.format(schema="plerin152", table="nps")
        },
        provide_context = True
    )