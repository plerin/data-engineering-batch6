from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
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

def execSQL(**context):
    schema = context["params"]["schema"]
    table = context["params"]["table"]
    select_sql = context["params"]["sql"]

    logging.info(schema)
    logging.info(table)
    logging.debug(select_sql)

    cur = get_Redshift_connection()

    sql = """DROP TABLE IF EXISTS {schema}.temp_{table};CREATE TABLE {schema}.temp_{table} AS """.format(schema=schema, table=table)
    sql += select_sql
    cur.execute(sql)

    # check the table empty
    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{table}".format(schema=schema, table=table))
    count = cur.fetchone()[0]   # sql 결과 값 중 한 줄을 읽고 커서는 다음으로 이동 ++ tuple형태로 리턴해주니까 [0]으로 값 선택
    if count == 0:
        raise ValueError("{schema}.{table} didn't have any record".format(schema=schema, table=table))
    
    try:
        sql = """DROP TABLE IF EXISTS {schema}.{table}; ALTER TABLE {schema}.temp_{table} RENAME TO {table};""".format(schema=schema, table=table)
        sql += "COMMIT;"
        logging.info(sql)
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK;")
        logging.error("Failed to sql. Completed ROLLBACK!")
        raise AirflowException("")


DAG_ID = "summary_week6"
default_args = {
    "concurrency" : 1,
    "catchup" : False,
    "start_date" : datetime(2021, 11, 27),
    "schedule_interval" : "@once"  # 스케줄링 돌리기 싫을 때 설정
}

with DAG(DAG_ID, default_args=default_args, tags=["assignment_week6"]) as dag:
    execsql = PythonOperator(
        task_id = "execsql",
        python_callable = execSQL,
        params = {
            "schema" : "plerin152",
            "table" : "channel_summary",
            "sql" : """
            SELECT
                DISTINCT t1.userid,
                FIRST_VALUE(t1.channel) OVER(PARTITION BY t1.userid ORDER BY t2.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS First_Channel,
                LAST_VALUE(t1.channel) OVER(PARTITION BY t1.userid ORDER BY t2.ts ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS Last_Channel
                FROM raw_data.user_session_channel t1
                LEFT JOIN raw_data.session_timestamp t2 ON t1.sessionid = t2.sessionid;
            """
        },
        provide_context = True
    )