from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.operators.redshift import RedshiftSQLOperator

from datetime import datetime, timedelta
import requests
import logging
import psycopg2
import json

API_URL = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units=metric"
API_KEY = Variable.get("open_weather_api_key")


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def extract(**context):
    logging.info('[START EXTRACT]')
    
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    exclude = context["params"]["exclude"]

    data = requests.get(
        API_URL.format(
            lat = lat, 
            lon = lat, 
            exclude = exclude, 
            api_key = API_KEY
        )
    )

    logging.info('[END EXTRACT]')
    logging.debug(json.dumps(data.json()))
    return (data.json())

def transform(**context):
    '''
    1. load result of extract using xcom
    2. change format for needed info 
    3. return transformed data
    4. loave a log using logging
    '''
    logging.info('[START TRANSFORM]')
    json_data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    weather_info = []
    
    for day in json_data['daily']:
        if day == '':
            continue
        col_date = datetime.fromtimestamp(day["dt"]).strftime('%Y-%m-%d')
        weather_info.append("('{}', {}, {}, {})".format(col_date, day["temp"]["day"], day["temp"]["min"], day["temp"]["max"]))
        
    logging.info('[END TRANSFORM]')
    return (weather_info)

def load(**context):
    '''
    1. insert into data using transaction
    2. define upsert (for promise primary key uniqueness)
    '''
    logging.info('[START LOAD]')

    schema = context["params"]["schema"]
    table = context["params"]["table"]
    execution_date = context["execution_date"]
    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    
    cur = get_Redshift_connection()

    create_sql = f"""DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} (LIKE {schema}.{table} INCLUDING DEFAULTS); INSERT INTO {schema}.temp_{table} SELECT * FROM {schema}.{table};"""
    logging.info(create_sql)

    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    insert_sql = f"INSERT INTO {schema}.temp_{table} VALUES " + ",".join(weather_info)
    logging.info(insert_sql)

    try:
        cur.execute(insert_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise

    # 기존 테이블 대체
    alter_sql = f"""DELETE FROM {schema}.{table};
    INSERT INTO {schema}.{table}
    SELECT date, temp, min_temp, max_temp FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
        FROM {schema}.temp_{table}
    )
    WHERE seq = 1;"""
    logging.info(alter_sql)

    try:
        cur.execute(alter_sql)
        cur.execute("COMMIT;")
    except:
        cur.execute("ROLLBACK;")
        raise

    logging.info('[END LOAD]')


with DAG(
    dag_id = 'assignment2_refactor_week5',
    start_date = datetime(2021, 12, 5),
    catchup = False,
    schedule_interval = '0 0 * * *',
    default_args = {
        'retries' : 1,
        'retry_delay' : timedelta(minutes=5),
        'max_active_runs' : 2
    },
    tags=['assignment']
) as dag:

    extract = PythonOperator(
        task_id = 'extract',
        params = {
            'lat' : 37.551254,
            'lon' : 126.988409,
            'exclude' : 'current,minutely,hourly,alerts'
        },
        provide_context = True,
        python_callable = extract
    )

    transform = PythonOperator(
        task_id = 'transform',
        python_callable = transform,
        provide_context = True
    )

    load = PythonOperator(
        task_id = 'load',
        python_callable = load,
        params = {
            'schema' : 'plerin152',
            'table' : 'weather_forecast'
        },
        provide_context = True
    )


    extract >> transform >> load