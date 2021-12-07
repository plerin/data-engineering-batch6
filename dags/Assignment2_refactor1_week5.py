from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
import requests
import logging
import psycopg2
import json

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def extract(**context):
    logging.info('[START EXTRACT]')

    api_key = context["params"]["api_key"]
    exclude = context["params"]["exclude"] 
    coord = context["params"]["coord"]
    
    url = "https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={exclude}&appid={api_key}&units=metric".format(lat = coord["lat"], lon=coord["lon"], exclude=exclude, api_key=api_key)

    data = requests.get(url)

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
    json_data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract_wheather")
    weather_info = []
    
    for day in json_data['daily']:
        if day == '':
            continue
        col_date = datetime.fromtimestamp(day["dt"]).strftime('%Y-%m-%d')
        col_temp = float(day["temp"]["day"])
        col_temp_min = float(day["temp"]["min"])
        col_temp_max = float(day["temp"]["max"])
        day_info = [col_date,col_temp,col_temp_min,col_temp_max]
        
        weather_info.append(day_info)
        
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
    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform_wheather")
    
    cur = get_Redshift_connection()
    sql = ''
    
    for day in weather_info:
        (col_date, col_temp, col_temp_min, col_temp_max) = day
        sql += f"INSERT INTO {schema}.temp_{table} VALUES ('{col_date}', {col_temp}, {col_temp_min}, {col_temp_max}, '{datetime.now()}');"
    
    sql += "BEGIN; DELETE FROM {schema}.{table};".format(schema=schema, table=table)
    sql += f"""
        INSERT INTO {schema}.{table}
        SELECT date, temp, min_temp, max_temp, created_date
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
            FROM {schema}.temp_{table}) WHERE seq =1;
        END;
    """
    cur.execute(sql)
    logging.info(sql)
    logging.info('[END LOAD]')


dag_assignment_week5 = DAG(
    dag_id = 'assignment_refactor1_week5',
    start_date = datetime(2021, 12, 5),
    catchup = False,
    schedule_interval = '0 0 * * *',
    default_args = {
        'retries' : 1,
        'retry_delay' : timedelta(minutes=5),
        'max_active_runs' : 2
    }
)

extract = PythonOperator(
    task_id = 'extract_wheather',
    python_callable = extract,
    params = {
        'api_key' : Variable.get("open_weather_api_key"),
        'exclude' : 'current,minutely,hourly',
        'coord' : {
            'lat' : 37.551254, 
            'lon' : 126.988409
        }
    },
    provide_context = True,
    dag = dag_assignment_week5
)

transform = PythonOperator(
    task_id = 'transform_wheather',
    python_callable = transform,
    params = {

    },
    provide_context = True,
    dag = dag_assignment_week5
)

load = PythonOperator(
    task_id = 'load_wheather',
    python_callable = load,
    params = {
        'schema' : 'plerin152',
        'table' : 'weather_forecast'
    },
    provide_context = True,
    dag = dag_assignment_week5
)


extract >> transform >> load