from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta, date
import requests
import logging
import psycopg2
import json

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    return hook.get_conn().cursor()

def extract(**context):
    '''
    1. call api using api_key
    2. return f.json()
    3. leave a log using logging 
    '''
    logging.info('[START EXTRACT]')

    api_key = context["params"]["api_key"]
    exclude = 'current,minutely,hourly'
    coord = {
        'lat' : 37.551254, 
        'lon' : 126.988409
    }
    url = f'https://api.openweathermap.org/data/2.5/onecall?lat={coord["lat"]}&lon={coord["lon"]}&exclude={exclude}&appid={api_key}&units=metric'
    data = requests.get(url)
    # json_data = data.json()

    # logging.info(date.fromtimestamp(json_data[0]["dt"]).strftime('%Y-%m-%d'))
    logging.info('[END EXTRACT]')
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
        col_date = date.fromtimestamp(day["dt"]).strftime('%Y-%m-%d')
        col_temp = float(day["temp"]["day"])
        col_temp_min = float(day["temp"]["min"])
        col_temp_max = float(day["temp"]["max"])
        day_info = [col_date,col_temp,col_temp_min,col_temp_max]
        
        weather_info.append(day_info)
        
    logging.info(weather_info)
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

    cur = get_Redshift_connection()
    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform_wheather")

    # sql = f"BEGIN; CREATE TABLE IF NOT EXISTS {schema}.{table} (date date primary key, temp float, min_temp float, max_temp float, created_date timestamp default GETDATE());"
    sql = f"BEGIN;DELETE FROM {schema}.{table};"
    for day in weather_info:
        (col_date, col_temp, col_temp_min, col_temp_max) = day
        print(col_date, col_temp, col_temp_min, col_temp_max)
        sql += f"INSERT INTO {schema}.{table} VALUES ('{col_date}', {col_temp}, {col_temp_min}, {col_temp_max});"
    sql += "END;"
    cur.execute(sql)
    # logging.info(weather_info)
    logging.info('[END LOAD]')


def guaranteePrimary(**context):
    '''
    1. create temp_table using CTAS
    2. INSERT INTO temp_table 
    3. DELETE FROM origin_table
    4. INSERT INTO oritin_table using select temp_table where seq = 1;
    '''
    logging.info("[START GUARANTEEPRIMARY]")

    cur = get_Redshift_connection()
    weather_info = context["task_instance"].xcom_pull(key="return_value", task_ids="transform_wheather")

    sql = "CREATE TABLE {schema}.temp_{table} AS SELECT * FROM {schema}.{table};".format(schema=schema, table=table)
    for day in weather_info:
        (col_date, col_temp, col_temp_min, col_temp_max) = day
        sql += f"INSERT INTO {schema}.{table} VALUES ('{col_date}', {col_temp}, {col_temp_min}, {col_temp_max});"
    
    sql += "BEGIN; DELETE FROM {schema}.{table}".format(schema=schema, table=table)
    sql += f"""
        INSERT INTO {schema}.{table}
        SELECT date, temp, min_temp, max_temp, created_date
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY date ORDER BY created_date DESC) seq
            FROM {schema}.temp_{table}) WHERE seq =1;
    """
    sql = "END;"
    cur.execute(sql)
    logging.info("[END GUARANTEEPRIMARY]")
    pass

dag_assignment_week5 = DAG(
    dag_id = 'weather_forecast',
    start_date = datetime(2021, 11, 5),
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
        'api_key' : Variable.get("open_weather_api_key")
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

guaranteePrimary = PythonOperator(
    task_id = 'guarantee_wheather',
    python_callable = guaranteePrimary,
    params = {
        'schema' : 'plerin152',
        'table' : 'weather_forecast'
    },
    provide_context = True,
    dag = dag_assignment_week5
)   


extract >> transform >> guarantee_wheather