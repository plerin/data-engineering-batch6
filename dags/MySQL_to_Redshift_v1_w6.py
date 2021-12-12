from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


DAG_ID = "assignment1_v1_week6"
default_args = {
    "concurrency" : 1,
    "catchup" : False,
    "start_date" : datetime(2021, 11, 27),
    "schedule_interval" : None  # 스케줄링 돌리기 싫을 때 설정
}

with DAG(DAG_ID, default_args=default_args, tags=['assignment']) as dag:
    schema = "plerin152"
    table = "nps"
    s3_bucket = "grepp-data-engineering"
    s3_key = schema + "-" + table

    mysql_to_s3_nps = MySQLToS3Operator(
        task_id = "mysql_to_s3_nps",
        query = "SELECT * FROM prod.nps",
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        mysql_conn_id = "mysql_conn_id",
        aws_conn_id = "aws_conn_id",
        verify = False
    )

    s3_to_redshift_nps = S3ToRedshiftOperator(
        task_id = "s3_to_redshift_nps",
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        schema = schema,
        table = table,
        copy_options = ["csv"],
        redshift_conn_id = "redshift_dev_db"
    )
    s3_to_redshift_nps

