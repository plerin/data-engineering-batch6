from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.mysql_to_s3 import MySQLToS3Operator
from plugins.s3_to_redshift_operator import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.s3_delete_objects import S3DeleteObjectsOperator
from airflow.models import Variable

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2
import json


DAG_ID = "assignment1_v3_week6"
default_args = {
    "concurrency" : 1,
    "catchup" : False,
    "start_date" : datetime(2021, 11, 27),
    "schedule_interval" : None  # 스케줄링 돌리기 싫을 때 설정
}

with DAG(DAG_ID, default_args=default_args, tags=['assignment_week6']) as dag:
    schema = "plerin152"
    table = "nps"
    s3_bucket = "grepp-data-engineering"
    s3_key = schema + "-" + table

    s3_folder_cleanup = S3DeleteObjectsOperator(
        task_id = "s3_folder_cleanup",
        bucket = s3_bucket,
        keys = s3_key,
        aws_conn_id = "aws_conn_id"
    )

    mysql_to_s3_nps = MySQLToS3Operator(
        task_id = "mysql_to_s3_nps",
        query = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')",
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
        redshift_conn_id = "redshift_dev_db",
        primary_key = "id",
        order_key = "created_at"
    )

    s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps

