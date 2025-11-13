import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

#Конфигурация DAG
OWNER = 'e.sosnovskiy'
DAG_ID = "raw_from_s3_to_pg"

#Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

#S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

#DuckDB
PASSWORD = Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 7, 27, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_start"].format("YYYY-MM-DD")
    return start_date, end_date

def get_and_transfer_raw_data_to_ods_pg(**context):
    """"""
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")
    con = duckdb.connect()

