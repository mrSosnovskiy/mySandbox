import logging
import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "e.sosnovkiy"
DAG_ID = "raw_from_api_to_s3"
LAYER = "raw"
SOURCE = "earthquake"

# S3
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

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
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_start"].format("YYYY-MM-DD")
    return start_date, end_date


def transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")

    con = duckdb.connect()
    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT * 
            FROM read_csv_auto(
                'https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}'
            ) AS res
        ) TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet';
        """
    )
    con.close()
    logging.info(f"✅ Download for date success: {start_date}")


# Определение DAG должно быть на верхнем уровне
with DAG(
        dag_id=DAG_ID,
        schedule_interval="0 5 * * *",
        default_args=args,
        tags=["s3", "raw"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
        doc_md=LONG_DESCRIPTION
) as dag:
    start = EmptyOperator(task_id="start")

    transfer_task = PythonOperator(
        task_id="transfer_api_data_to_s3",
        python_callable=transfer_api_data_to_s3,
    )

    end = EmptyOperator(task_id="end")

    start >> transfer_task >> end