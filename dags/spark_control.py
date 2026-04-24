import os
import sys
from datetime import datetime, timedelta

from airflow import DAG

sys.path.append("/opt/airflow")
from plugins.operators.spark_job_operator import SparkJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL", "admin@example.com")],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_control",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "processing"],
) as dag:

    process_staging_data = SparkJobOperator(
        task_id="process_staging_data",
        application="/opt/airflow/dags/include/spark_jobs/process_product_view.py",
        conn_id="spark_default",
        application_args=["--input-dir", "{{ var.value.get('staging_path', '/opt/airflow/staging') }}"],
        driver_memory="1g",
        executor_memory="2g",
        executor_cores=2,
        num_executors=2,
        name="process_product_view",
        packages=["org.postgresql:postgresql:42.6.0"],
    )
