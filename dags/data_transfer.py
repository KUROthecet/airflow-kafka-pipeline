import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

sys.path.append("/opt/airflow")
from plugins.sensors.kafka_topic_sensor import KafkaTopicSensor
from plugins.operators.data_transfer_operator import DataTransferOperator

def _validate_transfer(ti: TaskInstance) -> str:
    record_count = ti.xcom_pull(task_ids="transfer_to_staging", key="record_count")
    if record_count and record_count > 0:
        return "trigger_spark_job"
    return "validation_empty"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="data_transfer",
    default_args=default_args,
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kafka", "transfer", "staging"],
) as dag:

    wait_for_messages = KafkaTopicSensor(
        task_id="wait_for_messages",
        kafka_conn_id="kafka_default",
        topic="{{ var.value.get('kafka_topic', 'product_view') }}",
        consumer_group_id="airflow-transfer",
        poke_interval=60,
        timeout=600,
        mode="reschedule",
    )

    transfer_to_staging = DataTransferOperator(
        task_id="transfer_to_staging",
        kafka_conn_id="kafka_default",
        topic="{{ var.value.get('kafka_topic', 'product_view') }}",
        staging_path="{{ var.value.get('staging_path', '/opt/airflow/staging') }}",
        batch_size="{{ var.value.get('batch_size', 5000) | int }}",
        consumer_group_id="airflow-transfer",
    )

    validate_transfer = BranchPythonOperator(
        task_id="validate_transfer",
        python_callable=_validate_transfer,
    )

    trigger_spark_job = TriggerDagRunOperator(
        task_id="trigger_spark_job",
        trigger_dag_id="spark_control",
        reset_dag_run=True,
        wait_for_completion=False,
    )

    validation_empty = EmptyOperator(task_id="validation_empty")

    wait_for_messages >> transfer_to_staging >> validate_transfer
    validate_transfer >> trigger_spark_job
    validate_transfer >> validation_empty
