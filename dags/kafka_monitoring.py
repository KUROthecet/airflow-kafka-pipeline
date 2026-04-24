import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator

sys.path.append("/opt/airflow")
from plugins.operators.kafka_health_operator import KafkaHealthOperator

def _branch_on_health(ti: TaskInstance) -> str:
    is_healthy = ti.xcom_pull(task_ids="check_kafka_health", key="is_healthy")
    if is_healthy:
        return "health_ok"
    return "send_alert"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="kafka_monitoring",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["kafka", "monitoring", "health"],
) as dag:

    check_kafka_health = KafkaHealthOperator(
        task_id="check_kafka_health",
        kafka_conn_id="kafka_default",
        topic="{{ var.value.get('kafka_topic', 'product_view') }}",
        consumer_group_id="airflow-monitor",
        lag_threshold=10_000,
        timeout=10,
    )

    branch_health = BranchPythonOperator(
        task_id="branch_health",
        python_callable=_branch_on_health,
    )

    health_ok = EmptyOperator(task_id="health_ok")

    alert_email = os.environ.get("ALERT_EMAIL", "admin@example.com")
    send_alert = EmailOperator(
        task_id="send_alert",
        to=alert_email,
        subject="[ALERT] Kafka Cluster Unhealthy",
        html_content="""
        <h3>Kafka Health Check Failed</h3>
        <p>The Kafka cluster health check failed at {{ ts }}.</p>
        <p>Please check the logs for task <code>check_kafka_health</code> in DAG <code>kafka_monitoring</code>.</p>
        """,
    )

    check_kafka_health >> branch_health
    branch_health >> health_ok
    branch_health >> send_alert
