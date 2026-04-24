import os
import glob
import tarfile
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def _archive_files(staging_path: str, archive_path: str, days_to_keep=7):
    days_to_keep = int(days_to_keep)
    os.makedirs(archive_path, exist_ok=True)
    
    now = datetime.now()
    cutoff = now - timedelta(days=days_to_keep)
    
    csv_files = glob.glob(os.path.join(staging_path, "*.csv"))
    files_to_archive = [f for f in csv_files if datetime.fromtimestamp(os.path.getmtime(f)) < cutoff]
            
    if not files_to_archive:
        return 0
        
    archive_name = os.path.join(archive_path, f"archive_{now.strftime('%Y%m%d')}.tar.gz")
    
    with tarfile.open(archive_name, "w:gz") as tar:
        for f in files_to_archive:
            tar.add(f, arcname=os.path.basename(f))
            
    if os.path.exists(archive_name) and os.path.getsize(archive_name) > 0:
        for f in files_to_archive:
            os.remove(f)
    else:
        raise Exception("Archive verification failed.")
        
    return len(files_to_archive)

def _cleanup_old_archives(archive_path: str, retention_days=30):
    retention_days = int(retention_days)
    now = datetime.now()
    cutoff = now - timedelta(days=retention_days)
    
    tar_files = glob.glob(os.path.join(archive_path, "*.tar.gz"))
    deleted_count = 0
    
    for f in tar_files:
        if datetime.fromtimestamp(os.path.getmtime(f)) < cutoff:
            os.remove(f)
            deleted_count += 1
            
    return deleted_count

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="archival",
    default_args=default_args,
    schedule_interval="0 3 * * 0",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cleanup", "archival", "maintenance"],
) as dag:

    archive_staging_data = PythonOperator(
        task_id="archive_staging_data",
        python_callable=_archive_files,
        op_kwargs={
            "staging_path": "{{ var.value.get('staging_path', '/opt/airflow/staging') }}",
            "archive_path": "/opt/airflow/staging/archive",
            "days_to_keep": "{{ var.value.get('retention_days_staging', 7) }}"
        },
    )

    cleanup_old_archives = PythonOperator(
        task_id="cleanup_old_archives",
        python_callable=_cleanup_old_archives,
        op_kwargs={
            "archive_path": "/opt/airflow/staging/archive",
            "retention_days": "{{ var.value.get('retention_days_archive', 30) }}"
        },
    )

    archive_staging_data >> cleanup_old_archives
