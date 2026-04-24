import json
import os
import glob
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import pandas as pd


def _run_quality_checks(ti: TaskInstance, staging_path: str):
    report = {
        "files_checked": 0,
        "total_rows": 0,
        "null_counts": {},
        "invalid_product_ids": 0,
        "overall_status": "PASSED"
    }
    
    now = datetime.now()
    cutoff = now - timedelta(hours=24)
    csv_files = glob.glob(os.path.join(staging_path, "*.csv"))
    
    files_to_check = [f for f in csv_files if datetime.fromtimestamp(os.path.getmtime(f)) > cutoff]
            
    if not files_to_check:
        report["overall_status"] = "NO_DATA"
        ti.xcom_push(key="quality_report", value=report)
        return "NO_DATA"
        
    all_dfs = []
    for f in files_to_check:
        try:
            df = pd.read_csv(f)
            all_dfs.append(df)
            report["files_checked"] += 1
        except Exception as e:
            print(f"Error reading {f}: {e}")
            
    if not all_dfs:
        report["overall_status"] = "ERROR_READING_FILES"
        ti.xcom_push(key="quality_report", value=report)
        return "ERROR"
        
    combined_df = pd.concat(all_dfs, ignore_index=True)
    report["total_rows"] = int(combined_df.shape[0])
    
    null_counts = combined_df.isnull().sum().to_dict()
    report["null_counts"] = {k: int(v) for k, v in null_counts.items() if v > 0}
    
    id_col = 'id' if 'id' in combined_df.columns else ('product_id' if 'product_id' in combined_df.columns else None)
    
    if id_col:
        invalid_ids = combined_df[combined_df[id_col].isnull() | (combined_df[id_col] <= 0)]
        report["invalid_product_ids"] = int(invalid_ids.shape[0])
        
        if report["invalid_product_ids"] > (report["total_rows"] * 0.05):
            report["overall_status"] = "FAILED"
    else:
        print("Warning: Could not find ID column for business rule check.")
        
    ti.xcom_push(key="quality_report", value=report)
    
    report_dir = "/opt/airflow/staging/reports"
    os.makedirs(report_dir, exist_ok=True)
    report_file = os.path.join(report_dir, f"quality_report_{now.strftime('%Y%m%d_%H%M%S')}.json")
    with open(report_file, "w") as f:
        json.dump(report, f, indent=2)
        
    return report["overall_status"]


def _format_email_body(ti: TaskInstance):
    report = ti.xcom_pull(task_ids="run_quality_checks", key="quality_report")
    if not report:
        return "No report generated."
        
    html = f"<h3>Data Quality Report: {report['overall_status']}</h3>"
    html += "<ul>"
    html += f"<li>Files Checked: {report['files_checked']}</li>"
    html += f"<li>Total Rows: {report['total_rows']}</li>"
    html += f"<li>Invalid Product IDs: {report['invalid_product_ids']}</li>"
    html += "</ul>"
    
    if report['null_counts']:
        html += "<h4>Null Counts:</h4><ul>"
        for col, count in report['null_counts'].items():
            html += f"<li>{col}: {count}</li>"
        html += "</ul>"
        
    return html


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": [os.environ.get("ALERT_EMAIL", "admin@example.com")],
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="quality_check",
    default_args=default_args,
    schedule_interval="30 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["quality", "validation", "reporting"],
) as dag:

    run_quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=_run_quality_checks,
        op_kwargs={"staging_path": "{{ var.value.get('staging_path', '/opt/airflow/staging') }}"},
    )

    format_report = PythonOperator(
        task_id="format_report",
        python_callable=_format_email_body
    )

    send_quality_report = EmailOperator(
        task_id="send_quality_report",
        to=os.environ.get("ALERT_EMAIL", "admin@example.com"),
        subject="Daily Data Quality Report",
        html_content="{{ task_instance.xcom_pull(task_ids='format_report') }}",
    )

    run_quality_checks >> format_report >> send_quality_report
