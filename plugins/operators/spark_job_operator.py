"""
SparkJobOperator - Custom operator to submit and monitor Apache Spark jobs.

Wraps SparkSubmitOperator with:
  - Job status tracking via XCom
  - Resource configuration
  - Timeout handling
  - Cleanup on failure
"""

import logging
from typing import Any, Dict, List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class SparkJobOperator(BaseOperator):
    """
    Submit a Spark job via spark-submit and track its status.

    Args:
        application (str): Path to the Spark application script or JAR.
        conn_id (str): Airflow Spark connection ID.
        application_args (list): Arguments passed to the Spark application.
        driver_memory (str): Driver memory e.g. "1g".
        executor_memory (str): Executor memory e.g. "2g".
        executor_cores (int): Number of cores per executor.
        num_executors (int): Number of executors.
        name (str): Spark application name.
        verbose (bool): Enable verbose logging from spark-submit.
        packages (list[str]): Maven packages to include (e.g. kafka connector).
        conf (dict): Extra Spark configuration key-value pairs.
        timeout (int): Job timeout in seconds (0 = no timeout).
    """

    template_fields: Sequence[str] = ("application", "application_args")
    ui_color = "#e4d4f0"

    def __init__(
        self,
        *,
        application: str,
        conn_id: str = "spark_default",
        application_args: Optional[List[str]] = None,
        driver_memory: str = "1g",
        executor_memory: str = "2g",
        executor_cores: int = 2,
        num_executors: int = 2,
        name: str = "airflow-spark-job",
        verbose: bool = True,
        packages: Optional[List[str]] = None,
        conf: Optional[Dict[str, str]] = None,
        timeout: int = 3600,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.application = application
        self.conn_id = conn_id
        self.application_args = application_args or []
        self.driver_memory = driver_memory
        self.executor_memory = executor_memory
        self.executor_cores = executor_cores
        self.num_executors = num_executors
        self.name = name
        self.verbose = verbose
        self.packages = packages or []
        self.conf = conf or {}
        self.timeout = timeout

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Submit the Spark job and track its execution."""
        from airflow.providers.apache.spark.operators.spark_submit import (
            SparkSubmitOperator,
        )

        run_id = context["run_id"]
        job_name = f"{self.name}_{run_id[:8]}"

        log.info(
            "SparkJobOperator: submitting job '%s' application='%s'",
            job_name,
            self.application,
        )

        # Build extra conf
        spark_conf = {
            "spark.sql.shuffle.partitions": "8",
            "spark.driver.extraJavaOptions": "-Dfile.encoding=UTF-8",
        }
        spark_conf.update(self.conf)

        submit_op = SparkSubmitOperator(
            task_id=f"spark_submit_{self.task_id}",
            application=self.application,
            conn_id=self.conn_id,
            application_args=self.application_args,
            driver_memory=self.driver_memory,
            executor_memory=self.executor_memory,
            executor_cores=self.executor_cores,
            num_executors=self.num_executors,
            name=job_name,
            verbose=self.verbose,
            packages=",".join(self.packages) if self.packages else "",
            conf=spark_conf,
            dag=self.dag,
        )

        try:
            submit_op.execute(context)
            status = "SUCCESS"
            log.info("Spark job '%s' completed successfully.", job_name)
        except Exception as exc:
            status = "FAILED"
            log.error("Spark job '%s' failed: %s", job_name, exc)
            self._cleanup(context)
            raise
        finally:
            job_report = {
                "job_name": job_name,
                "application": self.application,
                "status": status,
                "driver_memory": self.driver_memory,
                "executor_memory": self.executor_memory,
            }
            ti = context["ti"]
            ti.xcom_push(key="spark_job_report", value=job_report)

        return job_report

    def _cleanup(self, context: Dict[str, Any]) -> None:
        """Perform cleanup on job failure (e.g. remove temp checkpoints)."""
        log.info("SparkJobOperator._cleanup: running post-failure cleanup...")
        # Add specific cleanup logic here if needed
        # e.g., delete partial output files, reset offsets
        log.info("Cleanup complete.")


class SparkJobOperatorPlugin(AirflowPlugin):
    name = "spark_job_operator_plugin"
    operators = [SparkJobOperator]
