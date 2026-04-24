"""
DataTransferOperator - Transfers messages from a Kafka topic to a staging CSV file.

Features:
  - Configurable batch size
  - Checkpointing (last-consumed offset saved to XCom)
  - Progress tracking per partition
  - Partial-failure handling (skips bad messages, logs errors)
"""

import csv
import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class DataTransferOperator(BaseOperator):
    """
    Consume messages from a Kafka topic and save them to a staging CSV.

    Args:
        kafka_conn_id (str): Airflow Connection ID for Kafka.
        topic (str): Kafka topic to consume from.
        staging_path (str): Directory path where CSV files are written.
        batch_size (int): Maximum number of messages to consume per run.
        consumer_group_id (str): Kafka consumer group ID.
        timeout_ms (int): Consumer poll timeout in milliseconds.
    """

    template_fields: Sequence[str] = ("topic", "staging_path")
    ui_color = "#d4f0e4"

    def __init__(
        self,
        *,
        kafka_conn_id: str = "kafka_default",
        topic: str = "product_view",
        staging_path: str = "/opt/airflow/staging",
        batch_size: int = 1000,
        consumer_group_id: str = "airflow-transfer",
        timeout_ms: int = 10_000,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.topic = topic
        self.staging_path = staging_path
        self.batch_size = batch_size
        self.consumer_group_id = consumer_group_id
        self.timeout_ms = timeout_ms

    def _ensure_staging_dir(self) -> None:
        os.makedirs(self.staging_path, exist_ok=True)

    def _get_output_path(self, run_id: str) -> str:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.topic}_{timestamp}_{run_id[:8]}.csv"
        return os.path.join(self.staging_path, filename)

    def _parse_message(self, raw_value: bytes) -> Optional[Dict[str, Any]]:
        """Attempt to parse a Kafka message value as JSON."""
        try:
            return json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            log.warning("Failed to parse message: %s — error: %s", raw_value[:100], exc)
            return None

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Consume up to batch_size messages and write to CSV."""
        from plugins.hooks.kafka_hook import KafkaHook

        self._ensure_staging_dir()
        run_id = context["run_id"]
        output_path = self._get_output_path(run_id)

        hook = KafkaHook(kafka_conn_id=self.kafka_conn_id)
        consumer = hook.get_consumer(
            self.topic,
            group_id=self.consumer_group_id,
        )

        records: List[Dict[str, Any]] = []
        errors = 0
        offsets: Dict[str, int] = {}

        log.info(
            "DataTransferOperator: starting consume from topic='%s', batch_size=%d",
            self.topic,
            self.batch_size,
        )

        try:
            count = 0
            for message in consumer:
                parsed = self._parse_message(message.value)
                if parsed is not None:
                    flat = {}
                    for k, v in parsed.items():
                        if isinstance(v, dict):
                            for sub_k, sub_v in v.items():
                                flat[f"{k}_{sub_k}"] = sub_v
                        else:
                            flat[k] = v
                    flat["_kafka_partition"] = message.partition
                    flat["_kafka_offset"] = message.offset
                    flat["_kafka_timestamp"] = message.timestamp
                    records.append(flat)

                    partition_key = f"{self.topic}-{message.partition}"
                    offsets[partition_key] = message.offset
                else:
                    errors += 1

                count += 1
                if count % 100 == 0:
                    log.info("Progress: %d messages consumed so far...", count)

                if count >= self.batch_size:
                    log.info("Reached batch_size=%d. Stopping.", self.batch_size)
                    break
        except Exception as exc:
            log.error("Consumer error: %s", exc)
            raise
        finally:
            consumer.close()

        record_count = len(records)
        if records:
            fieldnames = list(records[0].keys())
            with open(output_path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(records)
            log.info("Written %d records to %s", record_count, output_path)
        else:
            log.warning("No records consumed. Empty CSV will not be created.")

        report = {
            "topic": self.topic,
            "records_transferred": record_count,
            "errors": errors,
            "output_path": output_path if records else None,
            "checkpoints": offsets,
        }

        ti = context["ti"]
        ti.xcom_push(key="transfer_report", value=report)
        ti.xcom_push(key="record_count", value=record_count)
        ti.xcom_push(key="output_path", value=output_path if records else None)

        log.info("DataTransferOperator complete: %s", report)
        return report


class DataTransferOperatorPlugin(AirflowPlugin):
    name = "data_transfer_operator_plugin"
    operators = [DataTransferOperator]
