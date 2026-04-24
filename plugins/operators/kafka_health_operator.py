

import logging
import socket
from contextlib import closing
from typing import Any, Dict, List, Optional, Sequence

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

class KafkaHealthOperator(BaseOperator):

    template_fields: Sequence[str] = ("topic",)
    ui_color = "#f0e4d4"

    def __init__(
        self,
        *,
        kafka_conn_id: str = "kafka_default",
        brokers: Optional[List[str]] = None,
        topic: str = "product_view",
        consumer_group_id: str = "airflow-monitor",
        lag_threshold: int = 10_000,
        timeout: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.brokers = brokers or []
        self.topic = topic
        self.consumer_group_id = consumer_group_id
        self.lag_threshold = lag_threshold
        self.timeout = timeout

    def _check_tcp(self, host: str, port: int) -> bool:
        
        try:
            with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                sock.settimeout(self.timeout)
                result = sock.connect_ex((host, port))
                return result == 0
        except Exception as exc:
            log.warning("TCP check failed for %s:%s — %s", host, port, exc)
            return False

    def _parse_brokers(self, brokers_str: str) -> List[tuple]:
        
        pairs = []
        for entry in brokers_str.split(","):
            entry = entry.strip()
            if ":" in entry:
                host, port_str = entry.rsplit(":", 1)
                try:
                    pairs.append((host, int(port_str)))
                except ValueError:
                    log.warning("Invalid broker entry: %s", entry)
        return pairs

    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        
        from plugins.hooks.kafka_hook import KafkaHook

        hook = KafkaHook(kafka_conn_id=self.kafka_conn_id)
        cfg = hook.get_conn()

        broker_list = self.brokers or self._parse_brokers(cfg["bootstrap_servers"])

        report: Dict[str, Any] = {
            "brokers": {},
            "topic_available": False,
            "consumer_lag": {},
            "overall_healthy": True,
        }

        all_brokers_up = True
        for host, port in broker_list:
            is_up = self._check_tcp(host, port)
            report["brokers"][f"{host}:{port}"] = "UP" if is_up else "DOWN"
            log.info("Broker %s:%s → %s", host, port, "UP" if is_up else "DOWN")
            if not is_up:
                all_brokers_up = False

        if not all_brokers_up:
            report["overall_healthy"] = False
            log.warning("One or more Kafka brokers are unreachable.")

        try:
            report["topic_available"] = hook.topic_exists(self.topic)
            if not report["topic_available"]:
                log.warning("Topic '%s' does not exist on the cluster.", self.topic)
                report["overall_healthy"] = False
            else:
                log.info("Topic '%s' is available.", self.topic)
        except Exception as exc:
            log.error("Failed to list topics: %s", exc)
            report["topic_available"] = False
            report["overall_healthy"] = False

        try:
            offsets = hook.get_consumer_group_offsets(
                self.consumer_group_id, self.topic
            )
            report["consumer_lag"] = offsets
            total_lag = sum(offsets.values()) if offsets else 0
            log.info(
                "Consumer group '%s' total lag: %d", self.consumer_group_id, total_lag
            )
            if total_lag > self.lag_threshold:
                log.warning(
                    "Consumer lag %d exceeds threshold %d",
                    total_lag,
                    self.lag_threshold,
                )
                report["overall_healthy"] = False
        except Exception as exc:
            log.warning("Could not fetch consumer group offsets: %s", exc)
            report["consumer_lag"] = {}

        ti = context["ti"]
        ti.xcom_push(key="health_report", value=report)
        ti.xcom_push(key="is_healthy", value=report["overall_healthy"])

        log.info("Kafka health check complete. Healthy: %s", report["overall_healthy"])
        return report

class KafkaHealthOperatorPlugin(AirflowPlugin):
    name = "kafka_health_operator_plugin"
    operators = [KafkaHealthOperator]
