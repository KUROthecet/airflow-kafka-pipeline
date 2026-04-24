

import logging
from typing import Any, Dict, List, Optional

from airflow.hooks.base import BaseHook
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)

class KafkaHook(BaseHook):

    conn_name_attr = "kafka_conn_id"
    default_conn_name = "kafka_default"
    conn_type = "kafka"
    hook_name = "Kafka"

    def __init__(self, kafka_conn_id: str = "kafka_default") -> None:
        super().__init__()
        self.kafka_conn_id = kafka_conn_id
        self._config: Optional[Dict[str, Any]] = None

    def get_conn(self) -> Dict[str, Any]:
        
        if self._config is not None:
            return self._config

        conn = self.get_connection(self.kafka_conn_id)
        extras = conn.extra_dejson

        bootstrap = extras.get(
            "bootstrap_servers",
            f"{conn.host}:{conn.port}" if conn.host else "localhost:9092",
        )
        security_protocol = extras.get("security_protocol", "SASL_PLAINTEXT")
        sasl_mechanism = extras.get("sasl_mechanism", "PLAIN")
        sasl_username = extras.get("sasl_username", conn.login or "")
        sasl_password = extras.get("sasl_password", conn.password or "")

        self._config = {
            "bootstrap_servers": bootstrap,
            "security_protocol": security_protocol,
            "sasl_mechanism": sasl_mechanism,
            "sasl_plain_username": sasl_username,
            "sasl_plain_password": sasl_password,
        }

        log.info(
            "KafkaHook: connecting to brokers=%s protocol=%s",
            bootstrap,
            security_protocol,
        )
        return self._config

    def get_admin_client(self):
        
        from kafka import KafkaAdminClient

        cfg = self.get_conn()
        return KafkaAdminClient(
            bootstrap_servers=cfg["bootstrap_servers"],
            security_protocol=cfg["security_protocol"],
            sasl_mechanism=cfg["sasl_mechanism"],
            sasl_plain_username=cfg["sasl_plain_username"],
            sasl_plain_password=cfg["sasl_plain_password"],
            request_timeout_ms=10_000,
        )

    def get_consumer(self, topic: str, group_id: str = "airflow-consumer", **kwargs):
        
        from kafka import KafkaConsumer

        cfg = self.get_conn()
        return KafkaConsumer(
            topic,
            bootstrap_servers=cfg["bootstrap_servers"],
            security_protocol=cfg["security_protocol"],
            sasl_mechanism=cfg["sasl_mechanism"],
            sasl_plain_username=cfg["sasl_plain_username"],
            sasl_plain_password=cfg["sasl_plain_password"],
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=5_000,
            **kwargs,
        )

    def list_topics(self) -> List[str]:
        
        admin = self.get_admin_client()
        try:
            topics = list(admin.list_topics())
            log.info("KafkaHook.list_topics: %s", topics)
            return topics
        finally:
            admin.close()

    def topic_exists(self, topic: str) -> bool:
        
        return topic in self.list_topics()

    def get_consumer_group_offsets(self, group_id: str, topic: str) -> Dict[str, Any]:
        
        from kafka import KafkaAdminClient, TopicPartition

        cfg = self.get_conn()
        admin = KafkaAdminClient(
            bootstrap_servers=cfg["bootstrap_servers"],
            security_protocol=cfg["security_protocol"],
            sasl_mechanism=cfg["sasl_mechanism"],
            sasl_plain_username=cfg["sasl_plain_username"],
            sasl_plain_password=cfg["sasl_plain_password"],
        )
        try:
            offsets = admin.list_consumer_group_offsets(group_id)
            result = {
                f"{tp.topic}-{tp.partition}": meta.offset
                for tp, meta in offsets.items()
                if tp.topic == topic
            }
            return result
        finally:
            admin.close()

class KafkaHookPlugin(AirflowPlugin):
    name = "kafka_hook_plugin"
    hooks = [KafkaHook]
