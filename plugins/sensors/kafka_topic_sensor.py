"""
KafkaTopicSensor - Custom Airflow Sensor to wait for messages in a Kafka topic.

Periodically polls the topic and returns True when at least one
new message is available since the last checked offset.
"""

import logging
from typing import Any, Dict, Sequence

from airflow.sensors.base import BaseSensorOperator
from airflow.plugins_manager import AirflowPlugin

log = logging.getLogger(__name__)


class KafkaTopicSensor(BaseSensorOperator):
    """
    Waits for at least one new message on a Kafka topic.

    Args:
        kafka_conn_id (str): Airflow Connection ID for Kafka.
        topic (str): Kafka topic to monitor.
        consumer_group_id (str): Consumer group to use for offset tracking.
    """

    template_fields: Sequence[str] = ("topic",)
    ui_color = "#f0d4e4"

    def __init__(
        self,
        *,
        kafka_conn_id: str = "kafka_default",
        topic: str = "product_view",
        consumer_group_id: str = "airflow-sensor",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.kafka_conn_id = kafka_conn_id
        self.topic = topic
        self.consumer_group_id = consumer_group_id

    def poke(self, context: Dict[str, Any]) -> bool:
        """Poll Kafka for new messages."""
        from plugins.hooks.kafka_hook import KafkaHook

        hook = KafkaHook(kafka_conn_id=self.kafka_conn_id)
        
        if not hook.topic_exists(self.topic):
            log.warning("Topic '%s' does not exist yet.", self.topic)
            return False

        consumer = hook.get_consumer(
            self.topic,
            group_id=self.consumer_group_id,
            consumer_timeout_ms=2000,
            max_poll_records=1,
        )

        try:
            msg_batch = consumer.poll(timeout_ms=2000, max_records=1)
            
            has_messages = False
            for tp, messages in msg_batch.items():
                if messages:
                    has_messages = True
                    log.info("Found %d new messages on partition %s", len(messages), tp)
                    break
                    
            if has_messages:
                log.info("KafkaTopicSensor: new messages found. Sensor succeeded.")
                return True
            else:
                log.info("KafkaTopicSensor: no new messages.")
                return False
                
        except Exception as exc:
            log.warning("Error while polling Kafka: %s", exc)
            return False
        finally:
            consumer.close()


class KafkaTopicSensorPlugin(AirflowPlugin):
    name = "kafka_topic_sensor_plugin"
    sensors = [KafkaTopicSensor]
