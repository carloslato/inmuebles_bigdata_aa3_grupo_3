from kafka import KafkaConsumer

import json

from config import *
from logger_config import setup_logger

logger = setup_logger("kafka_consumer")

consumer = KafkaConsumer(
    TOPIC_EVENTS,
    TOPIC_ALERTS,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='monitor-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logger.info("Escuchando eventos Kafka...")

for message in consumer:

    topic = message.topic
    value = message.value

    if topic == TOPIC_ALERTS:
        logger.warning(f"[ALERTA] {value}")
    else:
        logger.info(f"[EVENTO] {value}")