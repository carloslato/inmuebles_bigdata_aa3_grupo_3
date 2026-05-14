from kafka import KafkaProducer

import json

from config import *
from logger_config import setup_logger

logger = setup_logger("metrics_producer")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_metrics(metrics):

    try:

        producer.send(
            TOPIC_METRICS,
            value=metrics
        )

        producer.flush()

        logger.info("Métricas enviadas a Kafka")

    except Exception as e:
        logger.error(f"Error enviando métricas: {str(e)}")
