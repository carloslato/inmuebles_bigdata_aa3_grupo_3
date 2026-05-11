#!/usr/bin/env python3
"""
Kafka Setup Script
==================
Configura los topics necesarios para el pipeline de Big Data inmobiliario.

Topics a crear:
- inmuebles_events: Para eventos de propiedades (3 particiones, replication=1)
- inmuebles_alerts: Para alertas del sistema (1 partición)

Este script se ejecuta dentro del contenedor 'pipeline' de Docker.
"""

import sys
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError


def log(msg):
    """Log with timestamp"""
    from datetime import datetime
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [KAFKA-SETUP] {msg}")
    sys.stdout.flush()


def wait_for_kafka(bootstrap_servers, max_attempts=30):
    """Wait for Kafka to be ready"""
    from kafka import KafkaProducer

    log("Esperando que Kafka esté disponible...")
    for attempt in range(max_attempts):
        try:
            producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
            producer.close()
            log("Kafka está listo!")
            return True
        except KafkaError as e:
            log(f"Intento {attempt + 1}/{max_attempts}: Kafka no está listo - {e}")
            time.sleep(2)

    log("Timeout esperando Kafka")
    return False


def setup_topics():
    """Create required Kafka topics"""
    bootstrap_servers = ['kafka:9092']

    # Wait for Kafka
    if not wait_for_kafka(bootstrap_servers):
        return False

    try:
        # Create admin client
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='pipeline-setup'
        )

        # Define topics
        topics = [
            NewTopic(
                name='inmuebles_events',
                num_partitions=3,
                replication_factor=1
            ),
            NewTopic(
                name='inmuebles_alerts',
                num_partitions=1,
                replication_factor=1
            )
        ]

        # Create topics
        log("Creando topics...")
        admin_client.create_topics(new_topics=topics, validate_only=False)

        log("Topics creados exitosamente:")
        for topic in topics:
            log(f"  - {topic.name} (partitions: {topic.num_partitions}, replication: {topic.replication_factor})")

        admin_client.close()
        return True

    except TopicAlreadyExistsError as e:
        log(f"Topics ya existen: {e}")
        return True
    except KafkaError as e:
        log(f"Error creando topics: {e}")
        return False
    except Exception as e:
        log(f"Error inesperado: {e}")
        return False


def main():
    """Main setup function"""
    log("=" * 50)
    log("INICIANDO CONFIGURACIÓN DE KAFKA")
    log("=" * 50)

    success = setup_topics()

    if success:
        log("=" * 50)
        log("CONFIGURACIÓN DE KAFKA COMPLETADA")
        log("=" * 50)
        return 0
    else:
        log("=" * 50)
        log("ERROR EN CONFIGURACIÓN DE KAFKA")
        log("=" * 50)
        return 1


if __name__ == "__main__":
    sys.exit(main())