"""
Pipeline Status Manager
=======================
Manages pipeline execution status for the dashboard.
Writes status to a shared volume as JSON so the dashboard can read it.
Also publishes status events to Kafka for real-time streaming.
"""

import json
import os
import time
from datetime import datetime

STATUS_FILE = "/pipeline_output/pipeline_status.json"
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_STATUS = "pipeline_status"

# Producer reutilizado para evitar reconexiones
_producer = None


def _get_kafka_producer():
    """Obtener o crear Kafka producer (singleton)"""
    global _producer
    if _producer is None:
        try:
            _producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks=1,
                retries=2,
                request_timeout_ms=3000
            )
        except Exception as e:
            print(f"[STATUS_MANAGER] Kafka no disponible: {e}")
            return None
    return _producer


def _publish_to_kafka(status):
    """Publicar estado actual a Kafka para streaming en tiempo real"""
    # Import aqui para evitar errores si kafka no esta disponible
    from kafka import KafkaProducer
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=1,
            retries=1,
            request_timeout_ms=2000
        )
        event = {
            "event_type": "pipeline_status_update",
            "timestamp": datetime.now().isoformat(),
            "status": status
        }
        producer.send(TOPIC_STATUS, value=event)
        producer.flush()
        producer.close()
        print(f"[STATUS_MANAGER] Publicado a Kafka: {status.get('current_step', 'N/A')}")
    except Exception as e:
        # Silencioso - Kafka puede no estar disponible
        pass


def get_status():
    """Get current pipeline status"""
    if os.path.exists(STATUS_FILE):
        try:
            with open(STATUS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass
    return {
        "pipeline": "not_started",
        "started_at": None,
        "completed_at": None,
        "steps": [],
        "current_step": None,
        "error": None,
        "stats": {}
    }


def save_status(status):
    """Save pipeline status to file"""
    os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
    with open(STATUS_FILE, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    # Publicar a Kafka para streaming en tiempo real
    _publish_to_kafka(status)


def init_pipeline():
    """Initialize pipeline status"""
    status = {
        "pipeline": "running",
        "started_at": datetime.now().isoformat(),
        "completed_at": None,
        "steps": [
            {"name": "scraper", "label": "Extracción de datos (Scraper)", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "extract_csv", "label": "Transformación a CSV y MD", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "mongodb_load", "label": "Carga a MongoDB", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "kafka_events", "label": "Generación de eventos Kafka", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "hadoop_wordcount", "label": "Análisis Hadoop WordCount", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "spark_streaming", "label": "Spark Streaming", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "spark_analysis", "label": "Análisis Spark", "status": "pending", "started_at": None, "completed_at": None},
            {"name": "mongodb_results", "label": "Guardado de resultados", "status": "pending", "started_at": None, "completed_at": None},
        ],
        "current_step": None,
        "error": None,
        "stats": {}
    }
    save_status(status)
    return status


def start_step(step_name):
    """Mark a step as started"""
    status = get_status()
    status["current_step"] = step_name
    for step in status["steps"]:
        if step["name"] == step_name:
            step["status"] = "running"
            step["started_at"] = datetime.now().isoformat()
            break
    save_status(status)
    return status


def complete_step(step_name, stats=None):
    """Mark a step as completed"""
    status = get_status()
    for step in status["steps"]:
        if step["name"] == step_name:
            step["status"] = "completed"
            step["completed_at"] = datetime.now().isoformat()
            break
    if stats:
        status["stats"].update(stats)
    save_status(status)
    return status


def fail_step(step_name, error_message):
    """Mark a step as failed"""
    status = get_status()
    status["pipeline"] = "failed"
    status["error"] = error_message
    for step in status["steps"]:
        if step["name"] == step_name:
            step["status"] = "failed"
            step["completed_at"] = datetime.now().isoformat()
            break
    save_status(status)
    return status


def complete_pipeline():
    """Mark the entire pipeline as completed"""
    status = get_status()
    status["pipeline"] = "completed"
    status["completed_at"] = datetime.now().isoformat()
    save_status(status)
    return status