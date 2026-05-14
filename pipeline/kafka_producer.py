from kafka import KafkaProducer

import json
import random
import time
import uuid

from collections import Counter
from datetime import datetime, timedelta

from config import *
from logger_config import setup_logger

from alerts_manager import persist_alert
from metrics_manager import MetricsManager
from kafka_metrics_producer import send_metrics

# =============================================
# Logger
# =============================================
logger = setup_logger("kafka_producer")

# =============================================
# Metrics Manager
# =============================================
metrics_manager = MetricsManager()

# =============================================
# Producer Kafka
# =============================================
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    compression_type="gzip",
    acks="all",
    retries=5,
    linger_ms=10,
    batch_size=16384
)

# =============================================
# Catálogo inmobiliario
# =============================================
DISTRICTS = {
    "Miraflores": {
        "base_price": 180000,
        "lat": -12.1211,
        "lon": -77.0297
    },
    "San Isidro": {
        "base_price": 250000,
        "lat": -12.0975,
        "lon": -77.0365
    },
    "Santiago de Surco": {
        "base_price": 140000,
        "lat": -12.1450,
        "lon": -76.9890
    },
    "La Molina": {
        "base_price": 160000,
        "lat": -12.0870,
        "lon": -76.9387
    },
    "Barranco": {
        "base_price": 170000,
        "lat": -12.1414,
        "lon": -77.0206
    },
    "San Borja": {
        "base_price": 145000,
        "lat": -12.1077,
        "lon": -76.9989
    },
    "Jesús María": {
        "base_price": 115000,
        "lat": -12.0746,
        "lon": -77.0503
    },
    "Lince": {
        "base_price": 85000,
        "lat": -12.0840,
        "lon": -77.0310
    },
    "Magdalena": {
        "base_price": 110000,
        "lat": -12.0924,
        "lon": -77.0678
    },
    "Pueblo Libre": {
        "base_price": 95000,
        "lat": -12.0765,
        "lon": -77.0672
    }
}

PROPERTY_TYPES = [
    "departamento",
    "casa",
    "terreno",
    "local_comercial"
]

PORTALS = [
    "adondevivir",
    "infocasas",
    "laencontre"
]

EVENT_TYPES = [
    "nueva_propiedad",
    "cambio_precio",
    "propiedad_vendida",
    "consulta_usuario",
    "propiedad_destacada"
]

# =============================================
# Generador de eventos
# =============================================
def generate_event():

    district = random.choice(list(DISTRICTS.keys()))
    district_info = DISTRICTS[district]

    event_type = random.choice(EVENT_TYPES)

    base_price = district_info["base_price"]

    variation = random.randint(-20000, 50000)

    price = max(30000, base_price + variation)

    area = random.randint(45, 300)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": (
            datetime.utcnow() - timedelta(
                minutes=random.randint(0, 10080)
            )
        ).isoformat(),
        "data": {
            "property_id": f"prop_{random.randint(1000, 9999)}",
            "title": f"{random.choice(PROPERTY_TYPES).title()} en {district}",
            "price": price,
            "currency": random.choice(["USD", "PEN"]),
            "district": district,
            "property_type": random.choice(PROPERTY_TYPES),
            "bedrooms": random.randint(1, 6),
            "bathrooms": random.randint(1, 5),
            "area": area,
            "portal": random.choice(PORTALS),
            "latitude": district_info["lat"],
            "longitude": district_info["lon"],
            "demand_score": random.randint(1, 100),
            "url": f"https://portal.com/property/{uuid.uuid4()}"
        },
        "metadata": {
            "source": EVENT_SOURCE,
            "version": EVENT_VERSION
        }
    }

    return event

# =============================================
# Reglas de alerta
# =============================================
def check_alert_rules(event):

    alerts = []

    data = event["data"]

    district = data["district"]
    price = data["price"]
    area = data["area"]
    currency = data["currency"]

    if (
        district in ["Miraflores", "San Isidro"]
        and price < 80000
        and currency == "USD"
    ):
        alerts.append({
            "alert_type": "precio_bajo",
            "severity": "high",
            "event": event
        })

    if (
        area > 150
        and price < 150000
        and currency == "USD"
    ):
        alerts.append({
            "alert_type": "oportunidad_inversion",
            "severity": "medium",
            "event": event
        })

    return alerts

# =============================================
# Ejecutar producer
# =============================================
def run_producer(
    num_events=TOTAL_EVENTS,
    delay=None
):

    if delay is None:
        delay = random.uniform(
            EVENT_DELAY_MIN,
            EVENT_DELAY_MAX
        )

    logger.info(
        f"Iniciando simulación Kafka con {num_events} eventos"
    )

    total_alerts = 0
    event_counter = Counter()

    start_time = time.time()

    try:

        for i in range(num_events):

            event = generate_event()

            event_type = event["event_type"]

            producer.send(
                TOPIC_EVENTS,
                key=event_type.encode(),
                value=event
            )

            event_counter[event_type] += 1

            metrics_manager.process_event(event)

            alerts = check_alert_rules(event)

            for alert in alerts:

                producer.send(
                    TOPIC_ALERTS,
                    value=alert
                )

                total_alerts += 1

                metrics_manager.process_alert()

                persist_alert(alert)

            if i % 100 == 0:
                logger.info(
                    f"Eventos procesados: {i}/{num_events}"
                )

            time.sleep(delay)

        producer.flush()

        execution_time = round(
            time.time() - start_time,
            2
        )

        streaming_metrics = metrics_manager.get_metrics()

        send_metrics(streaming_metrics)

        logger.info(streaming_metrics)

        stats = {
            "total_events": num_events,
            "alerts_generated": total_alerts,
            "events_per_type": dict(event_counter),
            "execution_time_seconds": execution_time,
            "streaming_metrics": streaming_metrics
        }

        logger.info("Simulación Kafka finalizada")
        logger.info(stats)

        return stats

    except Exception as e:
        logger.error(f"Error ejecutando producer: {str(e)}")
        raise e

    finally:
        producer.close()

# =============================================
# Main
# =============================================
if __name__ == "__main__":
    run_producer()
