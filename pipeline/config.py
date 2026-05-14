import os

# =============================================
# Kafka
# =============================================
KAFKA_BOOTSTRAP_SERVERS = os.environ.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092"
)

TOPIC_EVENTS = "inmuebles_events"
TOPIC_ALERTS = "inmuebles_alerts"
TOPIC_METRICS = "inmuebles_metrics"

# =============================================
# Simulación
# =============================================
TOTAL_EVENTS = int(os.environ.get("TOTAL_EVENTS", 1500))
EVENT_DELAY_MIN = 0.05
EVENT_DELAY_MAX = 0.20

# =============================================
# Metadata
# =============================================
EVENT_SOURCE = "kafka_producer"
EVENT_VERSION = "2.0"