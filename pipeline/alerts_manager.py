import csv
import json
import os

from datetime import datetime

from logger_config import setup_logger

logger = setup_logger("alerts_manager")

ALERTS_DIR = "alerts"
ALERTS_JSON = os.path.join(ALERTS_DIR, "alerts.json")
ALERTS_CSV = os.path.join(ALERTS_DIR, "alerts.csv")

os.makedirs(ALERTS_DIR, exist_ok=True)


def save_alert_json(alert):

    alerts = []

    if os.path.exists(ALERTS_JSON):
        try:
            with open(ALERTS_JSON, "r", encoding="utf-8") as f:
                alerts = json.load(f)
        except:
            alerts = []

    alerts.append(alert)

    with open(ALERTS_JSON, "w", encoding="utf-8") as f:
        json.dump(alerts, f, indent=2, ensure_ascii=False)


def save_alert_csv(alert):

    file_exists = os.path.exists(ALERTS_CSV)

    with open(ALERTS_CSV, "a", newline="", encoding="utf-8") as csvfile:

        fieldnames = [
            "timestamp",
            "alert_type",
            "severity",
            "district",
            "price",
            "property_type"
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        if not file_exists:
            writer.writeheader()

        data = alert["event"]["data"]

        writer.writerow({
            "timestamp": datetime.utcnow().isoformat(),
            "alert_type": alert["alert_type"],
            "severity": alert["severity"],
            "district": data["district"],
            "price": data["price"],
            "property_type": data["property_type"]
        })


def persist_alert(alert):

    try:
        save_alert_json(alert)
        save_alert_csv(alert)

        logger.info(
            f"Alerta persistida: {alert['alert_type']}"
        )

    except Exception as e:
        logger.error(f"Error persistiendo alerta: {str(e)}")
