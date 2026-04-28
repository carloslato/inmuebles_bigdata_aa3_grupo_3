"""
Pipeline Status Manager
=======================
Manages pipeline execution status for the dashboard.
Writes status to a shared volume as JSON so the dashboard can read it.
"""

import json
import os
import time
from datetime import datetime

STATUS_FILE = "/pipeline_output/pipeline_status.json"


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
            {"name": "hadoop_wordcount", "label": "Análisis Hadoop WordCount", "status": "pending", "started_at": None, "completed_at": None},
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