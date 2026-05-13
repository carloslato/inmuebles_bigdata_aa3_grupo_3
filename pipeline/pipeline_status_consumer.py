#!/usr/bin/env python3
"""
Pipeline Status Consumer
========================
Consume eventos de pipeline_status desde Kafka y los guarda en MongoDB.
Permite que el dashboard muestre el progreso del pipeline en tiempo real.

Uso:
    spark-submit --master local[*] pipeline_status_consumer.py
    
O directamente:
    python pipeline_status_consumer.py
"""

import os
import sys
import json
import time
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
MONGO_DB = "inmuebles"
TOPIC_STATUS = "pipeline_status"

# Timeout para el consumer (segundos)
CONSUMER_TIMEOUT = 300  # 5 minutos max


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [STATUS_CONSUMER] {msg}")


def consume_pipeline_status(timeout=CONSUMER_TIMEOUT):
    """Consumir eventos de pipeline_status desde Kafka y guardar en MongoDB"""
    log(f"Iniciando consumer de pipeline_status...")
    log(f"  Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    log(f"  Topic: {TOPIC_STATUS}")
    log(f"  MongoDB: {MONGODB_URI}")
    log(f"  Timeout: {timeout}s")
    
    try:
        # Conectar a MongoDB
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db["pipeline_status_events"]
        log("Conexión a MongoDB establecida")
        
        # Conectar a Kafka
        consumer = KafkaConsumer(
            TOPIC_STATUS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=timeout * 1000,
            group_id=f"pipeline_status_consumer_{int(time.time())}"
        )
        log(f"Suscripto al topic {TOPIC_STATUS}")
        
        eventos_guardados = 0
        start_time = time.time()
        
        for message in consumer:
            event = message.value
            event['fecha_ingesta'] = datetime.now().isoformat()
            event['offset'] = message.offset
            event['partition'] = message.partition
            
            # Guardar en MongoDB
            collection.insert_one(event)
            eventos_guardados += 1
            
            # Log del progreso
            status = event.get('status', {})
            current_step = status.get('current_step', 'N/A')
            pipeline_state = status.get('pipeline', 'N/A')
            log(f"Evento recibido: step={current_step}, estado={pipeline_state}")
            
            # Verificar timeout
            elapsed = time.time() - start_time
            if elapsed > timeout:
                log(f"Timeout alcanzado ({timeout}s)")
                break
        
        # Log final
        elapsed = time.time() - start_time
        log(f"Consumer finalizado en {elapsed:.1f}s")
        log(f"  Eventos guardados: {eventos_guardados}")
        
        # Contar total en MongoDB
        total = collection.count_documents({})
        log(f"  Total eventos en MongoDB: {total}")
        
        client.close()
        return eventos_guardados
        
    except Exception as e:
        log(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 0


def main():
    """Función principal"""
    print("=" * 60)
    print("PIPELINE STATUS CONSUMER")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("=" * 60)
    
    timeout = int(os.environ.get("STATUS_CONSUMER_TIMEOUT", str(CONSUMER_TIMEOUT)))
    eventos = consume_pipeline_status(timeout=timeout)
    
    print()
    print("=" * 60)
    print(f"CONSUMER FINALIZADO - {eventos} eventos procesados")
    print("=" * 60)
    
    return 0 if eventos > 0 else 1


if __name__ == "__main__":
    sys.exit(main())