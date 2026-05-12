#!/usr/bin/env python3
"""
Kafka Streaming Demo - Diagnóstico y Demostración
==================================================
Este script sirve para:
1. Verificar que Kafka está funcionando
2. Publicar eventos de prueba
3. Consumir eventos directamente (sin Spark)
4. Guardar datos en MongoDB para que el dashboard los muestre
5. Generar un reporte de diagnóstico

Uso:
    python kafka_streaming_demo.py
"""

import os
import sys
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from pymongo import MongoClient

# Configuración
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
MONGO_DB = "inmuebles"

TOPIC_EVENTS = "inmuebles_events"
TOPIC_ALERTS = "inmuebles_alerts"

DISTRITOS = ["Miraflores", "San Isidro", "Santiago de Surco", "La Molina", "Barranco", "San Borja"]
TIPOS_EVENTO = ["nueva_propiedad", "cambio_precio", "propiedad_vendida", "consulta_usuario"]


def log(msg):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


def check_kafka():
    """Verificar conexión a Kafka"""
    log("🔍 Verificando conexión a Kafka...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=5000
        )
        # Enviar mensaje de prueba
        test_msg = {"test": "connection", "timestamp": datetime.now().isoformat()}
        future = producer.send(TOPIC_EVENTS, value=test_msg)
        record_metadata = future.get(timeout=10)
        producer.close()
        log(f"✅ Kafka OK - Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
        return True
    except Exception as e:
        log(f"❌ Kafka Error: {e}")
        return False


def check_mongodb():
    """Verificar conexión a MongoDB"""
    log("🔍 Verificando conexión a MongoDB...")
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[MONGO_DB]
        collections = db.list_collection_names()
        log(f"✅ MongoDB OK - Colecciones: {collections}")
        client.close()
        return True
    except Exception as e:
        log(f"❌ MongoDB Error: {e}")
        return False


def generate_test_event():
    """Generar un evento de prueba realista"""
    district = random.choice(DISTRITOS)
    price = random.randint(50000, 500000)
    event_type = random.choice(TIPOS_EVENTO)
    
    event = {
        "event_id": f"demo_{int(time.time() * 1000)}",
        "event_type": event_type,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "data": {
            "property_id": f"prop_{random.randint(1000, 9999)}",
            "title": f"Propiedad de prueba en {district}",
            "price": price,
            "currency": random.choice(["USD", "PEN"]),
            "district": district,
            "property_type": random.choice(["departamento", "casa", "terreno"]),
            "bedrooms": random.randint(1, 5),
            "bathrooms": random.randint(1, 3),
            "area": random.randint(40, 200),
            "portal": random.choice(["adondevivir", "infocasas", "laencontre"]),
            "url": f"https://example.com/prop/{random.randint(1000, 9999)}"
        },
        "metadata": {
            "source": "kafka_demo_script",
            "version": "1.0",
            "demo": True
        }
    }
    return event


def generate_test_alert(event):
    """Generar una alerta basada en un evento"""
    data = event.get("data", {})
    price = data.get("price", 0)
    district = data.get("district", "N/A")
    
    alert = {
        "alert_id": f"alert_{int(time.time() * 1000)}",
        "alert_type": "demo_alert",
        "severity": random.choice(["low", "medium", "high"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "mensaje": f"Alerta demo: Propiedad en {district} a ${price:,}",
        "data": {
            "property_id": data.get("property_id"),
            "district": district,
            "price": price,
            "event_id_origen": event.get("event_id")
        },
        "metadata": {
            "source": "kafka_demo_script",
            "demo": True
        }
    }
    return alert


def publish_test_events(num_events=50):
    """Publicar eventos de prueba a Kafka"""
    log(f"📤 Publicando {num_events} eventos de prueba a Kafka...")
    
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3
    )
    
    events_published = 0
    alerts_published = 0
    
    try:
        for i in range(num_events):
            event = generate_test_event()
            
            # Enviar evento
            producer.send(TOPIC_EVENTS, value=event)
            events_published += 1
            
            # 30% de probabilidad de generar alerta
            if random.random() < 0.3:
                alert = generate_test_alert(event)
                producer.send(TOPIC_ALERTS, value=alert)
                alerts_published += 1
            
            # Pequeño delay
            time.sleep(0.05)
            
            if (i + 1) % 10 == 0:
                log(f"  Progreso: {i + 1}/{num_events} eventos")
        
        producer.flush()
        log(f"✅ {events_published} eventos y {alerts_published} alertas publicados")
        
    except Exception as e:
        log(f"❌ Error publicando: {e}")
        return 0, 0
    finally:
        producer.close()
    
    return events_published, alerts_published


def consume_and_save_to_mongodb(timeout=10):
    """Consumir eventos de Kafka y guardar en MongoDB"""
    log(f"📥 Consumiendo eventos de Kafka y guardando en MongoDB...")
    
    try:
        # Consumidor para eventos
        consumer_events = KafkaConsumer(
            TOPIC_EVENTS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=timeout * 1000,
            group_id=f"demo_consumer_{int(time.time())}"
        )
        
        # Consumidor para alertas
        consumer_alerts = KafkaConsumer(
            TOPIC_ALERTS,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='latest',
            consumer_timeout_ms=timeout * 1000,
            group_id=f"demo_alert_consumer_{int(time.time())}"
        )
        
        # Conectar a MongoDB
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        
        eventos_guardados = 0
        alertas_guardadas = 0
        
        # Consumir eventos
        log("  Consumiendo eventos...")
        for message in consumer_events:
            event = message.value
            event['fecha'] = datetime.now().isoformat()
            
            # Guardar en MongoDB
            db["eventos_streaming"].insert_one(event)
            eventos_guardados += 1
        
        # Consumir alertas
        log("  Consumiendo alertas...")
        for message in consumer_alerts:
            alert = message.value
            alert['fecha'] = datetime.now().isoformat()
            
            # Guardar en MongoDB
            db["alertas_streaming"].insert_one(alert)
            alertas_guardadas += 1
        
        # Generar resumen
        resumen = {
            "fecha": datetime.now().isoformat(),
            "total_eventos": eventos_guardados,
            "total_alertas": alertas_guardadas,
            "eventos_por_tipo": {},
            "eventos_por_distrito": {}
        }
        
        # Calcular estadísticas
        for event in db["eventos_streaming"].find({"fecha": {"$gte": resumen["fecha"][:-10]}}).limit(100):
            tipo = event.get("event_type", "unknown")
            distrito = event.get("data", {}).get("district", "unknown")
            
            resumen["eventos_por_tipo"][tipo] = resumen["eventos_por_tipo"].get(tipo, 0) + 1
            resumen["eventos_por_distrito"][distrito] = resumen["eventos_por_distrito"].get(distrito, 0) + 1
        
        # Guardar resumen
        db["resumen_eventos_streaming"].insert_one(resumen)
        
        client.close()
        
        log(f"✅ {eventos_guardados} eventos y {alertas_guardadas} alertas guardadas en MongoDB")
        log(f"📊 Resumen: {resumen['eventos_por_tipo']}")
        
        return eventos_guardados, alertas_guardadas
        
    except Exception as e:
        log(f"❌ Error consumiendo: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0


def show_dashboard_data():
    """Mostrar datos actuales en MongoDB para el dashboard"""
    log("📊 Mostrando datos actuales en MongoDB...")
    
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        
        # Contar documentos
        collections_info = {}
        for coll in ["eventos_streaming", "alertas_streaming", "resumen_eventos_streaming"]:
            count = db[coll].count_documents({})
            collections_info[coll] = count
            
            # Mostrar últimos documentos
            if count > 0:
                log(f"  {coll}: {count} documentos")
                latest = list(db[coll].find().sort("fecha", -1).limit(3))
                for doc in latest:
                    if coll == "eventos_streaming":
                        log(f"    - {doc.get('event_type', 'N/A')} | {doc.get('data', {}).get('district', 'N/A')} | ${doc.get('data', {}).get('price', 0):,}")
                    elif coll == "alertas_streaming":
                        log(f"    - {doc.get('mensaje', 'N/A')} | {doc.get('severity', 'N/A')}")
                    else:
                        log(f"    - {doc.get('total_eventos', 0)} eventos, {doc.get('total_alertas', 0)} alertas")
        
        client.close()
        return collections_info
        
    except Exception as e:
        log(f"❌ Error: {e}")
        return {}


def main():
    """Ejecutar demostración completa"""
    print("=" * 70)
    print("🚀 KAFKA STREAMING DEMO - DIAGNÓSTICO Y DEMOSTRACIÓN")
    print("=" * 70)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MongoDB: {MONGODB_URI}")
    print("=" * 70)
    print()
    
    # 1. Verificar conexiones
    kafka_ok = check_kafka()
    mongo_ok = check_mongodb()
    print()
    
    if not kafka_ok:
        log("❌ Kafka no está disponible. Verifica que el contenedor esté corriendo.")
        return 1
    
    if not mongo_ok:
        log("❌ MongoDB no está disponible. Verifica que el contenedor esté corriendo.")
        return 1
    
    # 2. Publicar eventos de prueba
    print()
    events_pub, alerts_pub = publish_test_events(num_events=50)
    print()
    
    if events_pub == 0:
        log("❌ No se pudieron publicar eventos")
        return 1
    
    # 3. Consumir y guardar en MongoDB
    events_saved, alerts_saved = consume_and_save_to_mongodb(timeout=5)
    print()
    
    # 4. Mostrar datos disponibles
    show_dashboard_data()
    print()
    
    # 5. Resumen final
    print("=" * 70)
    print("✅ DEMOSTRACIÓN COMPLETADA")
    print("=" * 70)
    print(f"Eventos publicados: {events_pub}")
    print(f"Alertas publicadas: {alerts_pub}")
    print(f"Eventos guardados: {events_saved}")
    print(f"Alertas guardadas: {alerts_saved}")
    print()
    print("📊 Ahora puedes abrir el dashboard en http://localhost:8080")
    print("   y navegar a la pestaña 'Streaming en Vivo' para ver los datos")
    print("=" * 70)
    
    return 0


if __name__ == "__main__":
    sys.exit(main())