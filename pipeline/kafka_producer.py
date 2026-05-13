#!/usr/bin/env python3
"""
Kafka Producer para Eventos Inmobiliarios
=========================================
Simula eventos en tiempo real del mercado inmobiliario y los publica en Kafka.

Tipos de eventos:
- nueva_propiedad: Se publica una nueva propiedad en un portal
- cambio_precio: Una propiedad existente cambia de precio
- propiedad_vendida: Una propiedad se marca como vendida/alquilada
- consulta_usuario: Un usuario consulta propiedades con ciertos filtros
- propiedad_destacada: Una propiedad se marca como destacada/premium

Reglas de alerta:
- precio_bajo: Propiedad en Miraflores/San Isidro con precio < $80,000 USD
- oportunidad_inversion: Propiedad con área > 150m² y precio < $150,000 USD
"""

import os
import json
import time
import random
import uuid
from datetime import datetime, timedelta
from kafka import KafkaProducer

# Configuración de Kafka
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC_EVENTS = "inmuebles_events"
TOPIC_ALERTS = "inmuebles_alerts"
TOTAL_EVENTS = random.randint(1000, 3000)

# Datos simulados
DISTRITOS = [
    "Miraflores", "San Isidro", "Santiago de Surco", "La Molina", "Barranco",
    "San Borja", "Jesús María", "Lince", "Magdalena", "Pueblo Libre"
]

TIPOS_PROPIEDAD = ["departamento", "casa", "terreno", "local_comercial"]

MONEDAS = ["PEN", "USD"]

PORTALES = ["adondevivir", "infocasas", "laencontre"]

# Rangos de precios por distrito (en USD)
PRECIOS_BASE = {
    "Miraflores": (80000, 500000),
    "San Isidro": (100000, 600000),
    "Santiago de Surco": (60000, 350000),
    "La Molina": (90000, 450000),
    "Barranco": (70000, 400000),
    "San Borja": (65000, 300000),
    "Jesús María": (50000, 250000),
    "Lince": (45000, 200000),
    "Magdalena": (40000, 180000),
    "Pueblo Libre": (55000, 280000)
}

# Títulos por tipo de propiedad
TITULOS_BASE = {
    "departamento": [
        "Departamento moderno en {district}",
        "Hermoso departamento con vista en {district}",
        "Departamento amplio ideal familia en {district}",
        "Departamento estrenar en {district}",
        "Departamento lujo en {district}"
    ],
    "casa": [
        "Casa familiar en {district}",
        "Casa con jardín en {district}",
        "Casa moderna en {district}",
        "Casa espaciosa en {district}",
        "Casa exclusiva en {district}"
    ],
    "terreno": [
        "Terreno residencial en {district}",
        "Terreno comercial en {district}",
        "Lote de terreno en {district}",
        "Terreno para construcción en {district}"
    ],
    "local_comercial": [
        "Local comercial en {district}",
        "Oficina en {district}",
        "Local para negocio en {district}",
        "Espacio comercial en {district}"
    ]
}

# Tipos de consulta de usuario
TIPOS_CONSULTA = ["venta", "alquiler", "ambos"]


def generate_property_id():
    """Genera un ID único para propiedad"""
    return f"prop_{uuid.uuid4().hex[:8]}"


def generate_event_id():
    """Genera un ID único para evento"""
    return f"evt_{uuid.uuid4().hex[:12]}"


def generate_timestamp():
    """Genera timestamp actual en formato ISO"""
    return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")


def generate_price(district, currency="USD"):
    """Genera precio aleatorio basado en el distrito"""
    min_price, max_price = PRECIOS_BASE.get(district, (50000, 300000))
    price = random.randint(min_price, max_price)
    
    if currency == "PEN":
        # Convertir a PEN (tasa aproximada 3.7)
        price = int(price * 3.7)
    
    return price


def generate_area(property_type):
    """Genera área según tipo de propiedad"""
    if property_type == "departamento":
        return random.randint(40, 200)
    elif property_type == "casa":
        return random.randint(80, 400)
    elif property_type == "terreno":
        return random.randint(100, 1000)
    elif property_type == "local_comercial":
        return random.randint(30, 500)
    return random.randint(50, 150)


def generate_bedrooms(area, property_type):
    """Genera número de dormitorios según área"""
    if property_type in ["terreno", "local_comercial"]:
        return 0
    base = max(1, area // 40)
    return min(base, random.randint(1, 6))


def generate_bathrooms(bedrooms):
    """Genera número de baños según dormitorios"""
    return max(1, bedrooms - 1) if bedrooms > 1 else 1


def generate_url(portal, property_id):
    """Genera URL simulada de la propiedad"""
    return f"https://www.{portal}.pe/propiedad/{property_id}"


def generate_base_property():
    """Genera datos base de una propiedad"""
    district = random.choice(DISTRITOS)
    property_type = random.choice(TIPOS_PROPIEDAD)
    currency = random.choice(MONEDAS)
    portal = random.choice(PORTALES)
    
    area = generate_area(property_type)
    price = generate_price(district, currency)
    
    property_id = generate_property_id()
    
    title_template = random.choice(TITULOS_BASE[property_type])
    title = title_template.format(district=district)
    
    return {
        "property_id": property_id,
        "title": title,
        "price": price,
        "currency": currency,
        "district": district,
        "property_type": property_type,
        "bedrooms": generate_bedrooms(area, property_type),
        "bathrooms": generate_bathrooms(generate_bedrooms(area, property_type)),
        "area": area,
        "portal": portal,
        "url": generate_url(portal, property_id)
    }


def generate_nueva_propiedad():
    """Genera evento de nueva propiedad"""
    data = generate_base_property()
    return {
        "event_id": generate_event_id(),
        "event_type": "nueva_propiedad",
        "timestamp": generate_timestamp(),
        "data": data,
        "metadata": {
            "source": "kafka_producer",
            "version": "1.0"
        }
    }


def generate_cambio_precio():
    """Genera evento de cambio de precio"""
    data = generate_base_property()
    precio_anterior = data["price"]
    # Cambio entre -20% y +15%
    cambio_pct = random.uniform(-0.20, 0.15)
    data["price"] = int(data["price"] * (1 + cambio_pct))
    data["precio_anterior"] = precio_anterior
    data["variacion_pct"] = round(cambio_pct * 100, 2)
    
    return {
        "event_id": generate_event_id(),
        "event_type": "cambio_precio",
        "timestamp": generate_timestamp(),
        "data": data,
        "metadata": {
            "source": "kafka_producer",
            "version": "1.0"
        }
    }


def generate_propiedad_vendida():
    """Genera evento de propiedad vendida/alquilada"""
    data = generate_base_property()
    data["estado"] = random.choice(["vendida", "alquilada"])
    data["fecha_venta"] = generate_timestamp()
    data["precio_venta"] = data["price"]
    
    return {
        "event_id": generate_event_id(),
        "event_type": "propiedad_vendida",
        "timestamp": generate_timestamp(),
        "data": data,
        "metadata": {
            "source": "kafka_producer",
            "version": "1.0"
        }
    }


def generate_consulta_usuario():
    """Genera evento de consulta de usuario"""
    district_filter = random.sample(DISTRITOS, k=random.randint(1, 3))
    property_type_filter = random.sample(TIPOS_PROPIEDAD, k=random.randint(1, 2))
    
    return {
        "event_id": generate_event_id(),
        "event_type": "consulta_usuario",
        "timestamp": generate_timestamp(),
        "data": {
            "user_id": f"user_{uuid.uuid4().hex[:6]}",
            "tipo_consulta": random.choice(TIPOS_CONSULTA),
            "filtros": {
                "distritos": district_filter,
                "tipos_propiedad": property_type_filter,
                "precio_min": random.randint(30000, 100000),
                "precio_max": random.randint(200000, 600000),
                "moneda": random.choice(MONEDAS),
                "dormitorios_min": random.randint(1, 4),
                "area_min": random.randint(50, 150)
            },
            "resultados_mostrados": random.randint(5, 50),
            "propiedades_clickeadas": random.randint(0, 5)
        },
        "metadata": {
            "source": "kafka_producer",
            "version": "1.0"
        }
    }


def generate_propiedad_destacada():
    """Genera evento de propiedad destacada/premium"""
    data = generate_base_property()
    # Las propiedades destacadas suelen ser de mayor valor
    data["price"] = int(data["price"] * random.uniform(1.1, 1.5))
    data["destacado"] = True
    data["tipo_destacado"] = random.choice(["premium", "gold", "platinum"])
    data["dias_destacado"] = random.randint(7, 30)
    data["costo_destacado"] = random.randint(50, 200)
    
    return {
        "event_id": generate_event_id(),
        "event_type": "propiedad_destacada",
        "timestamp": generate_timestamp(),
        "data": data,
        "metadata": {
            "source": "kafka_producer",
            "version": "1.0"
        }
    }


# Mapa de generadores por tipo de evento
EVENT_GENERATORS = {
    "nueva_propiedad": generate_nueva_propiedad,
    "cambio_precio": generate_cambio_precio,
    "propiedad_vendida": generate_propiedad_vendida,
    "consulta_usuario": generate_consulta_usuario,
    "propiedad_destacada": generate_propiedad_destacada
}

# Pesos para distribución de eventos (más comunes primero)
EVENT_WEIGHTS = {
    "nueva_propiedad": 40,
    "cambio_precio": 25,
    "consulta_usuario": 20,
    "propiedad_vendida": 10,
    "propiedad_destacada": 5
}


def generate_event(event_type=None):
    """
    Genera un evento aleatorio realista.
    
    Args:
        event_type: Tipo específico de evento (opcional). Si es None, 
                    selecciona aleatoriamente según pesos.
    
    Returns:
        dict: Evento generado en formato JSON
    """
    if event_type is None:
        # Seleccionar tipo según pesos
        event_type = random.choices(
            list(EVENT_WEIGHTS.keys()),
            weights=list(EVENT_WEIGHTS.values()),
            k=1
        )[0]
    
    generator = EVENT_GENERATORS.get(event_type)
    if generator:
        return generator()
    
    # Fallback a nueva_propiedad si tipo desconocido
    return generate_nueva_propiedad()


def check_alert_rules(event):
    """
    Evalúa reglas de alerta sobre un evento.
    
    Reglas:
    - precio_bajo: Propiedad en Miraflores/San Isidro con precio < $80,000 USD
    - oportunidad_inversion: Propiedad con área > 150m² y precio < $150,000 USD
    
    Args:
        event: Evento a evaluar
    
    Returns:
        list: Lista de alertas generadas (vacía si no hay alertas)
    """
    alerts = []
    
    # Solo aplicar reglas a eventos con datos de propiedad
    if event.get("event_type") not in ["nueva_propiedad", "cambio_precio", "propiedad_destacada"]:
        return alerts
    
    data = event.get("data", {})
    district = data.get("district", "")
    price = data.get("price", 0)
    currency = data.get("currency", "USD")
    area = data.get("area", 0)
    
    # Convertir precio a USD si está en PEN
    price_usd = price
    if currency == "PEN":
        price_usd = price / 3.7
    
    # Regla 1: precio_bajo
    # Propiedad en Miraflores o San Isidro con precio < $80,000 USD
    if district in ["Miraflores", "San Isidro"] and price_usd < 80000:
        alert = {
            "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
            "alert_type": "precio_bajo",
            "timestamp": generate_timestamp(),
            "severity": "high",
            "data": {
                "property_id": data.get("property_id"),
                "district": district,
                "price": price,
                "price_usd": round(price_usd, 2),
                "currency": currency,
                "title": data.get("title"),
                "url": data.get("url"),
                "portal": data.get("portal")
            },
            "rule_description": f"Propiedad en {district} con precio inusualmente bajo (${round(price_usd, 2)} USD)",
            "metadata": {
                "source": "kafka_producer_alert_rules",
                "version": "1.0"
            }
        }
        alerts.append(alert)
    
    # Regla 2: oportunidad_inversion
    # Propiedad con área > 150m² y precio < $150,000 USD
    if area > 150 and price_usd < 150000:
        alert = {
            "alert_id": f"alert_{uuid.uuid4().hex[:12]}",
            "alert_type": "oportunidad_inversion",
            "timestamp": generate_timestamp(),
            "severity": "medium",
            "data": {
                "property_id": data.get("property_id"),
                "district": district,
                "price": price,
                "price_usd": round(price_usd, 2),
                "currency": currency,
                "area": area,
                "price_per_m2": round(price_usd / area, 2) if area > 0 else 0,
                "title": data.get("title"),
                "url": data.get("url"),
                "portal": data.get("portal")
            },
            "rule_description": f"Oportunidad: {area}m² a ${round(price_usd, 2)} USD (${round(price_usd/area, 2)}/m²)",
            "metadata": {
                "source": "kafka_producer_alert_rules",
                "version": "1.0"
            }
        }
        alerts.append(alert)
    
    return alerts


def run_producer(num_events=1500, delay=0.1):
    """
    Ejecuta el productor de Kafka.
    
    Args:
        num_events: Número de eventos a generar (default: 1500)
        delay: Delay entre eventos en segundos (default: 0.1)
    
    Returns:
        dict: Estadísticas de la ejecución
    """
    log(f"Iniciando Kafka Producer...")
    log(f"  Topic Events: {TOPIC_EVENTS}")
    log(f"  Topic Alerts: {TOPIC_ALERTS}")
    log(f"  Bootstrap Servers: {KAFKA_BOOTSTRAP_SERVERS}")
    log(f"  Número de eventos: {num_events}")
    log(f"  Delay entre eventos: {delay}s")
    
    # Crear producer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=3,
        retry_backoff_ms=100
    )
    
    # Estadísticas
    stats = {
        "total_eventos": 0,
        "eventos_por_tipo": {},
        "alertas_generadas": 0,
        "alertas_por_tipo": {},
        "eventos_enviados": 0,
        "alertas_enviadas": 0,
        "errores": 0
    }
    
    start_time = time.time()
    
    try:
        for i in range(num_events):
            # Generar evento
            event = generate_event()
            event_type = event.get("event_type", "unknown")
            
            # Actualizar estadísticas
            stats["total_eventos"] += 1
            stats["eventos_por_tipo"][event_type] = stats["eventos_por_tipo"].get(event_type, 0) + 1
            
            # Enviar evento al topic
            try:
                future = producer.send(TOPIC_EVENTS, value=event)
                stats["eventos_enviados"] += 1
            except Exception as e:
                log(f"  [ERROR] Enviando evento {i}: {e}")
                stats["errores"] += 1
            
            # Verificar reglas de alerta
            alerts = check_alert_rules(event)
            
            for alert in alerts:
                stats["alertas_generadas"] += 1
                alert_type = alert.get("alert_type", "unknown")
                stats["alertas_por_tipo"][alert_type] = stats["alertas_por_tipo"].get(alert_type, 0) + 1
                
                # Enviar alerta
                try:
                    producer.send(TOPIC_ALERTS, value=alert)
                    stats["alertas_enviadas"] += 1
                except Exception as e:
                    log(f"  [ERROR] Enviando alerta: {e}")
                    stats["errores"] += 1
            
            # Pequeño delay para simular streaming
            if delay > 0:
                time.sleep(delay)
            
            # Progreso cada 100 eventos
            if (i + 1) % 100 == 0:
                elapsed = time.time() - start_time
                rate = (i + 1) / elapsed if elapsed > 0 else 0
                log(f"  Progreso: {i + 1}/{num_events} eventos ({rate:.1f} evt/s) - {stats['alertas_generadas']} alertas")
        
        # Flush para asegurar que todos los mensajes se envíen
        producer.flush()
        
        elapsed = time.time() - start_time
        log(f"Kafka Producer completado en {elapsed:.2f}s")
        log(f"  Total eventos: {stats['total_eventos']}")
        log(f"  Eventos enviados: {stats['eventos_enviados']}")
        log(f"  Alertas generadas: {stats['alertas_generadas']}")
        log(f"  Alertas enviadas: {stats['alertas_enviadas']}")
        log(f"  Errores: {stats['errores']}")
        log(f"  Eventos por tipo: {stats['eventos_por_tipo']}")
        log(f"  Alertas por tipo: {stats['alertas_por_tipo']}")
        
    except Exception as e:
        import traceback
        log(f"[ERROR] Producer error: {e}")
        log(f"[ERROR] Traceback: {traceback.format_exc()}")
        stats["errores"] += 1
        stats["error_detalle"] = str(e)
    
    finally:
        producer.close()
    
    return stats


def log(msg):
    """Log con timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [KAFKA_PRODUCER] {msg}")


if __name__ == "__main__":
    # Ejecutar con parámetros por defecto o desde variables de entorno
    num_events = int(os.environ.get("KAFKA_NUM_EVENTS", "1500"))
    delay = float(os.environ.get("KAFKA_EVENT_DELAY", "0.1"))
    
    stats = run_producer(num_events=num_events, delay=delay)
    
    # Imprimir resumen final
    print("\n" + "=" * 60)
    print("RESUMEN KAFKA PRODUCER")
    print("=" * 60)
    print(json.dumps(stats, indent=2, default=str))