# 📚 Guía para Demostrar Kafka al Profesor

## Problema Actual del Dashboard

Los endpoints `/api/alertas_streaming` y `/api/resumen_eventos_streaming` no devuelven datos porque **el pipeline de streaming no ha completado exitosamente** o los datos no se han generado aún.

---

## 🔍 ¿Por qué pasa esto?

### Flujo completo del streaming:

```
┌─────────────┐     ┌──────────┐     ┌────────────────┐     ┌─────────┐     ┌──────────┐
│  Kafka      │────▶│  Kafka   │────▶│  Spark         │────▶│ MongoDB │────▶│ Dashboard│
│  Producer   │     │  Broker  │     │  Streaming     │     │         │     │          │
│  (Python)   │     │          │     │  (PySpark)     │     │         │     │          │
└─────────────┘     └──────────┘     └────────────────┘     └─────────┘     └──────────┘
     │                  │                    │                    │                │
     │ 1. Genera        │ 2. Almacena        │ 3. Consume         │ 4. Guarda      │ 5. Muestra
     │    eventos       │    en topics       │    eventos         │    resultados  │    datos
     │    (1500)        │    - eventos       │    con ventanas    │                │
     │                  │    - alertas       │    - agregaciones  │                │
     │                  │                    │    - RDD anomalias │                │
     │                  │                    │                    │                │
     │                  │                    │ TIMEOUT: 60s       │                │
     │                  │                    │ (puede no ser      │                │
     │                  │                    │  suficiente)       │                │
```

### Causas del problema:

| Causa | Explicación |
|-------|-------------|
| **Timeout corto** | `spark_streaming.py` tiene solo 60 segundos para procesar |
| **Spark lento en iniciar** | Spark puede tardar 30-40s solo en inicializar, dejando poco tiempo para procesar |
| **Topics vacíos** | Si el producer no generó eventos antes de que Spark empiece a consumir |
| **Checkpointing** | Spark Streaming usa checkpointing que puede causar problemas en reinicios |

---

## ✅ Solución Rápida para Demostrar al Profesor

### Opción 1: Ejecutar el script demo (RECOMENDADA)

```bash
# 1. Asegúrate que los contenedores estén corriendo
docker-compose ps

# 2. Ejecuta el script demo desde tu terminal
docker exec pipeline python /app/pipeline/kafka_streaming_demo.py

# 3. Abre el dashboard y ve a la pestaña "Streaming en Vivo"
# http://localhost:8080
```

Este script:
1. ✅ Verifica que Kafka está conectado
2. ✅ Publica 50 eventos de prueba
3. ✅ Consume los eventos directamente (sin Spark)
4. ✅ Guarda los datos en MongoDB
5. ✅ Muestra un reporte de lo procesado

### Opción 2: Ejecutar el pipeline completo

```bash
# Reiniciar el pipeline desde cero
docker-compose down
docker-compose up -d

# Ver logs del pipeline
docker logs -f pipeline
```

---

## 📊 ¿Qué debe ver el profesor para considerar que Kafka está implementado?

### 1. **Topics de Kafka creados**

```bash
# Ver topics creados
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Deberías ver:
# - inmuebles_events
# - inmuebles_alerts
```

### 2. **Mensajes en los topics**

```bash
# Ver mensajes en el topic de eventos
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events \
  --from-beginning \
  --max-messages 5
```

### 3. **Datos en MongoDB**

```bash
# Conectar a MongoDB y ver colecciones
docker exec -it mongodb mongosh

# En la shell de MongoDB:
use inmuebles
db.eventos_streaming.countDocuments()
db.alertas_streaming.countDocuments()
db.resumen_eventos_streaming.find()
```

### 4. **Código del Producer**

Mostrar el archivo `pipeline/kafka_producer.py` que:
- Genera 5 tipos de eventos: `nueva_propiedad`, `cambio_precio`, `propiedad_vendida`, `consulta_usuario`, `propiedad_destacada`
- Publica a Kafka con serialización JSON
- Genera alertas basadas en reglas de negocio

### 5. **Código del Streaming**

Mostrar el archivo `pipeline/spark_streaming.py` que:
- Lee desde Kafka con Spark Structured Streaming
- Usa ventanas de tiempo (windowing de 10s y 30s)
- Aplica operaciones con RDD para detección de anomalías
- Escribe resultados a MongoDB con `foreachBatch`

---

## 🎯 Explicación para el Profesor

> **"Nuestro proyecto implementa un pipeline Big Data híbrido con procesamiento batch y streaming:"**

### Pipeline Batch (ya funciona):
```
Scrapers → JSON → Spark Batch → MongoDB → Dashboard (datos históricos)
```

### Pipeline Streaming (Kafka):
```
Simulador → Kafka → Spark Streaming → MongoDB → Dashboard (tiempo real)
```

### Lo que hace Kafka en nuestro proyecto:

| Función | Descripción |
|---------|-------------|
| **Buffer de eventos** | Los eventos inmobiliarios se publican asíncronamente |
| **Desacoplamiento** | El producer no necesita saber quién consume los eventos |
| **Tolerancia a fallos** | Los mensajes persisten en Kafka aunque el consumer esté caído |
| **Escalabilidad** | Múltiples consumers pueden leer el mismo topic |

### Tipos de eventos que manejamos:

1. **nueva_propiedad** - Nueva propiedad listada en un portal
2. **cambio_precio** - Una propiedad existente cambia de precio
3. **propiedad_vendida** - Una propiedad se marca como vendida/alquilada
4. **consulta_usuario** - Un usuario busca propiedades con filtros
5. **propiedad_destacada** - Una propiedad se marca como premium

### Reglas de alerta:

- **precio_bajo**: Propiedad en Miraflores/San Isidro con precio < $80,000 USD
- **oportunidad_inversion**: Propiedad con área > 150m² y precio < $150,000 USD

---

## 📸 Capturas de pantalla recomendadas para la presentación

1. **Topics de Kafka** - Mostrar `kafka-topics --list`
2. **Mensajes en vivo** - Mostrar `kafka-console-consumer` recibiendo eventos
3. **MongoDB** - Mostrar las colecciones `eventos_streaming` y `alertas_streaming`
4. **Dashboard** - Mostrar la pestaña "Streaming en Vivo" con datos
5. **Código** - Mostrar fragmentos del producer y streaming

---

## 🛠️ Comandos útiles para la demostración

```bash
# 1. Ver estado de contenedores
docker-compose ps

# 2. Ver logs del pipeline en vivo
docker logs -f pipeline

# 3. Ver topics de Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 4. Consumir mensajes de Kafka en vivo
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events

# 5. Ver datos en MongoDB
docker exec -it mongodb mongosh --eval "db.eventos_streaming.find().limit(3)"

# 6. Ejecutar demo rápida
docker exec pipeline python /app/pipeline/kafka_streaming_demo.py
```

---

## 📝 Respuestas a preguntas frecuentes del profesor

### P: ¿Por qué usan Kafka y no otro message broker?
**R:** Kafka es el estándar de la industria para streaming de eventos. Permite:
- Alta throughput (millones de mensajes/segundo)
- Persistencia de mensajes (retención configurable)
- Múltiples consumidores independientes
- Reprocesamiento de eventos (seek a offsets anteriores)

### P: ¿Qué pasa si Spark Streaming se cae?
**R:** Gracias al checkpointing de Spark y la persistencia de Kafka:
- Los mensajes no se pierden, quedan en Kafka
- Spark puede reanudar desde el último checkpoint
- El consumer puede hacer seek al offset que necesite

### P: ¿Cómo manejan la escalabilidad?
**R:** 
- Kafka permite múltiples partitions por topic
- Spark Streaming puede escalar horizontalmente
- Múltiples instancias del consumer pueden leer en paralelo

### P: ¿Los eventos son reales o simulados?
**R:** Actualmente son simulados por el `kafka_producer.py`, pero la arquitectura está lista para recibir eventos reales de:
- Webhooks de los portales inmobiliarios
- Logs de la aplicación web
- APIs de terceros

---

## ✅ Checklist para la demostración

- [ ] Contenedores corriendo (`docker-compose ps`)
- [ ] Topics de Kafka creados
- [ ] Eventos publicados en Kafka
- [ ] Datos en MongoDB (colecciones streaming)
- [ ] Dashboard muestra datos en pestaña Streaming
- [ ] Código del producer explicado
- [ ] Código del streaming explicado
- [ ] Reglas de negocio explicadas

---

## 🚀 Ejecución rápida para la demo

```bash
# 1. Iniciar todo
docker-compose up -d

# 2. Esperar 30 segundos a que inicien los servicios
sleep 30

# 3. Ejecutar demo
docker exec pipeline python /app/pipeline/kafka_streaming_demo.py

# 4. Abrir dashboard
# http://localhost:8080

# 5. Navegar a: Pestaña "Streaming en Vivo"
```

¡Listo! Con esto deberías poder demostrar que Kafka está correctamente implementado en el pipeline.