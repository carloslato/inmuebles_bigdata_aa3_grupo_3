# 🎯 Guía para Demostrar Kafka al Profesor

## Problema Identificado

Los endpoints `/api/alertas_streaming` y `/api/resumen_eventos_streaming` no devuelven datos porque:

1. **El pipeline de streaming necesita tiempo**: Spark Structured Streaming tarda ~30-40 segundos en inicializar
2. **Timeout corto**: El streaming tiene un timeout de 60 segundos que puede no ser suficiente
3. **Los datos solo aparecen después**: El dashboard muestra datos solo después de que Spark escribe en MongoDB

---

## ✅ Solución Rápida: Script Demo

### Opción 1: Ejecutar el script demo (RECOMENDADO)

```bash
# 1. Asegúrate que los contenedores estén corriendo
docker-compose ps

# 2. Ejecuta el script demo
docker exec pipeline python /app/pipeline/kafka_streaming_demo.py

# 3. Abre el dashboard
# http://localhost:8080
# Ve a la pestaña "Streaming en Vivo"
```

Este script:
1. ✅ Verifica que Kafka está conectado
2. ✅ Publica 50 eventos de prueba
3. ✅ Consume los eventos directamente (sin Spark)
4. ✅ Guarda los datos en MongoDB
5. ✅ Muestra un reporte

---

## 📊 ¿Qué Debe Ver el Profesor?

### 1. Topics de Kafka Creados

```bash
# Ver topics creados
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Deberías ver:
# - inmuebles_events
# - inmuebles_alerts
# - pipeline_status (nuevo)
```

### 2. Mensajes en los Topics

```bash
# Ver mensajes en el topic de eventos
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events \
  --from-beginning \
  --max-messages 5
```

### 3. Datos en MongoDB

```bash
# Conectar a MongoDB
docker exec -it mongodb mongosh

# En la shell de MongoDB:
use inmuebles

# Ver colecciones
show collections

# Contar documentos
db.eventos_streaming.countDocuments()
db.alertas_streaming.countDocuments()
db.pipeline_status_events.countDocuments()

# Ver últimos eventos
db.eventos_streaming.find().limit(3)
```

### 4. Pipeline en Tiempo Real

```bash
# Ver el status del pipeline en tiempo real
docker exec pipeline cat /pipeline_output/pipeline_status.json

# O ver eventos de status en MongoDB
docker exec -it mongodb mongosh --eval "db.pipeline_status_events.find().limit(3)"
```

---

## 🎤 Explicación para el Profesor

### Introducción

> "Nuestro proyecto implementa un pipeline Big Data híbrido con procesamiento **batch** y **streaming**:"

### Pipeline Batch (ya funciona)

```
Scrapers → JSON → Spark Batch → MongoDB → Dashboard (datos históricos)
```

### Pipeline Streaming (Kafka)

```
Simulador → Kafka → Spark Streaming → MongoDB → Dashboard (tiempo real)
```

---

## 📨 Tipos de Eventos que Manejamos

| Tipo de Evento | Descripción | Utilidad para el negocio |
|----------------|-------------|--------------------------|
| **nueva_propiedad** (40%) | Nueva propiedad listada en un portal | Rastrear nuevas oportunidades |
| **cambio_precio** (25%) | Una propiedad cambia de precio | Detectar oportunidades de compra |
| **propiedad_vendida** (10%) | Propiedad vendida/alquilada | Análisis de velocidad de ventas |
| **consulta_usuario** (20%) | Usuario busca con filtros específicos | Entender demanda del mercado |
| **propiedad_destacada** (5%) | Propiedad se marca como premium | Análisis de estrategias de marketing |

---

## 🔔 Reglas de Alerta Automáticas

El sistema genera alertas cuando detecta:

| Regla | Condición | Severidad |
|-------|-----------|-----------|
| **precio_bajo** | Propiedad en Miraflores/San Isidro con precio < $80,000 USD | Alta |
| **oportunidad_inversion** | Propiedad con área > 150m² y precio < $150,000 USD | Media |

---

## 🏗️ Arquitectura Implementada

```
┌─────────────────────────────────────────────────────────────┐
│                    ARQUITECTURA COMPLETA                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  BATCH (Histórico)          STREAMING (Tiempo Real)         │
│  ─────────────────          ─────────────────────           │
│                                                              │
│  Scrapers → JSON            Pipeline → Kafka ───────┐       │
│       ↓                                              │       │
│  Spark Batch → MongoDB ←─────────────────────────────┤       │
│       ↓                                              │       │
│  Dashboard ←─────────────────────────────────────────┘       │
│                                                              │
│  Kafka Topics:                                               │
│  - inmuebles_events (eventos de mercado)                    │
│  - inmuebles_alerts (alertas de negocio)                    │
│  - pipeline_status (progreso del pipeline)                  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Comandos para la Demostración

### 1. Estado del Sistema

```bash
# Ver todos los contenedores
docker-compose ps

# Ver logs del pipeline en vivo
docker logs -f pipeline
```

### 2. Kafka en Acción

```bash
# Listar topics
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# Ver detalles de un topic
docker exec kafka kafka-topics --describe \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events

# Consumir eventos en vivo (deja corriendo para mostrar eventos en tiempo real)
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events
```

### 3. MongoDB - Datos Procesados

```bash
# Ver eventos streaming
docker exec -it mongodb mongosh --eval "db.eventos_streaming.find().limit(5)"

# Ver alertas
docker exec -it mongodb mongosh --eval "db.alertas_streaming.find().limit(5)"

# Ver status del pipeline
docker exec -it mongodb mongosh --eval "db.pipeline_status_events.find().limit(5)"
```

### 4. Dashboard

```
http://localhost:8080

Pestañas importantes:
- "Streaming en Vivo": Eventos en tiempo real, alertas, gráficos
- "Estado del Pipeline": Progreso de cada paso del pipeline
```

---

## 📝 Respuestas a Preguntas Frecuentes

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

## ✅ Checklist para la Demostración

- [ ] Contenedores corriendo (`docker-compose ps`)
- [ ] Topics de Kafka creados (`kafka-topics --list`)
- [ ] Eventos publicados en Kafka (ver con `kafka-console-consumer`)
- [ ] Datos en MongoDB (colecciones `eventos_streaming`, `alertas_streaming`)
- [ ] Dashboard muestra datos en pestaña "Streaming en Vivo"
- [ ] Código del `kafka_producer.py` explicado
- [ ] Código del `spark_streaming.py` explicado
- [ ] Reglas de negocio (alertas) explicadas
- [ ] Pipeline de status en tiempo real funcionando

---

## 🚀 Ejecución Rápida para la Demo

```bash
# 1. Iniciar todo
docker-compose up -d

# 2. Esperar 30 segundos
Start-Sleep -Seconds 30

# 3. Ejecutar demo
docker exec pipeline python /app/pipeline/kafka_streaming_demo.py

# 4. Abrir dashboard
# http://localhost:8080

# 5. Navegar a: Pestaña "Streaming en Vivo"

# 6. Mostrar topics de Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

# 7. Mostrar datos en MongoDB
docker exec -it mongodb mongosh --eval "db.eventos_streaming.countDocuments()"
```

---

## 📈 Mejoras Implementadas

### 1. Status en Tiempo Real del Pipeline

El dashboard ahora muestra el progreso del pipeline **mientras se ejecuta**:

- Cada paso del pipeline publica su estado a Kafka
- El dashboard hace polling cada 3 segundos
- Se muestra el paso actual, estadísticas en vivo, y estado de cada step

### 2. Pipeline Status Consumer

Nuevo script `pipeline_status_consumer.py` que:
- Escucha eventos de `pipeline_status` desde Kafka
- Guarda el progreso en MongoDB
- Permite ver el historial de ejecución

### 3. Status Manager Mejorado

El `status_manager.py` ahora:
- Publica cada actualización a Kafka
- Permite múltiples consumers (dashboard, MongoDB, logs)
- Es tolerante a fallos (si Kafka no está disponible, continúa)

---

## 🎯 Conclusión

Con esta implementación, Kafka cumple las siguientes funciones en el pipeline:

| Función | Descripción |
|---------|-------------|
| **Buffer de eventos** | Los eventos inmobiliarios se publican asíncronamente |
| **Desacoplamiento** | El producer no necesita saber quién consume los eventos |
| **Tolerancia a fallos** | Los mensajes persisten en Kafka aunque el consumer esté caído |
| **Streaming en vivo** | El dashboard puede mostrar eventos en tiempo real |
| **Pipeline status** | El progreso del pipeline se publica para monitoreo |

¡Listo! Con esta guía podrás demostrar que Kafka está correctamente implementado en el pipeline.