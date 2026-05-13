# 📡 Explicación del Flujo de Eventos Kafka

## Caso de Uso: Empresa Inmobiliaria de Análisis de Mercado

Tu proyecto simula una empresa que hace **scraping de portales inmobiliarios** (AdondeVivir, InfoCasas, LaEncontre) para realizar estudios de mercado.

---

## 🔍 ¿Qué Eventos Envía el Proyecto Actualmente a Kafka?

### Evento Principal: `kafka_producer.py`

El archivo `pipeline/kafka_producer.py` se ejecuta **durante el pipeline** (Step 4/8) y genera eventos **simulados** del mercado inmobiliario.

#### Tipos de Eventos que Genera:

| Tipo de Evento | Descripción | Cuándo se "dispara" | Utilidad para el negocio |
|----------------|-------------|---------------------|--------------------------|
| **nueva_propiedad** (40%) | Nueva propiedad listada en un portal | Simula cuando un portal publica una propiedad nueva | Rastrear nuevas oportunidades |
| **cambio_precio** (25%) | Una propiedad cambia de precio | Simula bajadas/subidas de precio | Detectar oportunidades de compra |
| **propiedad_vendida** (10%) | Propiedad vendida/alquilada | Simula transacciones completadas | Análisis de velocidad de ventas |
| **consulta_usuario** (20%) | Usuario busca con filtros específicos | Simula comportamiento de usuarios | Entender demanda del mercado |
| **propiedad_destacada** (5%) | Propiedad se marca como premium | Simula propiedades destacadas | Análisis de estrategias de marketing |

#### Reglas de Alerta Automáticas:

El producer también genera alertas cuando detecta:
- **precio_bajo**: Propiedad en Miraflores/San Isidro con precio < $80,000 USD
- **oportunidad_inversion**: Propiedad con área > 150m² y precio < $150,000 USD

---

## 🤔 ¿Por Qué los Eventos No se Ven en Tiempo Real en el Dashboard?

### Problema Actual

El flujo actual es:

```
┌──────────────┐     ┌─────────┐     ┌─────────────────┐     ┌──────────┐     ┌───────────┐
│ run_pipeline │────▶│  Kafka  │────▶│ spark_streaming │────▶│ MongoDB  │────▶│ Dashboard │
│     .py      │     │ Broker  │     │    (PySpark)    │     │          │     │           │
└──────────────┘     └─────────┘     └─────────────────┘     └──────────┘     └───────────┘
     │                    │                    │                    │                 │
     │ Step 4:            │                    │ Problema:          │                 │
     │ Genera 1500        │ Topics:            │ - Spark tarda      │ Solo muestra    │
     │ eventos            │ - inmuebles_       │   ~30-40s en       │ datos después   │
     │ en ~150 segundos   │   events           │   inicializar      │ de que Spark    │
     │                    │ - inmuebles_       │ - Timeout de 60s   │ escribe en      │
     │                    │   alerts           │   es muy corto     │ MongoDB         │
     │                    │                    │                    │                 │
```

### ¿Qué Debería Pasar (Idealmente)?

Para un caso de uso real de empresa inmobiliaria, el flujo **debería ser**:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         PIPELINE EN EJECUCIÓN                        │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Step 1: Scraper → Publica "📥 Extrayendo datos de AdondeVivir"    │
│            ↓                                                         │
│  Step 2: Extract → Publica "📄 Generando CSV con 2,570 registros"  │
│            ↓                                                         │
│  Step 3: MongoDB → Publica "💾 Cargando 2,570 propiedades a MongoDB"│
│            ↓                                                         │
│  Step 4: Kafka → Publica eventos de mercado simulados               │
│            ↓                                                         │
│  Step 5: Hadoop → Publica "🐘 WordCount procesando descripciones"   │
│            ↓                                                         │
│  Step 6: Spark Streaming → Consume eventos Kafka y genera alertas   │
│            ↓                                                         │
│  Step 7: Spark Analysis → Publica "⚡ Analizando precios por distrito"│
│            ↓                                                         │
│  Step 8: MongoDB Results → Publica "✅ Pipeline completado"         │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │   Kafka Topic   │
                    │ pipeline_status │
                    └─────────────────┘
                              │
                              ▼
                    ┌─────────────────┐
                    │    Dashboard    │
                    │  (Tiempo Real)  │
                    └─────────────────┘
```

---

## ✅ Solución Implementada

### Cambios Realizados:

1. **nginx.conf**: Nueva ruta `/api/status_realtime` que lee directamente el archivo `pipeline_status.json` (que se actualiza en cada paso del pipeline)

2. **dashboard/index.html**: 
   - Función `loadRealtimePipelineStatus()` que hace polling cada 3 segundos
   - Renderizado en tiempo real del estado del pipeline
   - Muestra paso actual, estado de cada step, estadísticas en vivo

3. **pipeline_status.json**: Este archivo ya existía y se actualiza en cada paso del pipeline gracias al `status_manager.py`

---

## 🎯 ¿Cómo Demostrar Kafka al Profesor?

### Explicación del Caso de Uso Real:

> "En una empresa inmobiliaria real, Kafka permitiría:"

1. **Monitoreo en Tiempo Real del Pipeline**
   - Cada paso del scraper publica su progreso
   - El dashboard muestra qué portal se está scrapeando
   - Se puede ver cuántas propiedades se han procesado

2. **Alertas de Oportunidades**
   - Cuando se detecta una propiedad con precio inusualmente bajo
   - El equipo de adquisiciones recibe notificación inmediata

3. **Análisis de Comportamiento de Usuarios**
   - Las consultas de usuarios se publican a Kafka
   - Se puede analizar en tiempo real qué distritos son más buscados

4. **Desacoplamiento de Sistemas**
   - El scraper no necesita saber quién consume los datos
   - Múltiples sistemas pueden leer los mismos eventos (dashboard, alertas, reportes)

### Comandos para Demostrar:

```bash
# 1. Ver topics de Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
# Deberías ver: inmuebles_events, inmuebles_alerts

# 2. Consumir eventos en vivo
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic inmuebles_events

# 3. Ver el status del pipeline en tiempo real
docker exec pipeline cat /pipeline_output/pipeline_status.json

# 4. Ver datos en MongoDB
docker exec -it mongodb mongosh --eval "db.eventos_streaming.find().limit(5)"
```

---

## 📊 Resumen Visual del Flujo

```
┌─────────────────────────────────────────────────────────────┐
│                    ARQUITECTURA COMPLETA                     │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  BATCH (Histórico)          STREAMING (Tiempo Real)         │
│  ─────────────────          ─────────────────────           │
│                                                              │
│  Scrapers → JSON            Pipeline Steps → Kafka ─┐       │
│       ↓                                              │       │
│  Spark Batch → MongoDB ←─────────────────────────────┤       │
│       ↓                                              │       │
│  Dashboard ←─────────────────────────────────────────┘       │
│                                                              │
│  Kafka Topics:                                               │
│  - inmuebles_events (eventos de mercado)                    │
│  - inmuebles_alerts (alertas de negocio)                    │
│  - pipeline_status (progreso del pipeline) [archivo JSON]   │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 🔧 Próximos Pasos Sugeridos

Si quieres mejorar aún más la implementación:

1. **Publicar eventos de cada paso del pipeline a Kafka**
   - Modificar `status_manager.py` para publicar a un topic `pipeline_steps`

2. **Crear un consumer que escuche eventos del pipeline**
   - Actualizar MongoDB con eventos en tiempo real

3. **Extender el timeout de Spark Streaming**
   - Cambiar `STREAMING_TIMEOUT` de 60 a 180 segundos

4. **Agregar más métricas en tiempo real**
   - Velocidad de scraping (propiedades/segundo)
   - Precio promedio en tiempo real mientras se scrapea