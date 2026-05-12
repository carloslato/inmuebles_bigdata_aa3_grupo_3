# Informe AA4 — Análisis de Mercado Inmobiliario con Ecosistema Big Data

---

## Portada

**INSTITUTO:** INSTITUTO DE EDUCACIÓN SUPERIOR TECNOLÓGICO PRIVADO CERTUS
**Curso:** Diseño de Soluciones de Big Data
**Actividad de Aprendizaje:** AA4 — Procesamiento Streaming y Pipeline Completo
**Grupo:** Grupo 3

| Rol | Integrante |
| --- | ---------- |

RENATO LUIS YANAMANGO CASANA
CARLOS WOLLEY LA TORRE MACHADO
ANDREA CALDASS QUISPITUPA
JENIFER COLANA CHECCA
CARLOS FAUSTO HUAMAN RENGIFO

**Repositorio GitHub:** [URL del repositorio]

---

## 1. Introducción

El presente informe documenta el desarrollo de la Actividad de Aprendizaje 4 (AA4) del proyecto "Análisis de Mercado Inmobiliario con Ecosistema Big Data". Este entregable representa la evolución final del sistema, incorporando capacidades de **procesamiento en tiempo real** mediante Apache Kafka y Spark Structured Streaming sobre la base construida en AA3.

El proyecto aborda una problemática real del mercado inmobiliario peruano: la dispersión de información entre múltiples portales web, la dificultad para detectar tendencias de precio en tiempo real y la falta de herramientas de análisis accesibles para tomadores de decisión. La solución implementada integra tecnologías Big Data de nivel empresarial dentro de un ecosistema contenedorizado y reproducible.

---

## 2. Caso de Negocio — Evolución desde AA3

### 2.1 Contexto

El mercado inmobiliario peruano, particularmente en Lima Metropolitana, presenta alta volatilidad de precios y disparidad de información entre portales como AdondeVivir, InfoCasas y LaEncontre. Los compradores e inversionistas necesitan herramientas que agreguen, analicen y presenten esta información de forma consolidada.

### 2.2 Evolución del Proyecto

| Entregable | Capacidades incorporadas                                                                           |
| ---------- | -------------------------------------------------------------------------------------------------- |
| AA1        | Scraping de datos de portales inmobiliarios                                                        |
| AA2        | Almacenamiento en MongoDB, primeras consultas                                                      |
| AA3        | Pipeline batch con Spark (DataFrames, SQL, RDD), Hadoop, dashboard estático                        |
| **AA4**    | **Kafka + Spark Streaming, dashboard en tiempo real, historial de pipelines, alertas automáticas** |

### 2.3 Objetivos AA4

1. Implementar un flujo de procesamiento streaming con Apache Kafka y Spark Structured Streaming.
2. Generar alertas automáticas ante anomalías de precio o alta demanda por zona.
3. Visualizar eventos inmobiliarios en tiempo real mediante un dashboard en vivo.
4. Mantener un historial de ejecuciones del pipeline con métricas por corrida.
5. Asegurar la calidad e integridad de los datos acumulados en MongoDB.

---

## 3. Datos Utilizados

### 3.1 Tabla Completa de Archivos

| Archivo                       | Formato    | Cantidad Registros  | Fuente                     | Uso                              |
| ----------------------------- | ---------- | ------------------- | -------------------------- | -------------------------------- |
| inmuebles_adondevivir.json    | JSON       | ~1,800              | Scraping AdondeVivir       | Datos crudos para pipeline batch |
| inmuebles_infocasas.json      | JSON       | ~450                | Scraping InfoCasas         | Datos crudos para pipeline batch |
| inmuebles_laencontre.json     | JSON       | ~320                | Scraping LaEncontre        | Datos crudos para pipeline batch |
| inmuebles_todos.json          | JSON       | ~2,570              | Consolidación de fuentes   | Dataset unificado de entrada     |
| inmuebles.csv                 | CSV        | ~2,570              | Transformación desde JSON  | Input directo para Spark Batch   |
| descripciones_adondevivir.md  | MD         | 1 archivo           | Transformación             | Procesamiento texto en Hadoop    |
| descripciones_infocasas.md    | MD         | 1 archivo           | Transformación             | Procesamiento texto en Hadoop    |
| descripciones_laencontre.md   | MD         | 1 archivo           | Transformación             | Procesamiento texto en Hadoop    |
| eventos_inmobiliarios (Kafka) | JSON/Kafka | 1,000–3,000         | Simulador Python           | Procesamiento Spark Streaming    |
| pipeline_summary (MongoDB)    | BSON       | 1 doc por ejecución | Orquestador pipeline       | Historial y métricas             |
| alertas_streaming (MongoDB)   | BSON       | Variable            | Spark Structured Streaming | Alertas de precio y demanda      |

### 3.2 Descripción de los Datos

Los datos batch provienen de scraping de tres portales inmobiliarios peruanos. Cada registro contiene campos como: precio, tipo de inmueble (departamento, casa, oficina), distrito, área en m², número de habitaciones, baños, descripción y URL de origen.

Los datos streaming son generados por un simulador Python que modela eventos reales como: búsquedas por zona, consultas de precio, y transacciones completadas. Cada evento incluye: tipo de evento, zona, precio referencial, timestamp y metadatos del usuario simulado.

### 3.3 Calidad de Datos

- Los registros batch se cargan en MongoDB usando **upsert** para evitar duplicados entre ejecuciones.
- Los datos streaming se almacenan con **timestamp de ingesta** para permitir análisis temporal.
- Se aplican validaciones de rango de precios y completitud de campos obligatorios antes de cargar.
- El pipeline registra en `pipeline_summary` el número de registros procesados, rechazados y el tiempo de ejecución.

---

## 4. Arquitectura del Sistema

### 4.1 Diagrama General

```
┌─────────────────────────────────────────────────────────────────┐
│                    FUENTES DE DATOS                             │
│  JSON (3 portales) │ CSV consolidado │ MD descripciones         │
│  Simulador eventos streaming                                    │
└────────────┬────────────────────────────┬───────────────────────┘
             │ Batch                      │ Streaming
             ▼                            ▼
┌────────────────────┐        ┌───────────────────────┐
│   Apache Spark     │        │    Apache Kafka        │
│   Batch Pipeline   │        │  Topic: eventos_inm.  │
│  DataFrames/SQL/RDD│        └──────────┬────────────┘
└────────┬───────────┘                   │
         │                               ▼
         │                  ┌────────────────────────┐
         │                  │  Spark Structured      │
         │                  │  Streaming             │
         │                  │  Ventanas / Alertas    │
         │                  └──────────┬─────────────┘
         │                             │
         ▼                             ▼
┌─────────────────────────────────────────────────────┐
│                   MongoDB                           │
│  propiedades │ resultados_analisis │ pipeline_summary│
│  eventos_streaming │ alertas_streaming               │
└────────────────────────┬────────────────────────────┘
                         │
             ┌───────────┴───────────┐
             ▼                       ▼
   ┌──────────────────┐   ┌─────────────────────┐
   │ Dashboard Batch  │   │  Dashboard en Vivo  │
   │ Análisis hist.   │   │  Tiempo real        │
   └──────────────────┘   └─────────────────────┘
```

### 4.2 Componentes

**Apache Spark (Batch):** Orquesta el procesamiento de los archivos históricos. Implementa tres paradigmas: DataFrames para análisis tabulares de precios y zonas, Spark SQL para consultas declarativas sobre el dataset consolidado, y RDD para transformaciones de bajo nivel.

**Apache Kafka:** Actúa como sistema de mensajería entre el simulador de eventos y el procesamiento streaming. El topic `eventos_inmobiliarios` recibe eventos del productor Python y los entrega al consumidor Spark con garantías de al-menos-una-vez.

**Spark Structured Streaming:** Consume el topic Kafka en micro-batches de 10 segundos. Aplica ventanas deslizantes de 1 minuto para calcular métricas agregadas y detecta anomalías de precio mediante umbrales configurables.

**Hadoop HDFS:** Almacena los archivos de descripciones en texto plano. Spark accede a HDFS para el análisis de contenido textual de los anuncios.

**MongoDB:** Base de datos central del ecosistema. Cada colección tiene un rol específico y los índices están optimizados para las consultas más frecuentes del dashboard.

**Docker Compose:** Orquesta todos los servicios del ecosistema en contenedores aislados y reproducibles. Define la red interna entre componentes, los volúmenes persistentes y el orden de arranque.

---

## 5. Procesamiento Batch (Apache Spark)

### 5.1 Pipeline Batch

El pipeline batch se activa con `python run_pipeline.py` y ejecuta las siguientes etapas en secuencia:

1. **Carga:** Lee los archivos JSON y CSV desde `data/`.
2. **Limpieza:** Elimina registros con precio nulo, normaliza tipos de inmueble y estandariza nombres de distritos.
3. **Análisis con DataFrames:** Calcula precio promedio por distrito, distribución por tipo de inmueble y rango de precios por número de habitaciones.
4. **Análisis con Spark SQL:** Ejecuta queries para obtener los 10 distritos más caros, la correlación precio-área y los portales con mayor cobertura.
5. **Análisis con RDD:** Aplica transformaciones map-reduce para generar un índice de palabras clave en las descripciones.
6. **Carga a MongoDB:** Persiste los resultados en las colecciones correspondientes usando upsert.
7. **Registro de pipeline:** Guarda un documento en `pipeline_summary` con las métricas de la ejecución.

### 5.2 Análisis con DataFrames

```python
# Precio promedio por distrito
df.groupBy("distrito") \
  .agg(avg("precio").alias("precio_promedio"),
       count("*").alias("total_inmuebles")) \
  .orderBy(desc("precio_promedio"))
```

### 5.3 Análisis con Spark SQL

```python
spark.sql("""
    SELECT distrito,
           AVG(precio) as precio_promedio,
           COUNT(*) as cantidad,
           MAX(precio) as precio_maximo
    FROM inmuebles
    WHERE tipo = 'departamento'
    GROUP BY distrito
    ORDER BY precio_promedio DESC
    LIMIT 10
""")
```

### 5.4 Análisis con RDD

```python
# Word count en descripciones
rdd = sc.textFile("hdfs://hadoop:9000/descripciones/")
word_count = rdd.flatMap(lambda line: line.split()) \
               .map(lambda word: (word.lower(), 1)) \
               .reduceByKey(lambda a, b: a + b) \
               .sortBy(lambda x: x[1], ascending=False)
```

---

## 6. Procesamiento Streaming (Apache Kafka + Spark Streaming)

### 6.1 Productor Kafka

El productor (`src/kafka/producer.py`) simula eventos inmobiliarios publicados en tiempo real al topic `eventos_inmobiliarios`. Cada evento tiene la siguiente estructura:

```json
{
  "evento_id": "uuid-generado",
  "tipo": "consulta_precio",
  "zona": "Miraflores",
  "precio_referencial": 285000,
  "timestamp": "2024-11-15T14:32:00Z",
  "fuente": "simulador_v1"
}
```

El productor genera entre 10 y 50 eventos por segundo con distribución aleatoria de zonas y tipos.

### 6.2 Topic Kafka

El topic `eventos_inmobiliarios` está configurado con:

- **Particiones:** 3 (para paralelismo)
- **Factor de replicación:** 1 (entorno de desarrollo)
- **Retención:** 24 horas

Comandos de verificación:

```powershell
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092
docker exec kafka kafka-topics.sh --describe --topic eventos_inmobiliarios --bootstrap-server localhost:9092
```

### 6.3 Spark Structured Streaming

El pipeline de streaming (`src/spark/streaming_analysis.py`) consume el topic Kafka y aplica:

**Lectura del stream:**

```python
df_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "eventos_inmobiliarios") \
    .load()
```

**Ventana deslizante (métricas por zona cada 1 minuto):**

```python
df_windowed = df_stream \
    .withWatermark("timestamp", "2 minutes") \
    .groupBy(window("timestamp", "1 minute"), "zona") \
    .agg(count("*").alias("total_eventos"),
         avg("precio_referencial").alias("precio_promedio"))
```

**Detección de alertas:**

```python
df_alertas = df_windowed.filter(
    (col("precio_promedio") > UMBRAL_PRECIO_ALTO) |
    (col("total_eventos") > UMBRAL_DEMANDA_ALTA)
)
```

### 6.4 Sistema de Alertas

Las alertas se generan automáticamente cuando:

- El precio promedio en una zona supera el umbral configurado (default: 300,000 USD).
- El número de eventos en una ventana de 1 minuto supera el umbral de demanda (default: 100 eventos).

Cada alerta se persiste en la colección `alertas_streaming` de MongoDB con el nivel (WARNING / CRITICAL), zona afectada y valor que disparó la alerta.

---

## 7. MongoDB — Colecciones y Esquema

### 7.1 Colección: `propiedades`

Almacena los inmuebles procesados del pipeline batch.

```json
{
  "_id": "ObjectId",
  "fuente": "adondevivir",
  "tipo": "departamento",
  "distrito": "Miraflores",
  "precio": 285000,
  "area_m2": 95,
  "habitaciones": 3,
  "banos": 2,
  "descripcion": "Departamento moderno con vista al mar...",
  "url": "https://...",
  "pipeline_id": "uuid-ejecucion",
  "fecha_carga": "2024-11-15T12:00:00Z"
}
```

### 7.2 Colección: `resultados_analisis`

Almacena los resultados de los análisis Spark por ejecución.

```json
{
  "_id": "ObjectId",
  "pipeline_id": "uuid-ejecucion",
  "tipo_analisis": "precio_por_distrito",
  "resultado": [
    { "distrito": "San Isidro", "precio_promedio": 320000, "total": 180 },
    { "distrito": "Miraflores", "precio_promedio": 285000, "total": 240 }
  ],
  "fecha": "2024-11-15T12:05:00Z"
}
```

### 7.3 Colección: `eventos_streaming`

```json
{
  "_id": "ObjectId",
  "evento_id": "uuid",
  "tipo": "consulta_precio",
  "zona": "Miraflores",
  "precio_referencial": 285000,
  "timestamp_evento": "2024-11-15T14:32:00Z",
  "timestamp_ingesta": "2024-11-15T14:32:01Z",
  "ventana": "14:32:00-14:33:00"
}
```

### 7.4 Colección: `alertas_streaming`

```json
{
  "_id": "ObjectId",
  "zona": "San Isidro",
  "nivel": "WARNING",
  "tipo_alerta": "precio_alto",
  "valor": 325000,
  "umbral": 300000,
  "ventana": "14:32:00-14:33:00",
  "timestamp": "2024-11-15T14:33:00Z"
}
```

### 7.5 Colección: `pipeline_summary`

```json
{
  "_id": "ObjectId",
  "pipeline_id": "uuid",
  "fecha_inicio": "2024-11-15T12:00:00Z",
  "fecha_fin": "2024-11-15T12:08:00Z",
  "duracion_segundos": 480,
  "registros_procesados": 2570,
  "registros_rechazados": 23,
  "estado": "exitoso",
  "etapas": ["carga", "limpieza", "analisis_batch", "carga_mongodb"]
}
```

---

## 8. GitHub — Control de Versiones

### 8.1 Estructura de Ramas

| Rama                        | Propietario  | Descripción             |
| --------------------------- | ------------ | ----------------------- |
| `main`                      | Todos        | Rama principal estable  |
| `feature/scraping`          | Integrante 1 | Scrapers por portal     |
| `feature/spark-batch`       | Integrante 3 | Pipeline Spark batch    |
| `feature/kafka-streaming`   | Integrante 5 | Kafka + Spark Streaming |
| `feature/mongodb-dashboard` | Integrante 4 | MongoDB y dashboard     |
| `feature/arquitectura`      | Integrante 2 | Docker, infraestructura |

### 8.2 Convención de Commits

```
[Rol N] Descripción breve del cambio

Ejemplos:
[Rol 1] Agregar scraper InfoCasas con paginación
[Rol 3] Implementar análisis RDD para word count
[Rol 5] Documentación AA4 actualizada
```

### 8.3 Verificación del Repositorio

```powershell
# Ver todas las ramas
git branch -a

# Ver log de commits
git log --oneline --graph --all

# Ver contribuidores
git shortlog -sn
```

---

## 9. Visualizaciones

### 9.1 Dashboard Batch

El dashboard de análisis histórico presenta:

- **Gráfico de barras:** Precio promedio por distrito (top 15).
- **Gráfico de torta:** Distribución de inmuebles por tipo.
- **Scatter plot:** Correlación precio vs. área en m².
- **Heatmap:** Densidad de oferta por zona geográfica.
- **Tabla resumen:** Estadísticas descriptivas por portal de origen.

### 9.2 Dashboard en Vivo (Streaming)

El dashboard en tiempo real presenta:

- **Contador de eventos:** Total de eventos procesados en la sesión actual.
- **Gráfico de línea temporal:** Eventos por zona en los últimos 5 minutos.
- **Indicador de precio promedio:** Por zona, actualizado cada 10 segundos.
- **Panel de alertas activas:** Lista de alertas WARNING/CRITICAL con timestamp.
- **Mapa de calor en vivo:** Zonas con mayor actividad de eventos en tiempo real.

---

## 10. Beneficios del Sistema

1. **Centralización de información:** Consolida datos de tres portales distintos en una sola fuente de verdad, eliminando la necesidad de consultar múltiples sitios web manualmente.

2. **Detección temprana de tendencias:** El procesamiento streaming permite identificar cambios de precio o picos de demanda en tiempo real, antes de que se reflejen en análisis batch convencionales.

3. **Escalabilidad horizontal:** La arquitectura basada en Kafka y Spark permite escalar el procesamiento añadiendo brokers o workers sin modificar el código de negocio.

4. **Trazabilidad y auditoría:** El historial de pipelines en `pipeline_summary` permite auditar cada ejecución, identificar errores y comparar resultados entre corridas.

5. **Alertas automatizadas:** El sistema de alertas reduce la carga manual de monitoreo, notificando automáticamente cuando una zona supera umbrales de precio o demanda.

6. **Reproducibilidad:** El uso de Docker Compose garantiza que el ecosistema completo se levante de forma idéntica en cualquier entorno, facilitando el trabajo colaborativo y la evaluación académica.

7. **Preservación de datos históricos:** El uso de upsert en MongoDB asegura que cada ejecución del pipeline acumule datos en lugar de sobrescribirlos, construyendo un historial valioso para análisis temporales.

---

## 11. Métricas de Viabilidad

### Métrica 1 — Latencia de procesamiento streaming

| Indicador                            | Valor         |
| ------------------------------------ | ------------- |
| Latencia promedio (evento → MongoDB) | < 15 segundos |
| Tamaño de micro-batch                | 10 segundos   |
| Eventos procesados por minuto        | 600–1,800     |

El sistema procesa eventos en tiempo casi real, con una latencia máxima de 15 segundos desde la publicación en Kafka hasta la persistencia en MongoDB.

### Métrica 2 — Cobertura y calidad de datos

| Indicador                    | Valor                                  |
| ---------------------------- | -------------------------------------- |
| Registros totales scrapeados | ~2,570                                 |
| Tasa de registros válidos    | ~99.1% (2,547/2,570)                   |
| Portales cubiertos           | 3 (AdondeVivir, InfoCasas, LaEncontre) |
| Distritos representados      | 28+ distritos de Lima                  |

La cobertura de datos es suficiente para análisis estadísticos representativos del mercado inmobiliario limeño.

### Métrica 3 — Rendimiento del pipeline batch

| Indicador                     | Valor           |
| ----------------------------- | --------------- |
| Tiempo total de ejecución     | ~8 minutos      |
| Tiempo análisis Spark         | ~3 minutos      |
| Tiempo carga MongoDB          | ~1 minuto       |
| Registros por segundo (Spark) | ~14 registros/s |

El pipeline batch completo se ejecuta en menos de 10 minutos, permitiendo actualizaciones frecuentes del análisis histórico sin impacto en el sistema operativo.

---

## 12. Conclusiones

1. La integración de Apache Kafka con Spark Structured Streaming amplía significativamente las capacidades del sistema, permitiendo pasar de un análisis retrospectivo a uno en tiempo real sin sacrificar la robustez del pipeline batch existente.

2. La arquitectura de doble flujo (batch + streaming) demuestra ser práctica y complementaria: el batch provee análisis profundos y consolidados, mientras el streaming entrega visibilidad inmediata sobre la dinámica del mercado.

3. MongoDB demostró ser una elección adecuada como base de datos central, gracias a su flexibilidad de esquema para almacenar tanto resultados estructurados de Spark como eventos semi-estructurados del stream.

4. Docker Compose fue clave para garantizar la reproducibilidad del ecosistema, reduciendo significativamente el tiempo de configuración del entorno y eliminando problemas de dependencias entre integrantes del equipo.

5. El sistema de alertas automáticas implementado representa el primer paso hacia un sistema de inteligencia de mercado proactivo, capaz de notificar oportunidades o riesgos sin intervención humana continua.

---

## 13. Estructura de Exposición (15–20 minutos)

| Tiempo | Integrante   | Tema                                                   |
| ------ | ------------ | ------------------------------------------------------ |
| 3 min  | Integrante 1 | Caso de negocio, problema, objetivos, fuentes de datos |
| 3 min  | Integrante 2 | Arquitectura del sistema, flujo batch y streaming      |
| 4 min  | Integrante 3 | Spark: RDD, DataFrames, SQL, Structured Streaming      |
| 3 min  | Integrante 4 | MongoDB: colecciones, dashboard en tiempo real         |
| 4 min  | Integrante 5 | Kafka: topic, productor, conclusiones, GitHub          |
| 2 min  | Todos        | Preguntas del evaluador                                |

---

## Anexos

### Anexo A — Comandos Docker útiles

```powershell
# Ver estado de contenedores
docker-compose ps

# Ver logs de Kafka
docker-compose logs kafka

# Ver logs de Spark
docker-compose logs spark

# Acceder al shell de MongoDB
docker exec -it mongodb mongosh
```

### Anexo B — Verificación de Kafka

```powershell
# Listar topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# Ver mensajes en el topic
docker exec kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic eventos_inmobiliarios --from-beginning --max-messages 5
```

### Anexo C — Consultas MongoDB de verificación

```javascript
// Contar propiedades por portal
db.propiedades.aggregate([{ $group: { _id: "$fuente", total: { $sum: 1 } } }]);

// Ver últimas alertas
db.alertas_streaming.find().sort({ timestamp: -1 }).limit(10);

// Ver historial de pipelines
db.pipeline_summary.find().sort({ fecha_inicio: -1 });
```
