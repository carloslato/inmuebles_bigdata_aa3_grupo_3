# 🏢 Proyecto Inmobiliario Big Data

**Pipeline completo de Big Data para análisis de mercado inmobiliario**

[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)](https://docker.com)
[![Hadoop](https://img.shields.io/badge/Hadoop-3.2.1-66CCFF?logo=apachehadoop)](https://hadoop.apache.org)
[![Spark](https://img.shields.io/badge/Spark-3.5.0-E25A1C?logo=apachespark)](https://spark.apache.org)
[![MongoDB](https://img.shields.io/badge/MongoDB-7.0-47A248?logo=mongodb)](https://mongodb.com)
[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python)](https://python.org)

---

## 📋 Descripción

Este proyecto implementa un **flujo completo de Big Data** para analizar datos del mercado inmobiliario de Lima, Perú. El pipeline integra:

| Herramienta | Propósito |
|-------------|-----------|
| 🕸️ **Scraper** | Extrae ~18,000 propiedades de 3 portales inmobiliarios (Python + Scrapling) |
| 🐘 **Hadoop** | Procesamiento MapReduce (WordCount) sobre descripciones de propiedades |
| ⚡ **Spark** | Análisis de precios, ubicaciones, correlaciones y palabras clave (PySpark) |
| 🗄️ **MongoDB** | Almacenamiento de datos crudos y resultados de análisis |
| 📊 **Dashboard** | Visualización interactiva con charts, nube de palabras y estado del pipeline |

**Todo se ejecuta automáticamente con un solo `docker compose up`.**

---

## 🚀 Quick Start

### Requisitos

- **Docker** 24+ y **Docker Compose** v2+
- Al menos **8 GB RAM** disponible para los contenedores
- **10 GB espacio libre** en disco

### Ejecución

```bash
# 1. Clonar el repositorio
cd inmuebles_bigdata_aa3_grupo_3

# 2. Crear directorio de salida (output de pipeline)
mkdir -p pipeline_output

# 3. Levantar todo (se construye la imagen del pipeline automáticamente)
docker compose up --build -d

# 4. Ver logs del pipeline (se ejecuta automáticamente)
docker compose logs -f pipeline

# 5. Abrir el dashboard
#    → http://localhost:8080
```

### Ver resultados de cada componente

```bash
# Estado del pipeline en tiempo real
docker compose logs -f pipeline

# Hadoop NameNode UI
# → http://localhost:9870

# Dashboard (cuando el pipeline termina)
# → http://localhost:8080

# MongoDB (directamente)
docker exec -it mongodb mongosh inmuebles
```

---

## 🏗️ Arquitectura del Pipeline

```
docker compose up
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│                     PIPELINE DE BIG DATA                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌──────────────────┐                                           │
│  │  STEP 1: Scraper  │  Extrae datos de portales → JSON        │
│  │  (Python/Scrapl.) │  ~18,000 propiedades                    │
│  └────────┬─────────┘                                           │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │ STEP 2: Transform│  JSON → CSV + Descripciones MD           │
│  │ (Python/Pandas)  │  CSV a Hadoop input-data                 │
│  └────────┬─────────┘  MD con descripciones                    │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │STEP 3: MongoDB   │  Carga datos crudos a MongoDB            │
│  │ (PyMongo)        │  Colección: propiedades                  │
│  └────────┬─────────┘  Índices: portal, precio, ubicacion      │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │STEP 4: Hadoop    │  WordCount sobre descripciones           │
│  │ (MapReduce/Java) │  HDFS → Procesa → Resultados → JSON     │
│  └────────┬─────────┘                                          │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │STEP 5: Spark     │  8 análisis diferentes:                  │
│  │ (PySpark)        │  ├ Precios por distrito                  │
│  │                  │  ├ Distribución monedas                  │
│  │                  │  ├ Dormitorios/baños                     │
│  │                  │  ├ Área vs precio                        │
│  │                  │  ├ Comparación portales                  │
│  │                  │  ├ Top distritos por portal              │
│  │                  │  ├ Rangos de precio                      │
│  │                  │  └ Palabras frecuentes                   │
│  └────────┬─────────┘                                          │
│           ▼                                                     │
│  ┌──────────────────┐                                           │
│  │STEP 6: Resultados│  Guarda todo en MongoDB                  │
│  │ (PyMongo)        │  Colecciones: resultados_analisis,       │
│  └──────────────────┘  wordcount_results, pipeline_summary     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DASHBOARD (http://localhost:8080)             │
│                                                                 │
│  🔁 Estado del Pipeline en vivo (6 steps)                      │
│  📊 3 tarjetas de resumen (propiedades, precio, distritos)     │
│  📈 6 tabs con 12 charts y tablas interactivas:                │
│     ├ 💰 Precios (distrito, rango, moneda, área vs precio)    │
│     ├ 📍 Ubicaciones (top distritos, precio por distrito)     │
│     ├ 🏠 Características (dormitorios, baños)                 │
│     ├ 🌐 Portales (comparación, top distritos por portal)     │
│     ├ 📝 Palabras Clave (nube, WordCount Hadoop)              │
│     └ 🗃️ Datos RAW (preview de datos, MongoDB)               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📁 Estructura del Proyecto

```
inmuebles_bigdata_aa3_grupo_3/
│
├── docker-compose.yml              ← Orquestador principal
├── .dockerignore
│
├── apache-hadoop/                  ← Cluster Hadoop (clase 1)
│   ├── docker-compose.yml          ← (original, no usar)
│   ├── namenode_entrypoint.sh      ← Entrypoint con wait + WordCount + JSON
│   ├── hadoop.env                  ← Configuración Hadoop
│   ├── src/WordCount.java          ← Código MapReduce
│   ├── input-data/                 ← Datos de entrada (pipeline escribe aquí)
│   └── ...
│
├── scrape-data/                    ← Scraper (clase 1 - funcional)
│   ├── main.py                     ← 3 spiders (AdondeVivir, LaEncontre, InfoCasas)
│   ├── requirements.txt
│   └── docs/scraper_documentacion.md
│
├── spark/                          ← Apuntes de clase (Spark)
│   └── clase-5-spark.md
│
├── pipeline/                       ← Contenedor orquestador
│   ├── Dockerfile                  ← Imagen con Spark + Python + Scrapling
│   ├── requirements.txt
│   ├── run_pipeline.py             ← Orquestador de 6 pasos
│   ├── status_manager.py           ← Estado del pipeline para dashboard
│   └── spark_analysis.py           ← 8 análisis con PySpark
│
├── dashboard/                      ← Dashboard web
│   ├── index.html                  ← HTML + JS con Chart.js
│   └── nginx.conf                  ← Sirve dashboard + /data/ (outputs)
│
└── pipeline_output/                ← Output compartido (bind mount)
    ├── pipeline_status.json        ← Estado en vivo del pipeline
    ├── json/                       ← JSONs del scraper
    ├── csv/inmuebles.csv           ← Datos estructurados para Spark
    ├── descriptions/               ← Archivos .md para Hadoop
    ├── hadoop_output/              ← Resultados WordCount
    ├── spark_results/              ← Resultados de Spark (JSONs)
    └── pipeline_report.json        ← Reporte final
```

---

## 🔬 Análisis Realizados

### Spark (8 análisis)

| Análisis | Archivo | Descripción |
|----------|---------|-------------|
| Precios por distrito | `precios_por_distrito.json` | Cantidad, promedio, min, max, desviación por distrito |
| Distribución moneda | `precios_por_moneda.json` | PEN vs USD |
| Distribución dormitorios | `distribucion_dormitorios.json` | Cantidad y precio por # dormitorios |
| Distribución baños | `distribucion_banios.json` | Cantidad y precio por # baños |
| Área vs Precio | `area_vs_precio.json` | Correlación scatter |
| Comparación portales | `comparacion_portales.json` | Estadísticas por portal |
| Top distritos/portal | `top_distritos_por_portal.json` | Ranking por portal |
| Palabras frecuentes | `palabras_frecuentes_descripciones.json` | Top 200 palabras |
| Rangos de precio | `distribucion_rangos_precio.json` | Segmentación en 7 rangos |
| Estadísticas globales | `estadisticas_globales.json` | Resumen general |

### Hadoop

- **WordCount** sobre descripciones de propiedades (archivos .md)
- Resultados ordenados por frecuencia descendente
- Output disponible en MongoDB y archivo `hadoop_wordcount.json`

### MongoDB

| Colección | Contenido |
|-----------|-----------|
| `propiedades` | ~18,000 documentos con todos los campos de cada propiedad |
| `resultados_analisis` | Resultados de Spark (por tipo de análisis) |
| `wordcount_results` | Resultados de Hadoop WordCount |
| `pipeline_summary` | Resumen de cada ejecución del pipeline |

---

## 🧠 Sanitización de Precios

El módulo de Spark maneja formatos complejos de precios:

| Formato | Resultado |
|---------|-----------|
| `S/ 392,773` | 392773 PEN |
| `$ 240,000` | 240000 USD |
| `Desde S/ 293.000` | 293000 PEN |
| `desde 85000 usd hasta 120000 usd` | 85000-120000 USD |
| `US$ 1,450,000` | 1450000 USD |
| `$ 2,100,000` | 2100000 USD |

Se extrae: precio mínimo, precio máximo y moneda (PEN/USD).

---

## 📊 Dashboard

El dashboard en `http://localhost:8080` muestra:

- **🔁 Pipeline Status**: 6 pasos con indicadores visuales (pendiente, ejecutando, completado, falló)
- **📊 Stats Overview**: Total propiedades, precio promedio, top distritos
- **📈 6 Tabs de análisis**:
  - **Precios**: Bar chart por distrito, doughnut rangos, doughnut moneda, scatter área vs precio
  - **Ubicaciones**: Top 15 distritos (cantidad y precio)
  - **Características**: Distribución de dormitorios y baños
  - **Portales**: Comparación entre portales + tabla top distritos por portal
  - **Palabras Clave**: Nube de palabras + tabla WordCount de Hadoop
  - **Datos RAW**: Vista previa de datos Spark + colecciones MongoDB

El dashboard se actualiza automáticamente cada 15 segundos mientras el pipeline está en ejecución.

---

## 🔧 Troubleshooting

### Error: "pipeline_output bind mount failed"

```bash
# Crear el directorio manualmente antes de docker compose up
mkdir -p pipeline_output
```

### Error: Spark no encuentra el CSV

```bash
# Verificar que el pipeline generó el CSV
docker exec pipeline ls -la /pipeline_output/csv/
```

### Error: Hadoop no encuentra datos

```bash
# Verificar que el pipeline copió archivos al input-data
docker exec namenode ls -la /input-data/
```

### Error: MongoDB no arranca

```bash
# Verificar healthcheck
docker compose ps mongodb
docker compose logs mongodb
```

### Ver logs de cada servicio

```bash
docker compose logs -f pipeline     # Pipeline principal
docker compose logs -f namenode     # Hadoop NameNode
docker compose logs -f dashboard    # Nginx dashboard
docker compose logs -f mongodb      # MongoDB
```

---

## 📚 Referencias

- [Apache Hadoop](https://hadoop.apache.org/) - MapReduce + HDFS
- [Apache Spark](https://spark.apache.org/) - PySpark DataFrames
- [MongoDB](https://www.mongodb.com/) - Document database
- [Scrapling](https://github.com/D4Vinci/Scrapling) - Python web scraping library
- [Chart.js](https://www.chartjs.org/) - JavaScript charting library
- [Docker Compose](https://docs.docker.com/compose/) - Multi-container orchestration

---

## 📝 Licencia

Proyecto académico - Instituto CERTUS - Ciclo 5 - Big Data

**Grupo 3**