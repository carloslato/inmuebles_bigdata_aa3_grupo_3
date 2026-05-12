# 🏠 Análisis de Mercado Inmobiliario — Ecosistema Big Data (AA4)

> Proyecto académico del Grupo 3 — Análisis de datos del mercado inmobiliario peruano usando un ecosistema Big Data completo con procesamiento batch y streaming en tiempo real.

---

## 📋 Descripción

Este proyecto implementa un ecosistema Big Data end-to-end para el análisis del mercado inmobiliario peruano. Integra scraping de múltiples portales, procesamiento distribuido con Apache Spark (batch y streaming), mensajería con Apache Kafka, almacenamiento NoSQL con MongoDB y visualización mediante dashboards interactivos.

**AA4** extiende el sistema AA3 incorporando:

- ✅ Procesamiento en **tiempo real** con Kafka + Spark Structured Streaming
- ✅ **Dashboard en vivo** con actualización automática de eventos
- ✅ **Historial de pipelines** con métricas por ejecución
- ✅ **Sistema de alertas** automáticas por anomalías de precio o demanda

---

## 🏗️ Arquitectura

```
Fuentes Históricas          Flujo Batch
─────────────────           ───────────
JSON / CSV / MD   ──────►  Spark Batch ──► MongoDB ──► Dashboard
                                       ──► HDFS

Simulador Eventos           Flujo Streaming
─────────────────           ───────────────
Python Script     ──────►  Kafka ──► Spark Streaming ──► MongoDB ──► Dashboard Live
```

Ver diagrama completo en [`docs/arquitectura_aa4.md`](docs/arquitectura_aa4.md)

---

## 🛠️ Tecnologías

| Tecnología           | Versión | Rol                                 |
| -------------------- | ------- | ----------------------------------- |
| 🔥 Apache Spark      | 3.x     | Procesamiento batch y streaming     |
| 📨 Apache Kafka      | 3.x     | Mensajería y streaming de eventos   |
| 🐘 Apache Hadoop     | 3.x     | Almacenamiento HDFS                 |
| 🍃 MongoDB           | 6.x     | Base de datos NoSQL                 |
| 🐍 Python            | 3.10+   | Scrapers, productores, orquestación |
| 🐳 Docker Compose    | Latest  | Contenedorización                   |
| 📊 Matplotlib / Dash | Latest  | Visualizaciones y dashboard         |

---

## 📁 Estructura del Proyecto

```
proyecto-inmobiliario/
│
├── data/                          # Archivos de datos
│   ├── inmuebles_adondevivir.json # ~1,800 registros scrapeados
│   ├── inmuebles_infocasas.json   # ~450 registros scrapeados
│   ├── inmuebles_laencontre.json  # ~320 registros scrapeados
│   ├── inmuebles_todos.json       # Dataset consolidado (~2,570)
│   ├── inmuebles.csv              # Input Spark Batch
│   └── descripciones_*.md         # Archivos descriptivos para Hadoop
│
├── src/
│   ├── scrapers/                  # Scripts de scraping por portal
│   ├── kafka/
│   │   ├── producer.py            # Productor de eventos Kafka
│   │   └── consumer.py            # Consumidor (referencia)
│   ├── spark/
│   │   ├── batch_analysis.py      # Pipeline batch principal
│   │   └── streaming_analysis.py  # Pipeline streaming Kafka
│   ├── mongodb/
│   │   └── mongo_loader.py        # Carga y consultas MongoDB
│   └── dashboard/
│       ├── dashboard_batch.py     # Dashboard análisis histórico
│       └── dashboard_live.py      # Dashboard en tiempo real
│
├── docs/
│   ├── arquitectura_aa4.md        # Diagrama y descripción de arquitectura
│   ├── grupo4_Evidencia4.md       # Informe completo AA4
│   ├── indicaciones_aa4.md        # Lista de verificación del profesor
│   ├── informe_instrucciones.md   # Instructivo del informe
│   └── evidencias/                # Capturas de funcionamiento
│
├── docker-compose.yml             # Orquestación de contenedores
├── run_pipeline.py                # Script principal de ejecución
└── README.md
```

---

## 🚀 Instrucciones de Ejecución

### Prerrequisitos

- Docker Desktop instalado y corriendo
- Python 3.10+
- PowerShell (Windows)

### 1. Levantar el ecosistema

```powershell
docker-compose up -d
```

Verificar que todos los contenedores estén corriendo:

```powershell
docker-compose ps
```

Contenedores esperados: `spark`, `kafka`, `zookeeper`, `hadoop`, `mongodb`

### 2. Ejecutar el pipeline batch

```powershell
python run_pipeline.py
```

Esto ejecuta: carga de datos → Spark batch → análisis → carga MongoDB → dashboard.

### 3. Iniciar el productor Kafka (streaming)

```powershell
python src/kafka/producer.py
```

### 4. Iniciar Spark Structured Streaming

```powershell
docker exec -it spark spark-submit src/spark/streaming_analysis.py
```

### 5. Ver el dashboard en vivo

```powershell
python src/dashboard/dashboard_live.py
```

Abrir navegador en: `http://localhost:8050`

---

## 📊 Datos Utilizados

| Archivo                    | Formato    | Registros     | Fuente               | Uso                |
| -------------------------- | ---------- | ------------- | -------------------- | ------------------ |
| inmuebles_adondevivir.json | JSON       | ~1,800        | Scraping AdondeVivir | Datos crudos batch |
| inmuebles_infocasas.json   | JSON       | ~450          | Scraping InfoCasas   | Datos crudos batch |
| inmuebles_laencontre.json  | JSON       | ~320          | Scraping LaEncontre  | Datos crudos batch |
| inmuebles_todos.json       | JSON       | ~2,570        | Consolidación        | Dataset unificado  |
| inmuebles.csv              | CSV        | ~2,570        | Transformación       | Input Spark Batch  |
| descripciones\_\*.md       | MD         | 3 archivos    | Transformación       | Input Hadoop       |
| eventos_inmobiliarios      | JSON/Kafka | 1,000–3,000   | Simulación           | Spark Streaming    |
| pipeline_summary           | BSON       | Por ejecución | Pipeline             | Historial          |
| alertas_streaming          | BSON       | Variable      | Spark Streaming      | Alertas activas    |

---

## 👥 Integrantes del Grupo 3

| Rol          | Nombre             | Responsabilidad                        |
| ------------ | ------------------ | -------------------------------------- |
| Integrante 1 | [Apellido, Nombre] | Caso de negocio, datos, scraping       |
| Integrante 2 | [Apellido, Nombre] | Arquitectura, infraestructura Docker   |
| Integrante 3 | [Apellido, Nombre] | Spark: RDD, DataFrames, SQL, Streaming |
| Integrante 4 | [Apellido, Nombre] | MongoDB, dashboard en tiempo real      |
| Integrante 5 | [Apellido, Nombre] | Kafka, documentación, arquitectura     |

---

## 📝 Documentación

- [Informe completo AA4](docs/grupo4_Evidencia4.md)
- [Diagrama de arquitectura](docs/arquitectura_aa4.md)
- [Indicaciones del profesor](docs/indicaciones_aa4.md)
- [Evidencias de funcionamiento](docs/evidencias/)
