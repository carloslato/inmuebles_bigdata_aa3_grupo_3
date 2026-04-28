# Checklist de Requisitos para Exposición
## Verificación de Cumplimiento — Proyecto Inmobiliario Big Data

> **Propósito:** Este documento verifica que el proyecto cumple con cada requisito establecido por el profesor en las instrucciones de la Evidencia 3. Útil para la exposición y para validar que no se omitió ningún criterio.

---

## Parte A: Definición del Caso

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| A1 | Nombre del caso | ✅ Sí | **"Análisis de Mercado Inmobiliario con Ecosistema Big Data"** | Sección 2.1 |
| A2 | Problema identificado | ✅ Sí | Fragmentación informativa en 3 portales inmobiliarios peruanos, formatos dispares, sin consolidación | Sección 2.2 |
| A3 | Objetivo general de la solución | ✅ Sí | Pipeline automatizado Big Data que capture, transforme, analice y persista datos inmobiliarios de múltiples fuentes | Sección 2.3 |
| A4 | Actores o usuarios involucrados | ✅ Sí | 5 actores: compradores, vendedores/inmobiliarias, inversionistas, analistas de datos, administradores | Sección 2.4 |
| A5 | Justificación del caso | ✅ Sí | 5 razones: volumen, heterogeneidad, valor analítico, automatización, escalabilidad | Sección 2.5 |
| A6 | Continuidad futura con streaming | ✅ Sí | Kafka como bus de eventos + Spark Structured Streaming + MongoDB sink + notificaciones en tiempo real | Sección 2.6 |

---

## Parte B: Análisis de Requerimientos

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| B1 | Necesidades funcionales | ✅ Sí | 9 requerimientos funcionales identificados (RF-01 a RF-09) con tabla detallada | Sección 3.1 |
| B2 | Necesidades técnicas | ✅ Sí | 8 requerimientos técnicos (RT-01 a RT-08): Docker, Hadoop 3.2.1, Spark 3.5.0, MongoDB 7.0, Python 3.11, Scrapling, Network, volúmenes | Sección 3.2 |
| B3 | Origen y naturaleza de los datos | ✅ Sí | 3 fuentes: AdondeVivir (HTML/CSS), InfoCasas (JSON-LD), LaEncontre (CSS selectors) | Sección 3.3 |
| B4 | Problemas de calidad de datos | ✅ Sí | 6 problemas identificados: precios múltiples formatos, rangos, ubicaciones no estandarizadas, características mezcladas, datos faltantes, HTML embebido | Sección 3.4 |
| B5 | Requerimientos del procesamiento | ✅ Sí | Volumen (~8.6 MB), Velocidad (<15 min), Variedad (6 formatos), Veracidad (sanitización), Disponibilidad (MongoDB + dashboard) | Sección 3.5 |

---

## Parte C: Diseño de Base de Datos y Modelo de Datos (MongoDB)

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| C1 | Nombre de la base de datos | ✅ Sí | `inmuebles` | Sección 5.1 |
| C2 | Colecciones definidas | ✅ Sí | 4 colecciones: `propiedades`, `resultados_analisis`, `wordcount_results`, `pipeline_summary` | Sección 5.2 |
| C3 | Atributos principales | ✅ Sí | 16 atributos en `propiedades`, 5 en `resultados_analisis`, 6 en `wordcount_results`, 6 en `pipeline_summary` | Sección 5.2 |
| C4 | Identificadores | ✅ Sí | `_id` (ObjectId autogenerado) como PK en todas las colecciones. Índices en: portal, precio, ubicación, dormitorios, latitud | Sección 5.2 |
| C5 | Relaciones lógicas entre colecciones | ✅ Sí | 3 relaciones documentadas: pipeline_summary → resultados_analisis (1:N), pipeline_summary → wordcount_results (1:N), propiedades ↔ resultados_analisis (lógica) | Sección 5.3 |
| C6 | Justificación del modelo documental | ✅ Sí | 5 razones: esquema flexible, documentos anidados, consultas analíticas, escalabilidad horizontal, integración PySpark | Sección 5.4 |
| C7 | Ejemplos de documentos | ✅ Sí | Documento JSON de ejemplo para cada colección (propiedades, resultados_analisis, wordcount_results) | Sección 5.2 |

---

## Parte D: Diseño del Procesamiento de Datos

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| D1 | Cómo ingresan los archivos al ecosistema | ✅ Sí | Via scraping web → JSON → el orquestador los copia a volúmenes compartidos y al input-data de Hadoop | Sección 6.1 — Paso 1 |
| D2 | Qué datos serán leídos con Spark | ✅ Sí | CSV estructurado (`inmuebles.csv`) con 17 campos y ~2,567 registros | Sección 6.1 — Paso 5 |
| D3 | Qué transformaciones se aplicarán | ✅ Sí | Sanitización de precios (UDF), extracción de ubicaciones (split), parseo de características (regexp_extract), creación de columnas derivadas | Sección 6.1 — Paso 5, Sección 3.4 |
| D4 | Cómo se integrarán las fuentes | ✅ Sí | Los 3 JSONs de portales se consolidan en `inmuebles_todos.json`, luego se transforman a un CSV unificado para Spark y a archivos MD para Hadoop | Sección 6.1 — Pasos 2-5 |
| D5 | Qué datos finales se almacenarán en MongoDB | ✅ Sí | Spark results → `resultados_analisis`, Hadoop WordCount → `wordcount_results`, resumen → `pipeline_summary` | Sección 6.1 — Paso 6 |
| D6 | Diagrama de arquitectura | ✅ Sí | Diagrama ASCII completo mostrando: Docker Compose, pipeline container, Hadoop cluster, MongoDB, Dashboard, volúmenes compartidos y flujo de datos | Sección 6.4 |

---

## Parte E: Prototipo Funcional

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| E1 | Entorno desplegado correctamente | ✅ Sí | 8 contenedores Docker orquestados con `docker compose up --build -d` | Sección 8.1 |
| E2 | Acceso a las tecnologías requeridas | ✅ Sí | Spark, Hadoop (5 nodos), MongoDB y dashboard accesibles desde la red Docker | Sección 8.2 |
| E3 | Lectura de archivos | ✅ Sí | Spark lee CSV desde pipeline_output, Hadoop lee .md/.txt desde /input-data vía HDFS | Sección 6.1, 6.2, 6.3 |
| E4 | Procesamiento básico con Spark | ✅ Sí | 10 análisis con DataFrames, UDF con StructType, groupBy, agg, explode, regexp_extract, window functions | Sección 6.3 |
| E5 | Persistencia o carga de resultados en MongoDB | ✅ Sí | 3 colecciones pobladas automáticamente al final del pipeline | Sección 6.1 — Paso 6 |

---

## Parte F: Beneficios del Diseño

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| F1 | 5 beneficios tangibles e intangibles | ✅ Sí | 1. Automatización completa (tangible), 2. Consolidación multi-fuente (tangible), 3. Escalabilidad horizontal (intangible), 4. Trazabilidad y reproductibilidad (intangible), 5. Preparación para streaming (intangible) | Sección 9 |

---

## Parte G: Viabilidad de la Propuesta

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| G1 | Métrica de rendimiento | ✅ Sí | Throughput: 5.3 props/seg (completo), 14.2 props/seg (solo procesamiento) | Sección 10 — Métrica 1 |
| G2 | Métrica de tiempo | ✅ Sí | Tiempo total: ~8 min (completo), ~3 min (solo procesamiento), ~15 min (timeout máx) | Sección 10 — Métrica 2 |
| G3 | Métrica de esfuerzo | ✅ Sí | ~2,434 líneas de código en 5 lenguajes, 40-60 horas-hombre estimadas | Sección 10 — Métrica 3 |

---

## Parte H: Mejores Prácticas

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| H1 | 5 mejores prácticas Big Data | ✅ Sí | 1. Separación de responsabilidades (microservicios), 2. Inmutabilidad de datos (Data Lake), 3. Sanitización en capa ETL, 4. Monitoreo y observabilidad, 5. Idempotencia y reproducibilidad | Sección 11 |

---

## Parte I: Uso de Apache Spark

| # | Requisito | ¿Se cumple? | ¿Cómo se cumple? | Dónde está en el informe |
|---|-----------|-------------|-------------------|--------------------------|
| I1 | Apache Spark | ✅ Sí | PySpark 3.5.0 en modo local[*] | Sección 6.3 |
| I2 | Spark RDD y/o DataFrame | ✅ Sí | DataFrames con API funcional (groupBy, agg, filter, select, orderBy, explode, join) | Sección 6.3, spark_analysis.py |
| I3 | Spark SQL | ✅ Sí | Funciones SQL nativas: regexp_extract, split, when, lower, length, countDistinct, stddev, col() | Sección 6.3, spark_analysis.py |

---

## Requisitos Generales Mínimos

| # | Requisito | ¿Se cumple? | Detalle |
|---|-----------|-------------|---------|
| R1 | 5 archivos de entrada mínimo | ✅ Sí | 11 archivos (4 JSON, 1 CSV, 5 MD, 1 TXT) |
| R2 | 3 formatos diferentes mínimo | ✅ Sí | 6 formatos: JSON, CSV, MD, TXT, HTML, Java |
| R3 | Docker como entorno | ✅ Sí | docker-compose.yml con 8 servicios |
| R4 | Apache Hadoop | ✅ Sí | 5 nodos (NameNode, DataNode, RM, NM, HS) + WordCount |
| R5 | Apache Spark | ✅ Sí | PySpark 3.5.0 con 10 análisis |
| R6 | MongoDB | ✅ Sí | MongoDB 7.0, 4 colecciones con índices |
| R7 | Datos generados con IA / simulados / públicos | ✅ Sí | Datos reales extraídos de 3 portales web públicos |
| R8 | Estructura del informe según orden | ✅ Sí | 13 secciones en orden especificado (Portada → Introducción → ... → Referencias) |
| R9 | Prototipo funcional demostrable | ✅ Sí | `docker compose up --build -d` → dashboard en http://localhost:8080 |
| R10 | Diagrama de arquitectura | ✅ Sí | Diagrama ASCII en Sección 6.4 |

---

## Preguntas Frecuentes para la Exposición

### Pregunta 1: ¿Por qué usaron Scrapling y no BeautifulSoup o Selenium?

**Respuesta:** Scrapling ofrece scraping asíncrono nativo (async/await), manejo automático de cookies y fingerprints de navegador (Playwright), y es más resistente a bloqueos por rate limiting. BeautifulSoup es solo parsing HTML (no descarga), y Selenium es más pesado para scraping a gran escala. Scrapling es el equilibrio ideal para nuestro caso.

### Pregunta 2: ¿Cuántos datos procesaron exactamente?

**Respuesta:** Procesamos 2,567 propiedades distribuidas en: AdondeVivir (~1,800), InfoCasas (~450), LaEncontre (~317). El JSON consolidado pesa ~4.46 MB y el CSV estructurado ~1.2 MB. Esto equivale a aproximadamente 5.3 propiedades por segundo de procesamiento.

### Pregunta 3: ¿Dónde está el valor agregado del pipeline? ¿No es más fácil abrir cada portal?

**Respuesta:** El valor agregado está en la **consolidación y análisis**. Abrir cada portal individualmente solo muestra propiedades sueltas. Con nuestro pipeline puedes responder preguntas como: *"¿Cuál es el precio promedio de un departamento de 3 dormitorios en Miraflores, comparando los 3 portales?"* Eso es imposible de obtener manualmente sin un trabajo de horas o días.

### Pregunta 4: ¿Cómo manejan los diferentes formatos de precio?

**Respuesta:** Implementamos una UDF (User Defined Function) de Spark que aplica expresiones regulares para detectar moneda (S/, $, US$, USD), extraer valores numéricos (manejando comas como separadores de miles), y detectar rangos ("desde X hasta Y"). La UDF retorna un StructType con tres campos: precio_min, precio_max, moneda.

### Pregunta 5: ¿Por qué MongoDB y no SQL?

**Respuesta:** Los datos inmobiliarios son naturalmente **documentales** y **heterogéneos**. Un departamento en AdondeVivir puede tener campo "extras" con datos que InfoCasas no tiene. MongoDB maneja esto sin problemas (esquema flexible). Además, los resultados de análisis contienen arrays de datos que se almacenan naturalmente como documentos embebidos. Si usáramos SQL, tendríamos que normalizar todo en tablas separadas con joins complejos.

### Pregunta 6: ¿El scraper funciona siempre? ¿Qué pasa si los portales cambian su HTML?

**Respuesta:** El scraper depende de selectores CSS específicos de cada portal. Si un portal rediseña su página, los selectores pueden fallar. El pipeline está diseñado para **fallar gracefully**: si el scraper falla, el paso 1 reporta el error y el pipeline se detiene, preservando datos de ejecuciones anteriores en MongoDB. Para mantenerlo funcionando, los selectores deben actualizarse periódicamente. Esta es una limitación conocida del web scraping.

### Pregunta 7: ¿Cómo se garantiza que el pipeline sea reproducible?

**Respuesta:** Cada ejecución tiene un `pipeline_id` único basado en timestamp. Antes de insertar nuevos datos, se limpian las colecciones anteriores (`delete_many({})`). El pipeline genera un reporte JSON completo con todos los pasos. Si falla, se corrige el error y se re-ejecuta; los resultados serán consistentes porque las transformaciones son determinísticas.

### Pregunta 8: ¿Qué pasa si Hadoop tarda demasiado o falla?

**Respuesta:** El orquestador espera hasta 10 minutos por Hadoop (configurable). Si Hadoop no completa en ese tiempo, el pipeline continúa igual (Step 4 es "best-effort"). Hadoop escribe una señal de completado (`.hadoop_complete`) en el volumen compartido, y el pipeline la detecta. Si no hay señal, simplemente se salta la carga de resultados de WordCount a MongoDB.

### Pregunta 9: ¿Qué pasa con el dashboard si el pipeline no ha terminado?

**Respuesta:** El dashboard tiene un modo de auto-refresh cada 15 segundos mientras el pipeline está en ejecución. Cada paso del pipeline tiene un indicador visual (pendiente → corriendo → completado → falló). Si abres el dashboard antes de que termine, verás los pasos completados hasta el momento y los pendientes. Cuando el pipeline termina, el auto-refresh se detiene y se muestran todos los resultados finales.

### Pregunta 10: ¿Cómo se integraría Kafka para la fase de streaming?

**Respuesta:** En lugar de que el scraper escriba archivos JSON, cada propiedad extraída se publicaría como un mensaje en un topic de Kafka ("nuevas-propiedades"). Spark Structured Streaming consumiría ese topic en micro-batches, aplicaría las mismas transformaciones (sanitización de precios, extracción de ubicaciones), y escribiría los resultados en MongoDB en tiempo real. El dashboard se actualizaría automáticamente con los nuevos datos. Además, podríamos agregar un topic de "alertas" para notificar cuando aparezca una propiedad que cumpla criterios predefinidos (ej: precio < $100,000 en Miraflores).

### Pregunta 11: ¿Cuánto tiempo tomaría escalar esto a 100,000 propiedades?

**Respuesta:** Nuestro pipeline actual procesa 2,567 propiedades en ~8 minutos. Teóricamente, 100,000 propiedades tomarían ~5.2 horas si el scraper escala linealmente. Sin embargo, Spark con `local[*]` está limitado a los recursos de una sola máquina. En un clúster real con 4 nodos (8 cores cada uno), el tiempo de procesamiento de Spark se reduciría dramáticamente. Hadoop HDFS distribuiría los datos naturalmente. El cuello de botella principal sería el scraping, que se paraleliza fácilmente con más spiders.

### Pregunta 12: ¿Por qué incluyeron WordCount de Hadoop si Spark también puede contar palabras?

**Respuesta:** Hadoop WordCount es el "Hello World" del procesamiento MapReduce y era un requisito del curso demostrar el uso de Hadoop. Elegimos aplicarlo a las descripciones de propiedades porque tiene sentido de negocio: las palabras más frecuentes en descripciones revelan qué características valoran más los vendedores ("vista", "cochera", "seguridad", "remodelado"). Además, demostramos ambas tecnologías: Hadoop para procesamiento batch clásico y Spark para análisis más complejos, mostrando que no son excluyentes sino complementarias.

### Pregunta 13: ¿Qué librerías de Python usaron y por qué?

**Respuesta:**
- **Scrapling** → Scraping asíncrono anti-detección
- **PyMongo** → Driver oficial MongoDB
- **csv / json / shutil / glob** → Librerías estándar para transformación
- **re** → Expresiones regulares para sanitización/precios (en Spark UDF)

No usamos Pandas ni NumPy directamente porque Spark DataFrames ya proporcionan toda la funcionalidad necesaria.

### Pregunta 14: ¿El proyecto se puede ejecutar en cualquier computadora?

**Respuesta:** Sí, siempre que tenga Docker Desktop instalado y al menos 8 GB de RAM disponibles. El proyecto es completamente auto-contenido: el docker-compose.yml descarga todas las imágenes necesarias (MongoDB 7.0, Hadoop, Nginx) y construye la imagen del pipeline automáticamente. Solo se necesita ejecutar `docker compose up --build -d` y esperar ~8-15 minutos.

### Pregunta 15: ¿Cómo verifican que los resultados de Spark sean correctos?

**Respuesta:** Usamos varias técnicas:
- **Validación cruzada:** Contamos registros totales y por portal, y verificamos que sumen correctamente (2,567 totales = suma de los 3 portales)
- **Rangos lógicos:** Verificamos que los precios sanitizados estén en rangos esperados (> $10,000 y < $10,000,000)
- **Datos nulos:** Filtramos con `.isNotNull()` y reportamos cuántos registros se descartaron en cada análisis
- **Estadísticas descriptivas:** La desviación estándar y los valores min/max nos permiten identificar outliers

---

## Resumen Visual para la Exposición

```
╔══════════════════════════════════════════════════════════════╗
║           MAPA DE CUMPLIMIENTO - EVALUACIÓN                 ║
╠══════════════════════════════════════════════════════════════╣
║                                                              ║
║  A. Definición del caso         ████████████ 100% ✅        ║
║  B. Análisis de requerimientos  ████████████ 100% ✅        ║
║  C. Diseño BD MongoDB           ████████████ 100% ✅        ║
║  D. Diseño procesamiento        ████████████ 100% ✅        ║
║  E. Prototipo funcional         ████████████ 100% ✅        ║
║  F. Beneficios (5)              ████████████ 100% ✅        ║
║  G. Métricas (3)                ████████████ 100% ✅        ║
║  H. Mejores prácticas (5)       ████████████ 100% ✅        ║
║  I. Apache Spark (RDD/DF/SQL)   ████████████ 100% ✅        ║
║                                                              ║
║  Requisitos mínimos:                                        ║
║  ├── 5+ archivos entrada        ████████████ 100% ✅        ║
║  ├── 3+ formatos diferentes     ████████████ 100% ✅        ║
║  ├── Docker                     ████████████ 100% ✅        ║
║  ├── Hadoop                     ████████████ 100% ✅        ║
║  ├── Spark                      ████████████ 100% ✅        ║
║  ├── MongoDB                    ████████████ 100% ✅        ║
║  ├── Datos reales/simulados     ████████████ 100% ✅        ║
║  └── Prototipo demostrable      ████████████ 100% ✅        ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
```

---

## Orden Sugerido de Exposición (15-20 min)

| Minuto | Tema | Quién expone | Diapositivas clave |
|--------|------|-------------|-------------------|
| 0-2 | 📌 Caso y problemática | Integrante 1 | Fragmentación portales, problema |
| 2-4 | 📊 Datos utilizados | Integrante 1 | 11 archivos, 6 formatos, tabla |
| 4-6 | 🗄️ Modelo MongoDB | Integrante 2 | 4 colecciones, documento ejemplo |
| 6-8 | 🏗️ Arquitectura | Integrante 2 | Diagrama ASCII, flujo ETL |
| 8-11 | ⚡ Procesamiento Spark | Integrante 3 | 10 análisis, UDF sanitización |
| 11-13 | 🐳 Prototipo funcional | Integrante 4 | Comandos, live demo |
| 13-14 | ✅ Beneficios | Integrante 4 | 5 beneficios |
| 14-15 | 📐 Métricas | Integrante 5 | Throughput, tiempo, esfuerzo |
| 15-16 | 📚 Mejores prácticas | Integrante 5 | 5 prácticas Big Data |
| 16-18 | 🔮 Continuidad streaming | Integrante 3 | Kafka + Spark Streaming |
| 18-20 | ❓ Preguntas | Todo el equipo | FAQ preparada |

---

*Documento generado para la exposición de la Evidencia 3 — Grupo 3*
*Curso: Big Data I — Instituto CERTUS — Ciclo 5*