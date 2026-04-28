# Clase 5 — Apache Spark + PySpark (Docker)

## Objetivo de la clase
En esta clase se ejecuta un análisis simple de archivos CSV usando **Apache Spark (PySpark)** dentro de Docker.

Se busca aprender:
- Qué es Spark y para qué sirve
- Cómo leer múltiples CSV
- Cómo hacer agregaciones (`groupBy`, `sum`, `avg`)
- Cómo ordenar resultados
- Cómo ejecutar un script con `spark-submit`

---

## Conceptos clave

### ¿Qué es Apache Spark?
Apache Spark es un motor de procesamiento distribuido diseñado para trabajar con grandes volúmenes de datos.

Se usa para:
- procesamiento batch
- análisis de datos
- ETL
- Machine Learning
- streaming

---

### ¿Qué es PySpark?
PySpark es la API de Spark para Python.

Permite trabajar con:
- DataFrames
- consultas tipo SQL
- transformaciones distribuidas

---

## Estructura típica del proyecto

Se trabaja con una estructura similar:

```

hadoop-spark/  
│  
├── data/  
│ ├── ventas1.csv  
│ ├── ventas2.csv  
│ └── ventas3.csv  
│  
├── spark/  
│ └── analisis.py  
│  
└── docker/  
└── docker-compose.yml

````

### Docker compose usado en clase

```yaml
version: "3"

services:
  namenode:
    image: bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8
    container_name: namenode
    ports:
      - "9870:9870"
    environment:
      - CLUSTER_NAME=test-cluster
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    networks:
      - hadoop-network

  datanode:
    image: bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8
    container_name: datanode
    environment:
      - CORE_CONF_fs_defaultFS=hdfs://namenode:9000
    depends_on:
      - namenode
    networks:
      - hadoop-network

networks:
  hadoop-network:
    driver: bridge
```

### Para levantarlo:

```bash
docker compose up -d
```

### Verificar contenedores:

```bash
docker ps
```

### Acceder al namenode:

```bash
docker exec -it namenode bash
```

---

## Script PySpark usado en clase

Archivo: `spark/analisis.py`

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum

spark = SparkSession.builder.appName("AnalisisVentas").getOrCreate()

df = spark.read.csv("/app/data/*.csv", header=True, inferSchema=True)

df.show()

# Total de ventas por categoria
ventas_categoria = df.groupBy("categoria").agg(sum("precio").alias("total"))
ventas_categoria.show()

# Promedio de precios por categoria
promedio = df.groupBy("categoria").agg(avg("precio").alias("promedio"))
promedio.show()

# Top productos por precio
top = df.orderBy(col("precio").desc())
top.show()

spark.stop()
````

---

## Explicación del código

### 1. Crear sesión Spark

```python
spark = SparkSession.builder.appName("AnalisisVentas").getOrCreate()
```

Esto inicializa Spark y permite usar DataFrames.

---

### 2. Leer múltiples CSV

```python
df = spark.read.csv("/app/data/*.csv", header=True, inferSchema=True)
```

- `header=True`: usa la primera fila como nombres de columnas
    
- `inferSchema=True`: detecta automáticamente si un campo es string, int, float, etc.
    
- `/app/data/*.csv`: lee todos los CSV de esa carpeta
    

---

### 3. Mostrar datos

```python
df.show()
```

Imprime las primeras filas.

---

### 4. Agrupar por categoría y sumar precios

```python
ventas_categoria = df.groupBy("categoria").agg(sum("precio").alias("total"))
ventas_categoria.show()
```

---

### 5. Promedio por categoría

```python
promedio = df.groupBy("categoria").agg(avg("precio").alias("promedio"))
promedio.show()
```

---

### 6. Ordenar por precio descendente

```python
top = df.orderBy(col("precio").desc())
top.show()
```

---

## Ejecución del ejemplo con Docker

### Comando usado en clase

Se ejecuta Spark directamente con la imagen oficial.

Desde la carpeta raíz del proyecto:

```bash
docker run -it --rm \
  -v ${PWD}:/app \
  apache/spark:4.0.2-scala2.13-java17-python3-ubuntu \
  /opt/spark/bin/spark-submit /app/spark/analisis.py
```

---

## Explicación del comando Docker

### `docker run`

Ejecuta un contenedor temporal.

### `-it`

Modo interactivo.

### `--rm`

Elimina el contenedor al terminar.

### `-v ${PWD}:/app`

Monta la carpeta actual dentro del contenedor en `/app`.

Así Spark puede acceder a:

- `/app/data`
    
- `/app/spark/analisis.py`
    

### Imagen:

```
apache/spark:4.0.2-scala2.13-java17-python3-ubuntu
```

Incluye Spark y Python listos para ejecutar.

### Ejecuta:

```
/opt/spark/bin/spark-submit /app/spark/analisis.py
```

---

## Problema común: "no reconoce pyspark"

### Caso 1: IDE lo marca en amarillo

Eso es solo un warning local en VSCode, porque en tu PC no está instalado.

Solución opcional para el IDE:

```bash
pip install pyspark
```

Pero OJO:

- No es necesario si ejecutas todo con Docker.
    
- Solo es para que VSCode deje de marcar error.
    

---

## CSV esperado (estructura mínima)

Tus archivos `ventas1.csv`, `ventas2.csv`, etc. deben tener columnas como:

```csv
producto,categoria,precio
Laptop,Tecnologia,2500
Mouse,Tecnologia,50
Camisa,Ropa,80
```

El script asume que existe la columna:

- `categoria`
    
- `precio`
    

Si no existen, fallará.

---

## Comandos útiles de verificación

### Ver qué archivos existen dentro del contenedor

Puedes entrar al contenedor:

```bash
docker run -it --rm -v ${PWD}:/app apache/spark:4.0.2-scala2.13-java17-python3-ubuntu bash
```

Dentro:

```bash
ls -la /app
ls -la /app/data
ls -la /app/spark
```

---

## Resultado esperado

Al ejecutar el script deberías ver en consola:

- Un `df.show()` con datos
    
- Total de ventas por categoría
    
- Promedio por categoría
    
- Tabla ordenada por precio descendente
    

---

## Mini resumen para examen / entrevista

- Spark procesa datos en paralelo usando clusters (aunque aquí usamos modo local en Docker).
    
- `SparkSession` es la puerta de entrada para usar Spark.
    
- `df.groupBy().agg()` permite calcular sumatorias y promedios.
    
- `orderBy(col(...).desc())` ordena datos.
    
- `spark-submit` ejecuta el script distribuido.
    

---

## Checklist para que funcione sí o sí

1. Tener Docker instalado
    
2. Tener esta estructura:
    

```
data/*.csv
spark/analisis.py
```

3. Ejecutar desde la carpeta raíz:
    

```bash
docker run -it --rm -v ${PWD}:/app apache/spark:4.0.2-scala2.13-java17-python3-ubuntu /opt/spark/bin/spark-submit /app/spark/analisis.py
```

---

## Comando alternativo (Windows PowerShell)

Si `${PWD}` falla en PowerShell, usa:

```powershell
docker run -it --rm `
  -v ${PWD}:/app `
  apache/spark:4.0.2-scala2.13-java17-python3-ubuntu `
  /opt/spark/bin/spark-submit /app/spark/analisis.py
```

---

## Notas extra

- Esta clase NO usa HDFS directamente.
    
- Se trabaja con archivos montados localmente al contenedor.
    
- El análisis es básico, pero es la base de cualquier ETL.
    

---

