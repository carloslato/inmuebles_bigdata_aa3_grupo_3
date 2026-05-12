#!/usr/bin/env python3
"""
Spark Structured Streaming - Inmuebles Big Data
================================================
Procesa eventos en tiempo real desde Kafka y escribe resultados a MongoDB.

Caracteristicas:
- Lectura de eventos desde Kafka (topic: inmuebles_events)
- Procesamiento con ventanas de tiempo (windowing)
- Deteccion de anomalias con RDD
- Escritura de resultados a MongoDB

Uso:
    spark-submit --master local[*] spark_streaming.py
"""

import os
import sys
import json
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, window, count, avg, countDistinct,
    when, lit, unix_timestamp, expr, struct, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    TimestampType, IntegerType, MapType, LongType, FloatType
)
from pyspark import RDD

# Configuracion
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
MONGO_DB = "inmuebles"

# Topics de Kafka
TOPIC_EVENTS = "inmuebles_events"
TOPIC_ALERTS = "inmuebles_alerts"

# Timeout para el streaming (segundos)
STREAMING_TIMEOUT = 60


def get_spark_session():
    """Crear SparkSession con configuracion Kafka"""
    spark = SparkSession.builder \
        .appName("InmueblesStreaming") \
        .master("local[*]") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.2.0") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"[INFO] SparkSession creada: {spark.sparkContext.appName}")
    return spark


def get_event_schema():
    """Definir schema para los eventos de Kafka"""
    # Schema para el campo data (anidado)
    data_schema = StructType([
        StructField("property_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("district", StringType(), True),
        StructField("bedrooms", IntegerType(), True),
        StructField("bathrooms", IntegerType(), True),
        StructField("area", DoubleType(), True),
        StructField("url", StringType(), True),
        StructField("portal", StringType(), True),
        StructField("description", StringType(), True)
    ])
    
    # Schema principal del evento
    event_schema = StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("data", data_schema, True)
    ])
    
    return event_schema


def write_to_mongodb(df, epoch_id, collection_name):
    """
    Escribir un micro-batch a MongoDB usando pymongo directamente.
    Esta funcion se usa con foreachBatch.
    """
    from pymongo import MongoClient, errors
    
    try:
        # Conectar a MongoDB
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db[collection_name]
        
        # Convertir DataFrame a documentos
        rows = df.collect()
        docs = []
        for row in rows:
            doc = row.asDict()
            # Serializar tipos no JSON
            for key, val in doc.items():
                if hasattr(val, 'isoformat'):
                    doc[key] = val.isoformat()
                elif hasattr(val, '__float__'):
                    doc[key] = float(val) if val is not None else None
                elif hasattr(val, '__int__'):
                    doc[key] = int(val) if val is not None else None
            docs.append(doc)
        
        if docs:
            # Insertar documentos
            try:
                collection.insert_many(docs, ordered=False)
            except errors.BulkWriteError as bwe:
                # Ignorar errores de documentos duplicados
                pass
            
            print(f"[INFO] Epoch {epoch_id}: {len(docs)} documentos insertados en {collection_name}")
        
        client.close()
        
    except Exception as e:
        print(f"[ERROR] Error escribiendo a MongoDB: {e}")


def detect_anomalies_rdd(df):
    """
    Operacion con RDD para detectar anomalias en precios.
    Detecta propiedades con precio > 1M USD o precio < 1000 USD.
    """
    print("[INFO] Ejecutando deteccion de anomalias con RDD...")
    
    rdd = df.rdd
    
    def detectar_anomalia(row):
        """Detectar si un evento tiene precio anomalo"""
        anomalias = []
        try:
            # Convertir Row a dict
            row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
            
            # Obtener data (puede ser Row, dict, o string JSON)
            data = row_dict.get("data")
            
            # Si data es string JSON, parsearlo
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except json.JSONDecodeError:
                    data = {}
            
            # Si data es Row, convertirlo
            if hasattr(data, 'asDict'):
                data = data.asDict()
            
            if isinstance(data, dict):
                price = data.get("price")
                event_id = row_dict.get("event_id", "unknown")
                event_type = row_dict.get("event_type", "unknown")
                
                if price is not None:
                    # Precio muy alto (mas de 1M)
                    if price > 1000000:
                        anomalias.append({
                            "tipo_alerta": "anomalia_precio_alto",
                            "event_id": f"anomalia_{event_id}_{int(time.time())}",
                            "descripcion": f"Precio muy alto detectado: ${price:,.2f} USD",
                            "timestamp": datetime.now().isoformat(),
                            "datos_relacionados": {
                                "event_id_original": str(event_id),
                                "event_type": str(event_type),
                                "price": float(price),
                                "umbral": 1000000
                            }
                        })
                    # Precio muy bajo (menos de 1000)
                    elif price < 1000 and price > 0:
                        anomalias.append({
                            "tipo_alerta": "anomalia_precio_bajo",
                            "event_id": f"anomalia_{event_id}_{int(time.time())}",
                            "descripcion": f"Precio muy bajo detectado: ${price:,.2f} USD",
                            "timestamp": datetime.now().isoformat(),
                            "datos_relacionados": {
                                "event_id_original": str(event_id),
                                "event_type": str(event_type),
                                "price": float(price),
                                "umbral": 1000
                            }
                        })
        except Exception as e:
            print(f"[WARN] Error procesando row en RDD: {e}")
        return anomalias
    
    # Aplicar la funcion a cada row y flattener results
    anomalias_rdd = rdd.flatMap(detectar_anomalia)
    anomalias_lista = anomalias_rdd.collect()
    
    print(f"[INFO] RDD: {len(anomalias_lista)} anomalias detectadas")
    return anomalias_lista


def procesar_alertas_kafka(spark):
    """
    Leer alertas desde el topic de Kafka y escribir a MongoDB.
    """
    print("[INFO] Configurando streaming de alertas desde Kafka...")
    
    try:
        df_alerts = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_ALERTS) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        # Parsear el valor JSON
        alert_schema = StructType([
            StructField("alert_id", StringType(), True),
            StructField("tipo_alerta", StringType(), True),
            StructField("descripcion", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("datos_relacionados", MapType(StringType(), StringType()), True)
        ])
        
        df_alerts_parsed = df_alerts.select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), alert_schema).alias("alert")
        ).select("alert.*")
        
        return df_alerts_parsed
    
    except Exception as e:
        print(f"[WARN] Error configurando alertas Kafka: {e}")
        return None


def main():
    """Funcion principal del streaming"""
    print("=" * 60)
    print("SPARK STRUCTURED STREAMING - INMUEBLES BIG DATA")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"MongoDB: {MONGODB_URI}")
    print(f"Timeout: {STREAMING_TIMEOUT} segundos")
    print("=" * 60)
    
    # Crear SparkSession
    spark = get_spark_session()
    
    # Lista para almacenar anomalias detectadas por RDD
    anomalias_detectadas = []
    
    try:
        # 1. Lectura desde Kafka
        print("\n[1/6] Leyendo desde Kafka...")
        df_kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", TOPIC_EVENTS) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()
        
        print(f"[INFO] DataFrame Kafka creado")
        df_kafka.printSchema()
        
        # 2. Parsear el valor JSON
        print("\n[2/6] Parseando eventos JSON...")
        event_schema = get_event_schema()
        
        df_parsed = df_kafka.select(
            col("key").cast("string").alias("key"),
            from_json(col("value").cast("string"), event_schema).alias("event")
        ).select("event.*")
        
        # Cachear el DataFrame parseado
        df_parsed.cache()
        print(f"[INFO] DataFrame parseado y cacheado")
        df_parsed.printSchema()
        
        # 3. Resumen 1 - Eventos por tipo (window de 10 segundos)
        print("\n[3/6] Configurando resumen de eventos por tipo...")
        eventos_por_tipo = df_parsed \
            .withWatermark("timestamp", "20 seconds") \
            .groupBy(
                window(col("timestamp"), "10 seconds"),
                col("event_type")
            ) \
            .agg(
                count("*").alias("total_eventos"),
                countDistinct("key").alias("eventos_unicos")
            )
        
        print("[INFO] Resumen eventos por tipo configurado")
        
        # 4. Resumen 2 - Precio promedio por distrito (ventana deslizante 30 seg)
        print("\n[4/6] Configurando resumen de precios por distrito...")
        
        # Extraer data.price y data.district para el analisis
        df_with_data = df_parsed.withColumn(
            "data_struct",
            from_json(col("data").cast("string"), get_event_schema()["data"])
        )
        
        precio_promedio_distrito = df_with_data \
            .filter(col("event_type").isin("nueva_propiedad", "cambio_precio")) \
            .withWatermark("timestamp", "30 seconds") \
            .groupBy(
                window(col("timestamp"), "30 seconds", "15 seconds"),
                col("data_struct.district").alias("distrito")
            ) \
            .agg(
                avg("data_struct.price").alias("precio_promedio"),
                count("*").alias("total_propiedades"),
                spark_min("data_struct.price").alias("precio_minimo"),
                spark_max("data_struct.price").alias("precio_maximo")
            )
        
        print("[INFO] Resumen precios por distrito configurado")
        
        # 5. Operacion con RDD - Detectar anomalias
        print("\n[5/6] Ejecutando deteccion de anomalias con RDD...")
        
        # Funcion para procesar batch con RDD
        def process_batch_with_rdd(df, epoch_id):
            """Procesar cada batch detectando anomalias con RDD"""
            # Detectar anomalias
            anomalias = detect_anomalies_rdd(df)
            if anomalias:
                anomalias_detectadas.extend(anomalias)
                print(f"[INFO] Total anomalias acumuladas: {len(anomalias_detectadas)}")
            
            # Escribir eventos a MongoDB
            write_to_mongodb(df, epoch_id, "eventos_streaming")
        
        # 6. Escritura a MongoDB
        print("\n[6/6] Configurando escritura a MongoDB...")
        
        # Query para eventos por tipo (console + MongoDB)
        query_eventos = eventos_por_tipo.writeStream \
            .foreachBatch(lambda df, eid: write_to_mongodb(df, eid, "resumen_eventos_streaming")) \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        print("[INFO] Query eventos por tipo iniciada")
        
        # Query para precios por distrito (MongoDB)
        query_precios = precio_promedio_distrito.writeStream \
            .foreachBatch(lambda df, eid: write_to_mongodb(df, eid, "resumen_precios_streaming")) \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        print("[INFO] Query precios por distrito iniciada")
        
        # Query principal para eventos (con RDD)
        query_eventos_raw = df_parsed.writeStream \
            .foreachBatch(process_batch_with_rdd) \
            .outputMode("update") \
            .trigger(processingTime="5 seconds") \
            .start()
        
        print("[INFO] Query eventos raw iniciada")
        
        # Esperar a que lleguen eventos (timeout)
        print(f"\n[INFO] Esperando eventos por {STREAMING_TIMEOUT} segundos...")
        print("[INFO] Presione Ctrl+C para detener antes del timeout")
        
        # Esperar con timeout
        start_time = time.time()
        try:
            while time.time() - start_time < STREAMING_TIMEOUT:
                time.sleep(5)
                elapsed = int(time.time() - start_time)
                print(f"[INFO] Streaming activo por {elapsed} segundos...")
                
                # Verificar estado de las queries
                if not query_eventos.isRunning or not query_precios.isRunning:
                    print("[WARN] Una de las queries se detuvo")
                    break
                    
        except KeyboardInterrupt:
            print("\n[INFO] Interrupcion por usuario")
        
        # Detener queries
        print("\n[INFO] Deteniendo streaming...")
        query_eventos.stop()
        query_precios.stop()
        query_eventos_raw.stop()
        
        # Esperar a que terminen
        query_eventos.awaitTermination(timeout=10)
        query_precios.awaitTermination(timeout=10)
        query_eventos_raw.awaitTermination(timeout=10)
        
        # Escribir anomalias detectadas a MongoDB
        if anomalias_detectadas:
            print(f"\n[INFO] Escribiendo {len(anomalias_detectadas)} anomalias a MongoDB...")
            write_anomalies_to_mongodb(anomalias_detectadas)
        
        print("\n" + "=" * 60)
        print("STREAMING FINALIZADO")
        print("=" * 60)
        print(f"Eventos procesados: Ver MongoDB colecciones")
        print(f"Anomalias detectadas: {len(anomalias_detectadas)}")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n[ERROR] Error en streaming: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()
        print("[INFO] SparkSession cerrada")


def write_anomalies_to_mongodb(anomalias):
    """Escribir lista de anomalias a MongoDB"""
    from pymongo import MongoClient
    
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client[MONGO_DB]
        collection = db["alertas_streaming"]
        
        if anomalias:
            collection.insert_many(anomalias)
            print(f"[INFO] {len(anomalias)} alertas escritas a alertas_streaming")
        
        client.close()
    except Exception as e:
        print(f"[ERROR] Error escribiendo alertas: {e}")


if __name__ == "__main__":
    main()