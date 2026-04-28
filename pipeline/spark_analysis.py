#!/usr/bin/env python3
"""
Spark Analysis Module
====================
Análisis de datos inmobiliarios con PySpark.

Realiza:
1. Análisis de precios (sanitización, estadísticas por ubicación)
2. Análisis de características (dormitorios, baños, áreas comunes)
3. Análisis de ubicaciones (distritos más frecuentes)
4. Correlaciones entre características y precios
5. Análisis de palabras clave en descripciones (top términos por portal)

Uso:
    spark-submit --master local[*] spark_analysis.py --input /path/to/inmuebles.csv --output /path/to/output/
"""

import argparse
import json
import os
import re
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, avg, sum as spark_sum, when, lit,
    regexp_extract, lower, split, explode, desc, asc,
    countDistinct, round as spark_round, stddev, min, max as spark_max, length
)
from pyspark.sql.types import DoubleType, IntegerType


def parse_args():
    parser = argparse.ArgumentParser(description="Spark Analysis for Real Estate Data")
    parser.add_argument("--input", required=True, help="Path to input CSV file")
    parser.add_argument("--output", required=True, help="Path to output directory")
    return parser.parse_args()


def sanitize_price(price_str):
    """
    Sanitiza precios de texto a numérico.
    Maneja formatos como:
    - "S/ 392,773"
    - "$ 240,000"
    - "Desde S/ 293.000"
    - "desde 85000 usd hasta 120000 usd"
    - "$ 2,100,000"
    - "US$ 1,450,000"
    """
    if not price_str or not isinstance(price_str, str):
        return None, None, None
    
    price_lower = price_str.lower().strip()
    
    # Detectar moneda
    currency = "PEN"
    if "$" in price_str or "usd" in price_lower or "us$" in price_lower:
        currency = "USD"
    elif "s/" in price_lower:
        currency = "PEN"
    
    # Buscar rangos "desde X hasta Y"
    range_pattern = r'(?:desde|de|entre)\s*\$?\s*[\d,.\s]+\s*(?:usd|soles|)\s*(?:hasta|a)\s*\$?\s*[\d,.\s]+'
    range_numbers = re.findall(r'[\d,.\s]+', price_str.replace(",", "").replace(".", ""))
    
    numeric_vals = []
    for n in range_numbers:
        n_clean = n.strip()
        if n_clean and n_clean.replace('.', '').replace(',', '').isdigit():
            try:
                val = float(n_clean.replace(',', ''))
                if val > 100:
                    numeric_vals.append(val)
            except ValueError:
                pass
    
    if not numeric_vals:
        return None, None, None
    
    numeric_vals.sort()
    if len(numeric_vals) >= 2 and max(numeric_vals) > min(numeric_vals) * 1.3:
        return min(numeric_vals), max(numeric_vals), currency
    
    return numeric_vals[0], numeric_vals[0], currency


def extract_location_features(df):
    """Extrae distrito y ciudad de la columna ubicacion"""
    df = df.withColumn(
        "distrito",
        when(
            col("ubicacion").contains(","),
            split(col("ubicacion"), ",").getItem(0)
        ).otherwise(col("ubicacion"))
    )
    df = df.withColumn(
        "ciudad",
        when(
            col("ubicacion").contains(","),
            split(col("ubicacion"), ",").getItem(1)
        ).otherwise(lit("Lima"))
    )
    return df


def parse_caracteristicas(df):
    """Extrae m², dormitorios, baños de la columna caracteristicas"""
    df = df.withColumn(
        "area_m2",
        regexp_extract(
            lower(col("caracteristicas")),
            r'(\d+[\.,]?\d*)\s*(?:m[²2]|mts|metros)',
            1
        ).cast(DoubleType())
    )
    df = df.withColumn(
        "area_m2_final",
        when(
            col("area_m2").isNull() | (col("area_m2") == 0),
            regexp_extract(lower(col("area")), r'(\d+)', 1).cast(DoubleType())
        ).otherwise(col("area_m2"))
    )
    df = df.withColumn(
        "dorm_carac",
        regexp_extract(
            lower(col("caracteristicas")),
            r'(\d+)\s*(?:dorm|hab|cuartos|bed)',
            1
        ).cast(IntegerType())
    )
    df = df.withColumn(
        "dormitorios_final",
        when(
            col("dormitorios").isNull() | (col("dormitorios") == 0) | (col("dormitorios") == ""),
            col("dorm_carac")
        ).otherwise(col("dormitorios").cast(IntegerType()))
    )
    df = df.withColumn(
        "banios_carac",
        regexp_extract(
            lower(col("caracteristicas")),
            r'(\d+)\s*(?:bañ|ban|bath|wc)',
            1
        ).cast(IntegerType())
    )
    df = df.withColumn(
        "banios_final",
        when(
            col("banios").isNull() | (col("banios") == 0) | (col("banios") == ""),
            col("banios_carac")
        ).otherwise(col("banios").cast(IntegerType()))
    )
    return df


def analysis_price_by_district(df):
    """Precios promedio por distrito"""
    return (df
        .filter(col("precio_min").isNotNull() & col("distrito").isNotNull())
        .groupBy("distrito")
        .agg(
            count("*").alias("cantidad"),
            spark_round(avg("precio_min"), 2).alias("precio_promedio"),
            spark_round(min("precio_min"), 2).alias("precio_minimo"),
            spark_round(spark_max("precio_min"), 2).alias("precio_maximo"),
            spark_round(stddev("precio_min"), 2).alias("precio_stddev"),
            countDistinct("portal").alias("portales_origen")
        )
        .filter(col("cantidad") >= 3)
        .orderBy(desc("cantidad"))
    )


def analysis_price_by_currency(df):
    """Distribución por moneda"""
    return (df
        .filter(col("moneda").isNotNull())
        .groupBy("moneda")
        .agg(
            count("*").alias("cantidad"),
            spark_round(avg("precio_min"), 2).alias("precio_promedio")
        )
        .orderBy(desc("cantidad"))
    )


def analysis_rooms_distribution(df):
    """Distribución de dormitorios y baños"""
    rooms = (df
        .filter(col("dormitorios_final").isNotNull() & (col("dormitorios_final") > 0) & (col("dormitorios_final") <= 20))
        .groupBy("dormitorios_final")
        .agg(count("*").alias("cantidad"), spark_round(avg("precio_min"), 2).alias("precio_promedio"))
        .orderBy("dormitorios_final")
    )
    baths = (df
        .filter(col("banios_final").isNotNull() & (col("banios_final") > 0) & (col("banios_final") <= 20))
        .groupBy("banios_final")
        .agg(count("*").alias("cantidad"), spark_round(avg("precio_min"), 2).alias("precio_promedio"))
        .orderBy("banios_final")
    )
    return rooms, baths


def analysis_area_vs_price(df):
    """Correlación área vs precio"""
    return (df
        .filter(col("area_m2_final").isNotNull() & (col("area_m2_final") > 20) & (col("area_m2_final") < 1000) & col("precio_min").isNotNull())
        .select(col("area_m2_final").alias("area_m2"), col("precio_min").alias("precio"), col("distrito"), col("portal"))
        .orderBy(desc("area_m2"))
    )


def analysis_portal_comparison(df):
    """Comparación entre portales"""
    return (df
        .filter(col("precio_min").isNotNull())
        .groupBy("portal")
        .agg(
            count("*").alias("cantidad"),
            spark_round(avg("precio_min"), 2).alias("precio_promedio"),
            spark_round(min("precio_min"), 2).alias("precio_minimo"),
            spark_round(spark_max("precio_min"), 2).alias("precio_maximo"),
            spark_round(stddev("precio_min"), 2).alias("precio_stddev"),
            countDistinct("distrito").alias("distritos_distintos"),
            countDistinct("moneda").alias("monedas_usadas")
        )
        .orderBy(desc("cantidad"))
    )


def analysis_top_districts_by_portal(df):
    """Top distritos por portal"""
    return (df
        .filter(col("distrito").isNotNull() & col("portal").isNotNull())
        .groupBy("portal", "distrito")
        .agg(count("*").alias("cantidad"))
        .orderBy("portal", desc("cantidad"))
    )


def analysis_word_frequency(df):
    """Palabras más frecuentes en descripciones"""
    stop_words = set([
        "para", "con", "por", "del", "las", "los", "una", "que",
        "esta", "este", "todo", "más", "muy", "entre", "tiene",
        "sobre", "como", "está", "m2", "s/", "desde", "hasta",
        "son", "cuenta", "cada", "tipo", "alto", "bajo", "solo",
        "también", "forma", "parte", "así", "sino", "donde",
        "nuestro", "porque", "calle", "cerca", "dentro", "casa",
        "departamento", "departamentos", "vivienda", "propiedad",
        "inmueble", "inmuebles", "edificio", "proyecto", "proyectos"
    ])
    
    words = (df
        .filter(col("descripcion").isNotNull() & (length(col("descripcion")) > 10))
        .select(explode(split(lower(col("descripcion")), "\\s+")).alias("word"), col("portal"))
        .filter((length(col("word")) > 3) & ~col("word").isin(list(stop_words)))
        .groupBy("word")
        .agg(count("*").alias("frecuencia"), countDistinct("portal").alias("portales_encontrados"))
        .orderBy(desc("frecuencia"))
        .limit(200)
    )
    return words


def analysis_price_ranges(df):
    """Rangos de precio"""
    df_with_range = df.filter(col("precio_min").isNotNull()).withColumn(
        "rango_precio",
        when(col("precio_min") < 100000, "menos_100k")
        .when(col("precio_min") < 200000, "100k_200k")
        .when(col("precio_min") < 300000, "200k_300k")
        .when(col("precio_min") < 500000, "300k_500k")
        .when(col("precio_min") < 750000, "500k_750k")
        .when(col("precio_min") < 1000000, "750k_1M")
        .otherwise("mas_1M")
    )
    return (df_with_range
        .groupBy("rango_precio")
        .agg(count("*").alias("cantidad"), spark_round(avg("precio_min"), 2).alias("precio_promedio"))
        .orderBy("rango_precio")
    )


def save_results(results, name, output_dir):
    """Guarda un DataFrame como JSON readable"""
    rows = results.collect()
    data = []
    for row in rows:
        d = row.asDict()
        clean = {}
        for k, v in d.items():
            if hasattr(v, '__float__'):
                clean[k] = float(v)
            elif hasattr(v, '__int__'):
                clean[k] = int(v)
            else:
                clean[k] = v
        data.append(clean)
    
    json_path = os.path.join(output_dir, f"{name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"  ✓ {name}.json ({len(data)} registros)")
    return data


def main():
    args = parse_args()
    input_path = args.input
    output_dir = args.output
    os.makedirs(output_dir, exist_ok=True)
    
    print("=" * 60)
    print("SPARK ANALYSIS - INMUEBLES BIG DATA")
    print("=" * 60)
    
    spark = SparkSession.builder \
        .appName("InmueblesAnalysis") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(f"\nLeyendo datos desde: {input_path}")
    
    df = spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("multiLine", "true") \
        .option("encoding", "UTF-8") \
        .csv(input_path)
    
    total_count = df.count()
    print(f"Total registros cargados: {total_count}")
    print("\nSchema:")
    df.printSchema()
    
    # Sanitizar precios
    print("\n[1/8] Sanitizando precios...")
    from pyspark.sql.functions import udf
    sanitize_udf = udf(sanitize_price)
    df = df.withColumn("precio_info", sanitize_udf(col("precio")))
    df = df.withColumn("precio_min", col("precio_info").getItem("_1").cast(DoubleType()))
    df = df.withColumn("precio_max", col("precio_info").getItem("_2").cast(DoubleType()))
    df = df.withColumn("moneda", col("precio_info").getItem("_3"))
    prices_parsed = df.filter(col("precio_min").isNotNull()).count()
    print(f"  Precios parseados: {prices_parsed}/{total_count}")
    
    print("\n[2/8] Extrayendo ubicaciones...")
    df = extract_location_features(df)
    
    print("\n[3/8] Normalizando características...")
    df = parse_caracteristicas(df)
    
    print("\n[4/8] Precios por distrito...")
    save_results(analysis_price_by_district(df), "precios_por_distrito", output_dir)
    
    print("\n[5/8] Distribución por moneda...")
    save_results(analysis_price_by_currency(df), "precios_por_moneda", output_dir)
    
    print("\n[6/8] Dormitorios y baños...")
    rooms, baths = analysis_rooms_distribution(df)
    save_results(rooms, "distribucion_dormitorios", output_dir)
    save_results(baths, "distribucion_banios", output_dir)
    
    print("\n[7/8] Área vs Precio...")
    save_results(analysis_area_vs_price(df), "area_vs_precio", output_dir)
    
    print("\n[8/8] Comparación portales...")
    save_results(analysis_portal_comparison(df), "comparacion_portales", output_dir)
    
    print("\n[+] Análisis adicionales...")
    save_results(analysis_top_districts_by_portal(df), "top_distritos_por_portal", output_dir)
    save_results(analysis_price_ranges(df), "distribucion_rangos_precio", output_dir)
    save_results(analysis_word_frequency(df), "palabras_frecuentes_descripciones", output_dir)
    
    # Estadísticas globales
    print("\nGenerando estadísticas globales...")
    portal_counts = df.groupBy("portal").agg(count("*").alias("count")).collect()
    stats = {
        "total_propiedades": total_count,
        "por_portal": {row["portal"]: row["count"] for row in portal_counts}
    }
    
    price_stats = df.agg(
        spark_round(avg("precio_min"), 2).alias("precio_promedio"),
        spark_round(min("precio_min"), 2).alias("precio_minimo"),
        spark_round(spark_max("precio_min"), 2).alias("precio_maximo")
    ).collect()
    if price_stats:
        stats["precio_promedio"] = float(price_stats[0]["precio_promedio"]) if price_stats[0]["precio_promedio"] else 0
        stats["precio_minimo"] = float(price_stats[0]["precio_minimo"]) if price_stats[0]["precio_minimo"] else 0
        stats["precio_maximo"] = float(price_stats[0]["precio_maximo"]) if price_stats[0]["precio_maximo"] else 0
    
    top_locs = analysis_price_by_district(df).limit(10).select("distrito", "cantidad").collect()
    stats["top_10_distritos"] = [{"distrito": r["distrito"], "cantidad": int(r["cantidad"])} for r in top_locs]
    
    stats_path = os.path.join(output_dir, "estadisticas_globales.json")
    with open(stats_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)
    print(f"  ✓ estadisticas_globales.json")
    
    print("\n" + "=" * 60)
    print("ANÁLISIS SPARK COMPLETADO")
    print("=" * 60)
    print(f"\nArchivos generados en {output_dir}:")
    for f in sorted(os.listdir(output_dir)):
        if f.endswith(".json"):
            fsize = os.path.getsize(os.path.join(output_dir, f))
            print(f"  📊 {f} ({fsize/1024:.1f} KB)")
    
    spark.stop()


if __name__ == "__main__":
    main()