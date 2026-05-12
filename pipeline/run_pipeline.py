#!/usr/bin/env python3
"""
Pipeline Orchestrator
=====================
Orquesta todo el flujo de Big Data para el proyecto inmobiliario.

Flujo:
1. Ejecutar scraper → JSON
2. Transformar JSON → CSV y descripciones MD
3. Cargar datos a MongoDB
4. Generar eventos en Kafka (producer)
5. Ejecutar Hadoop WordCount sobre descripciones
6. Ejecutar Spark Structured Streaming desde Kafka
7. Ejecutar Spark para análisis de datos (batch)
8. Guardar resultados en MongoDB

Este script se ejecuta dentro del contenedor 'pipeline' de Docker.
"""

import os
import sys
import time
import json
import subprocess
from datetime import datetime

# Añadir directorio actual al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from status_manager import (
    init_pipeline, start_step, complete_step, fail_step, complete_pipeline
)

# Configuración de rutas compartidas
SCRAPER_DIR = "/app/scrape-data"
PIPELINE_DIR = "/app/pipeline"
OUTPUT_DIR = "/pipeline_output"
HADOOP_INPUT_DIR = "/app/apache-hadoop/input-data"
MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
MONGO_DB = "inmuebles"


def log(msg):
    """Log with timestamp"""
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] [PIPELINE] {msg}")
    sys.stdout.flush()


def run_cmd(cmd, cwd=None, timeout=None):
    """Run a command and return (returncode, stdout, stderr)"""
    log(f"Running: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        if result.stdout:
            for line in result.stdout.split("\n"):
                if line.strip():
                    log(f"  {line.strip()}")
        if result.stderr:
            for line in result.stderr.split("\n"):
                if line.strip():
                    log(f"  [ERR] {line.strip()}")
        return result.returncode, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        log(f"  [TIMEOUT] Command timed out after {timeout}s")
        return -1, "", "timeout"
    except Exception as e:
        log(f"  [EXCEPTION] {e}")
        return -1, "", str(e)


def step_scraper():
    """Step 1: Run the scraper"""
    log("=" * 60)
    log("STEP 1/7: Extrayendo datos de portales inmobiliarios (Scraper)")
    log("=" * 60)
    start_step("scraper")

    try:
        # Ejecutar el scraper directamente con python
        rc, out, err = run_cmd(
            ["python", "main.py"],
            cwd=SCRAPER_DIR,
            timeout=1200  # 10 min max
        )

        if rc != 0:
            raise Exception(f"Scraper failed with code {rc}: {err}")

        # Verificar que se generaron los archivos JSON
        import glob as gl
        json_files = gl.glob(os.path.join(SCRAPER_DIR, "inmuebles_*.json"))
        log(f"JSON files generated: {json_files}")

        stats = {}
        for jf in json_files:
            with open(jf, "r", encoding="utf-8") as f:
                data = json.load(f)
            name = os.path.basename(jf).replace("inmuebles_", "").replace(".json", "")
            count = len(data) if isinstance(data, list) else 1
            stats[f"scraped_{name}"] = count
            log(f"  {name}: {count} propiedades")

        # Copiar JSONs al output compartido
        os.makedirs(os.path.join(OUTPUT_DIR, "json"), exist_ok=True)
        for jf in json_files:
            dest = os.path.join(OUTPUT_DIR, "json", os.path.basename(jf))
            import shutil
            shutil.copy2(jf, dest)
            log(f"  Copied {os.path.basename(jf)} to shared output")

        complete_step("scraper", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] Scraper error: {e}")
        fail_step("scraper", str(e))
        return False, {}


def step_extract_csv():
    """Step 2: Transform JSON to CSV and descriptions MD"""
    log("=" * 60)
    log("STEP 2/7: Transformando datos a CSV y descripciones MD")
    log("=" * 60)
    start_step("extract_csv")

    try:
        # Cargar datos consolidados
        todos_json = os.path.join(SCRAPER_DIR, "inmuebles_todos.json")
        if not os.path.exists(todos_json):
            # Buscar en output compartido
            todos_json = os.path.join(OUTPUT_DIR, "json", "inmuebles_todos.json")

        if not os.path.exists(todos_json):
            raise Exception("No se encontro inmuebles_todos.json")

        with open(todos_json, "r", encoding="utf-8") as f:
            todos = json.load(f)

        log(f"Cargados {len(todos)} registros totales")

        # --- 2a: Generar CSV con datos estructurados ---
        import csv
        os.makedirs(os.path.join(OUTPUT_DIR, "csv"), exist_ok=True)

        # Campos a incluir en el CSV
        fieldnames = [
            "portal", "precio", "titulo", "ubicacion", "direccion",
            "descripcion", "caracteristicas", "dormitorios", "banios",
            "area", "latitud", "longitud", "extras", "url", "agencia",
            "tipologia", "etiquetas", "tipo_publicacion"
        ]

        csv_path = os.path.join(OUTPUT_DIR, "csv", "inmuebles.csv")
        with open(csv_path, "w", encoding="utf-8-sig", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for item in todos:
                # Sanitizar strings para CSV
                row = {}
                for k in fieldnames:
                    val = item.get(k, "")
                    if isinstance(val, str):
                        val = val.replace("\n", " ").replace("\r", " ")
                    row[k] = val
                writer.writerow(row)

        log(f"CSV generado: {csv_path}")

        # Copiar CSV para que Hadoop lo use
        shutil_path = os.path.join(HADOOP_INPUT_DIR, "inmuebles.csv")
        import shutil
        shutil.copy2(csv_path, shutil_path)
        log(f"CSV copiado a input-data de Hadoop: {shutil_path}")

        # --- 2b: Generar archivos MD por portal con descripciones ---
        os.makedirs(os.path.join(OUTPUT_DIR, "descriptions"), exist_ok=True)
        os.makedirs(HADOOP_INPUT_DIR, exist_ok=True)

        # Separar descripciones por portal
        portal_texts = {}
        for item in todos:
            portal = item.get("portal", "unknown")
            if portal not in portal_texts:
                portal_texts[portal] = []
            desc = item.get("descripcion", "").strip()
            if desc:
                portal_texts[portal].append(desc)

        md_files_created = []
        for portal, descs in portal_texts.items():
            md_content = f"# Descripciones de propiedades - Portal: {portal}\n\n"
            md_content += f"Total descripciones: {len(descs)}\n\n"
            md_content += "---\n\n"
            for i, desc in enumerate(descs, 1):
                md_content += f"### Propiedad {i}\n\n{desc}\n\n---\n\n"

            md_filename = f"descripciones_{portal}.md"
            md_path = os.path.join(OUTPUT_DIR, "descriptions", md_filename)
            with open(md_path, "w", encoding="utf-8") as f:
                f.write(md_content)

            # Tambien copiar al input-data de Hadoop para WordCount
            hadoop_md_path = os.path.join(HADOOP_INPUT_DIR, md_filename)
            shutil.copy2(md_path, hadoop_md_path)

            md_files_created.append(md_filename)
            log(f"MD generado: {md_filename} ({len(descs)} descripciones)")

        stats = {
            "total_propiedades": len(todos),
            "csv_registros": len(todos),
            "archivos_md": len(md_files_created),
            "portales": ", ".join(portal_texts.keys())
        }

        complete_step("extract_csv", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] Extract error: {e}")
        fail_step("extract_csv", str(e))
        return False, {}


def step_mongodb_load():
    """Step 3: Load data into MongoDB"""
    log("=" * 60)
    log("STEP 3/7: Cargando datos a MongoDB")
    log("=" * 60)
    start_step("mongodb_load")

    try:
        from pymongo import MongoClient

        # Cargar datos JSON consolidados
        todos_json = os.path.join(SCRAPER_DIR, "inmuebles_todos.json")
        if not os.path.exists(todos_json):
            todos_json = os.path.join(OUTPUT_DIR, "json", "inmuebles_todos.json")

        with open(todos_json, "r", encoding="utf-8") as f:
            todos = json.load(f)

        log(f"Cargando {len(todos)} documentos a MongoDB...")

        # Conectar a MongoDB (con reintentos)
        client = None
        for attempt in range(10):
            try:
                client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
                client.admin.command("ping")
                log("Conexion a MongoDB establecida")
                break
            except Exception as e:
                log(f"Esperando MongoDB (intento {attempt+1}/10): {e}")
                time.sleep(3)
        else:
            raise Exception("No se pudo conectar a MongoDB despues de 10 intentos")

        db = client[MONGO_DB]
        collection = db["propiedades"]

        # Generar pipeline_id para trazabilidad
        pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        log(f"Pipeline ID: {pipeline_id}")
        
        # Insertar datos preservando historial (upsert por property_id)
        insertados = 0
        actualizados = 0
        for item in todos:
            item["pipeline_id"] = pipeline_id
            item["fecha_carga"] = datetime.now().isoformat()
            # Upsert basado en property_id + portal para evitar duplicados
            property_id = item.get("property_id", f"{item.get('portal', 'unknown')}_{item.get('url', '')}")
            result = collection.update_one(
                {"property_id": property_id},
                {"$set": item},
                upsert=True
            )
            if result.upserted_id:
                insertados += 1
            else:
                actualizados += 1
        
        log(f"Procesados {len(todos)} documentos: {insertados} insertados, {actualizados} actualizados")
        log(f"Documentos totales en coleccion: {collection.count_documents({})}")

        # Crear indices para busquedas rapidas
        collection.create_index("portal")
        collection.create_index("precio")
        collection.create_index("ubicacion")
        collection.create_index("dormitorios")
        collection.create_index("latitud")
        log("Indices creados en MongoDB")

        client.close()

        stats = {
            "mongodb_documentos": len(todos),
            "mongodb_coleccion": "propiedades",
            "mongodb_db": MONGO_DB
        }

        complete_step("mongodb_load", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] MongoDB load error: {e}")
        fail_step("mongodb_load", str(e))
        return False, {}


def step_kafka_events():
    """Step 4: Generar eventos de Kafka para simulación en tiempo real"""
    log("=" * 60)
    log("STEP 4/8: Generando eventos inmobiliarios en Kafka (Producer)")
    log("=" * 60)
    start_step("kafka_events")

    try:
        from kafka_producer import run_producer

        # Configuración desde variables de entorno o valores por defecto
        num_events = int(os.environ.get("KAFKA_NUM_EVENTS", "1500"))
        delay = float(os.environ.get("KAFKA_EVENT_DELAY", "0.1"))

        log(f"Configuración: {num_events} eventos, delay={delay}s")

        # Ejecutar productor
        stats = run_producer(num_events=num_events, delay=delay)

        complete_step("kafka_events", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] Kafka events error: {e}")
        fail_step("kafka_events", str(e))
        return False, {}


def step_hadoop_wordcount():
    """
    Step 5: Wait for Hadoop WordCount to complete
    Hadoop se ejecuta en su propio contenedor (namenode) y escribe
    directamente en /host_output/ que es la raiz de pipeline_output/.
    Buscamos el archivo .hadoop_complete como senal de finalizacion.
    """
    log("=" * 60)
    log("STEP 5/8: Hadoop WordCount sobre descripciones")
    log("=" * 60)
    start_step("hadoop_wordcount")

    try:
        # Hadoop escribe directamente en OUTPUT_DIR (/host_output mapeado a pipeline_output/)
        log("Esperando que Hadoop WordCount termine...")
        log(f"  Buscando archivos en: {OUTPUT_DIR}")

        # Esperar hasta que aparezca la senal de completado o archivos part-r-*
        max_wait = 600  # 10 min max
        waited = 0
        found = False

        while waited < max_wait:
            # Verificar senal de completado (.hadoop_complete)
            complete_signal = os.path.join(OUTPUT_DIR, ".hadoop_complete")
            if os.path.exists(complete_signal):
                log("  Senal .hadoop_complete encontrada!")
                found = True
                break

            # Verificar archivos de output part-r-* directamente en OUTPUT_DIR
            hadoop_files = []
            if os.path.exists(OUTPUT_DIR):
                hadoop_files = [f for f in os.listdir(OUTPUT_DIR)
                                if f.startswith("part-") or f == "hadoop_wordcount.json"]

            if hadoop_files:
                log(f"  Archivos de Hadoop encontrados: {hadoop_files}")
                found = True
                break

            time.sleep(5)
            waited += 5
            if waited % 30 == 0:
                log(f"  Esperando Hadoop... ({waited}s)")

        if not found:
            log("  [WARN] No se encontraron archivos de Hadoop despues del timeout. Continuando...")
        else:
            # Mover/enlazar archivos de Hadoop a hadoop_output para claridad
            hadoop_output_dir = os.path.join(OUTPUT_DIR, "hadoop_output")
            os.makedirs(hadoop_output_dir, exist_ok=True)

            for fname in os.listdir(OUTPUT_DIR):
                if fname.startswith("part-") or fname == "hadoop_wordcount.json" or fname.startswith(".hadoop"):
                    src = os.path.join(OUTPUT_DIR, fname)
                    dst = os.path.join(hadoop_output_dir, fname)
                    if os.path.isfile(src) and not os.path.exists(dst):
                        import shutil
                        shutil.copy2(src, dst)
                        log(f"  Copiado {fname} a hadoop_output/")

            log("Hadoop WordCount completado")

        stats = {"hadoop_completado": found}
        if found:
            # Calcular tamano total de archivos Hadoop
            hadoop_files_size = 0
            for f in os.listdir(OUTPUT_DIR):
                if f.startswith("part-") or f == "hadoop_wordcount.json":
                    fpath = os.path.join(OUTPUT_DIR, f)
                    if os.path.isfile(fpath):
                        hadoop_files_size += os.path.getsize(fpath)
            stats["hadoop_output_size"] = hadoop_files_size

        complete_step("hadoop_wordcount", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] Hadoop step error: {e}")
        fail_step("hadoop_wordcount", str(e))
        return False, {}


def step_spark_streaming():
    """Step 6: Ejecutar Spark Structured Streaming"""
    log("=" * 60)
    log("STEP 6/8: Spark Structured Streaming desde Kafka")
    log("=" * 60)
    start_step("spark_streaming")

    try:
        spark_script = os.path.join(PIPELINE_DIR, "spark_streaming.py")
        if not os.path.exists(spark_script):
            raise Exception(f"Spark streaming script not found: {spark_script}")

        # Ejecutar Spark submit con timeout de 90 segundos (60s streaming + overhead)
        rc, out, err = run_cmd(
            [
                "spark-submit",
                "--master", "local[*]",
                "--name", "InmueblesStreaming",
                "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                spark_script
            ],
            cwd=PIPELINE_DIR,
            timeout=90
        )

        if rc != 0:
            # No fallar si es solo timeout, el streaming puede haber procesado eventos
            log(f"[WARN] Spark streaming termino con codigo {rc}: {err}")

        # Verificar resultados en MongoDB
        from pymongo import MongoClient
        stats = {"streaming_ejecutado": True}
        
        try:
            client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
            db = client[MONGO_DB]
            
            # Contar documentos en cada coleccion
            for coll_name in ["eventos_streaming", "resumen_eventos_streaming", 
                              "resumen_precios_streaming", "alertas_streaming"]:
                try:
                    count = db[coll_name].count_documents({})
                    stats[coll_name] = count
                    log(f"  {coll_name}: {count} documentos")
                except Exception as e:
                    stats[coll_name] = 0
                    log(f"  {coll_name}: 0 documentos (no existe o error)")
            
            client.close()
        except Exception as e:
            log(f"[WARN] No se pudo verificar MongoDB: {e}")
            stats["mongodb_verificacion"] = False

        complete_step("spark_streaming", stats)
        return True, stats

    except subprocess.TimeoutExpired:
        log("[INFO] Spark streaming timeout (esperado, continuando...)")
        # Timeout es esperado, el streaming se ejecuto por 60 segundos
        stats = {"streaming_timeout": True, "streaming_ejecutado": True}
        complete_step("spark_streaming", stats)
        return True, stats
        
    except Exception as e:
        log(f"  [FAIL] Spark streaming error: {e}")
        fail_step("spark_streaming", str(e))
        return False, {}


def step_spark_analysis():
    """Step 7: Run Spark analysis"""
    log("=" * 60)
    log("STEP 7/8: Analisis con Spark (batch)")
    log("=" * 60)
    start_step("spark_analysis")

    try:
        spark_script = os.path.join(PIPELINE_DIR, "spark_analysis.py")
        if not os.path.exists(spark_script):
            raise Exception(f"Spark script not found: {spark_script}")

        # Ejecutar Spark submit
        csv_path = os.path.join(OUTPUT_DIR, "csv", "inmuebles.csv")
        output_path = os.path.join(OUTPUT_DIR, "spark_results")

        rc, out, err = run_cmd(
            [
                "spark-submit",
                "--master", "local[*]",
                "--name", "InmueblesAnalysis",
                spark_script,
                "--input", csv_path,
                "--output", output_path
            ],
            cwd=PIPELINE_DIR,
            timeout=300
        )

        if rc != 0:
            raise Exception(f"Spark analysis failed with code {rc}: {err}")

        # Verificar resultados
        results = {}
        if os.path.exists(output_path):
            for f in os.listdir(output_path):
                fpath = os.path.join(output_path, f)
                if os.path.isfile(fpath):
                    try:
                        with open(fpath, "r", encoding="utf-8") as fh:
                            results[f] = json.load(fh)
                    except (json.JSONDecodeError, IOError):
                        pass

        log(f"Spark analysis results: {list(results.keys())}")

        stats = {
            "spark_resultados": list(results.keys()),
            "spark_output_dir": output_path
        }

        complete_step("spark_analysis", stats)

        return True, stats

    except Exception as e:
        log(f"  [FAIL] Spark analysis error: {e}")
        fail_step("spark_analysis", str(e))
        return False, {}


def step_mongodb_results():
    """Step 8: Save analysis results to MongoDB"""
    log("=" * 60)
    log("STEP 8/8: Guardando resultados de analisis en MongoDB")
    log("=" * 60)
    start_step("mongodb_results")

    try:
        from pymongo import MongoClient

        # Conectar a MongoDB
        client = None
        for attempt in range(5):
            try:
                client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
                client.admin.command("ping")
                log("Conexion a MongoDB establecida")
                break
            except Exception as e:
                log(f"Esperando MongoDB (intento {attempt+1}/5): {e}")
                time.sleep(3)
        else:
            raise Exception("No se pudo conectar a MongoDB")

        db = client[MONGO_DB]

        # Guardar resultados de Spark (preservando historial)
        spark_results_dir = os.path.join(OUTPUT_DIR, "spark_results")
        results_collection = db["resultados_analisis"]
        pipeline_id = datetime.now().strftime("%Y%m%d_%H%M%S")

        if os.path.exists(spark_results_dir):
            for fname in os.listdir(spark_results_dir):
                fpath = os.path.join(spark_results_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        with open(fpath, "r", encoding="utf-8") as fh:
                            data = json.load(fh)
                        doc = {
                            "pipeline_id": pipeline_id,
                            "tipo_analisis": fname.replace(".json", ""),
                            "archivo": fname,
                            "fecha": datetime.now().isoformat(),
                            "data": data
                        }
                        results_collection.insert_one(doc)
                        log(f"Resultado guardado en MongoDB: {fname}")
                    except Exception as e:
                        log(f"  Error guardando {fname}: {e}")

        # Guardar resumen de Hadoop WordCount (top palabras) - preservando historial
        # Buscar en: hadoop_output/ (organizado) o en la raiz de OUTPUT_DIR (donde Hadoop los escribe)
        wordcount_collection = db["wordcount_results"]

        hadoop_dirs_to_check = [
            os.path.join(OUTPUT_DIR, "hadoop_output"),
            OUTPUT_DIR
        ]

        part_files = []
        wordcount_json = None

        for hdir in hadoop_dirs_to_check:
            if os.path.exists(hdir):
                for fname in os.listdir(hdir):
                    fpath = os.path.join(hdir, fname)
                    if os.path.isfile(fpath):
                        if fname.startswith("part-"):
                            part_files.append(fpath)
                        elif fname == "hadoop_wordcount.json":
                            wordcount_json = fpath

        # Si no se genero el JSON en Hadoop (porque python3 no esta en la imagen), lo generamos aqui
        if not wordcount_json and part_files:
            log("  Generando hadoop_wordcount.json desde archivos part-r-*...")
            words = []
            for pf in part_files:
                with open(pf, "r", encoding="utf-8", errors="ignore") as fh:
                    for line in fh:
                        line = line.strip()
                        if "\t" in line:
                            word, count = line.split("\t", 1)
                            try:
                                words.append({"palabra": word, "frecuencia": int(count)})
                            except ValueError:
                                pass
            words.sort(key=lambda x: x["frecuencia"], reverse=True)
            wordcount_json = os.path.join(OUTPUT_DIR, "hadoop_wordcount.json")
            with open(wordcount_json, "w", encoding="utf-8") as f:
                json.dump(words, f, ensure_ascii=False, indent=2)
            log(f"  WordCount JSON generado con {len(words)} palabras")

        # Cargar WordCount JSON en MongoDB
        if wordcount_json and os.path.exists(wordcount_json):
            with open(wordcount_json, "r", encoding="utf-8") as f:
                wc_data = json.load(f)
            doc = {
                "tipo": "hadoop_wordcount",
                "archivo": "hadoop_wordcount.json",
                "fecha": datetime.now().isoformat(),
                "total_palabras": len(wc_data),
                "data": wc_data[:1000]  # Top 1000 palabras
            }
            wordcount_collection.insert_one(doc)
            log(f"WordCount guardado en MongoDB ({len(wc_data)} palabras)")

        # Tambien guardar los archivos part-r-* raw
        for pf in part_files:
            with open(pf, "r", encoding="utf-8", errors="ignore") as fh:
                content = fh.read()
            doc = {
                "tipo": "hadoop_wordcount_raw",
                "archivo": os.path.basename(pf),
                "fecha": datetime.now().isoformat(),
                "contenido": content[:50000]  # Limitar tamano
            }
            wordcount_collection.insert_one(doc)
            log(f"WordCount raw guardado en MongoDB: {os.path.basename(pf)}")

        # Generar documento de resumen del pipeline
        status = __import__("status_manager", fromlist=["get_status"]).get_status()
        started_at = status.get("started_at")
        completed_at = status.get("completed_at")
        duration_seconds = None
        if started_at and completed_at:
            try:
                duration_seconds = (datetime.fromisoformat(completed_at) - datetime.fromisoformat(started_at)).total_seconds()
            except Exception:
                duration_seconds = None

        summary_doc = {
            "pipeline_id": datetime.now().strftime("%Y%m%d_%H%M%S"),
            "fecha": datetime.now().isoformat(),
            "fecha_ejecucion": datetime.now().isoformat(),
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_seconds": duration_seconds,
            "estado": status.get("pipeline", "unknown"),
            "stats": status.get("stats", {}),
            "steps": status.get("steps", [])
        }
        db["pipeline_summary"].insert_one(summary_doc)
        log("Resumen del pipeline guardado en MongoDB")

        client.close()

        stats = {
            "mongodb_resultados": True,
            "colecciones": ["resultados_analisis", "wordcount_results", "pipeline_summary"]
        }

        complete_step("mongodb_results", stats)
        return True, stats

    except Exception as e:
        log(f"  [FAIL] MongoDB results error: {e}")
        fail_step("mongodb_results", str(e))
        return False, {}


def start_status_consumer():
    """Iniciar el consumer de pipeline_status en segundo plano"""
    log("Iniciando Pipeline Status Consumer en segundo plano...")
    import subprocess
    try:
        # Iniciar consumer en segundo plano
        process = subprocess.Popen(
            ["python", "pipeline_status_consumer.py"],
            cwd=PIPELINE_DIR,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            start_new_session=True
        )
        log(f"Consumer iniciado con PID: {process.pid}")
        return process
    except Exception as e:
        log(f"Error iniciando consumer: {e}")
        return None


def main():
    """Main pipeline orchestrator"""
    log("=" * 60)
    log("INICIANDO PIPELINE DE BIG DATA - INMUEBLES")
    log("=" * 60)
    log(f"Timestamp: {datetime.now().isoformat()}")

    # Inicializar estado del pipeline
    init_pipeline()

    # Configurar Kafka topics
    log("\n")
    log("Configurando Kafka...")
    rc, out, err = run_cmd(
        ["python", "kafka_setup.py"],
        cwd=PIPELINE_DIR,
        timeout=60  # 1 min max
    )
    if rc != 0:
        log(f"Error configurando Kafka: {err}")
        log("Pipeline detenido por error en Kafka setup")
        return 1

    # Iniciar consumer de pipeline_status en segundo plano
    consumer_process = start_status_consumer()
    if consumer_process is None:
        log("[WARN] No se pudo iniciar el status consumer, pero el pipeline continua...")

    all_success = True
    all_stats = {}

    # Step 1: Scraper
    log("\n")
    success, stats = step_scraper()
    all_stats.update(stats)
    if not success:
        log("Pipeline detenido por error en scraper")
        all_success = False

    # Steps 2-8 solo si todo va bien
    if all_success:
        log("\n")
        success, stats = step_extract_csv()
        all_stats.update(stats)
        if not success:
            all_success = False

    if all_success:
        log("\n")
        success, stats = step_mongodb_load()
        all_stats.update(stats)
        if not success:
            all_success = False

    # Step 4: Kafka Events (nuevo step entre MongoDB y Hadoop)
    if all_success:
        log("\n")
        success, stats = step_kafka_events()
        all_stats.update(stats)
        # Kafka events no es critico, continuar incluso si falla
        if not success:
            log("[WARN] Kafka events fallo pero el pipeline continua...")

    if all_success:
        log("\n")
        success, stats = step_hadoop_wordcount()
        all_stats.update(stats)

    if all_success:
        log("\n")
        success, stats = step_spark_streaming()
        all_stats.update(stats)
        # Streaming no es critico, continuar incluso si falla

    if all_success:
        log("\n")
        success, stats = step_spark_analysis()
        all_stats.update(stats)
        if not success:
            all_success = False

    if all_success:
        log("\n")
        success, stats = step_mongodb_results()
        all_stats.update(stats)

    # Finalizar
    if all_success:
        complete_pipeline()
        log("=" * 60)
        log("PIPELINE COMPLETADO EXITOSAMENTE")
        log("=" * 60)
    else:
        log("=" * 60)
        log("PIPELINE FINALIZADO CON ERRORES")
        log("=" * 60)

    # Generar reporte final
    status = __import__("status_manager", fromlist=["get_status"]).get_status()
    report_path = os.path.join(OUTPUT_DIR, "pipeline_report.json")
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(status, f, ensure_ascii=False, indent=2)
    log(f"Reporte final guardado en {report_path}")

    sys.exit(0 if all_success else 1)


if __name__ == "__main__":
    main()