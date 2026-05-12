from flask import Flask, jsonify
from pymongo import MongoClient
from flask_cors import CORS
import os

app = Flask(__name__)
CORS(app)

MONGODB_URI = os.environ.get("MONGODB_URI", "mongodb://mongodb:27017/")
client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
db = client["inmuebles"]

ALLOWED_COLLECTIONS = {
    "propiedades",
    "resultados_analisis",
    "wordcount_results",
    "pipeline_summary",
    "eventos_streaming",
    "alertas_streaming",
    "resumen_eventos_streaming"
}


def collection_sort(collection):
    if collection == "pipeline_summary":
        return [("fecha", -1), ("fecha_ejecucion", -1), ("_id", -1)]
    return [("fecha", -1), ("_id", -1)]


def get_collection_safe(collection_name):
    if collection_name not in ALLOWED_COLLECTIONS:
        return None
    return db[collection_name]


@app.route("/api/<collection>")
def get_collection(collection):
    collection_obj = get_collection_safe(collection)
    if collection_obj is None:
        return jsonify({"error": "Colección no permitida"}), 404

    docs = list(collection_obj.find({}, {"_id": 0}).sort(collection_sort(collection)).limit(100))
    return jsonify(docs)


@app.route("/api/<collection>/latest")
def get_latest(collection):
    collection_obj = get_collection_safe(collection)
    if collection_obj is None:
        return jsonify({"error": "Colección no permitida"}), 404

    docs = list(collection_obj.find({}, {"_id": 0}).sort(collection_sort(collection)).limit(10))
    return jsonify(docs)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
