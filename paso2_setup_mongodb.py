"""
PASO 2 - Importar datos JSON a MongoDB (base de datos: turismo)
===============================================================
Este script carga los 5 archivos JSON en colecciones separadas dentro
de la base de datos 'turismo' en MongoDB.

Colecciones creadas:
  - europa    (costos_turisticos_europa.json)
  - africa    (costos_turisticos_africa.json)
  - america   (costos_turisticos_america.json)
  - asia      (costos_turisticos_asia.json)
  - big_mac   (paises_mundo_big_mac.json)

USO:
    python paso2_setup_mongodb.py

REQUISITOS:
    pip install pymongo
"""

import json
import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

from dotenv import load_dotenv, dotenv_values 

load_dotenv() 

# accessing and printing value
print(os.getenv("CONNECTION_STRING"))

# ─────────────────────────────────────────
# CONFIGURACIÓN — ajusta según tu instancia
# ─────────────────────────────────────────

# MongoDB Atlas:  "mongodb+srv://usuario:password@cluster.mongodb.net/"
# MongoDB local:  "mongodb://localhost:27017/"
MONGO_URI = os.getenv("CONNECTION_STRING")

DB_NAME = "turismo"

# Archivos JSON y la colección destino
ARCHIVOS = {
    "europa":  "Datos_para_MongoDB\costos_turisticos_europa.json",
    "africa":  "Datos_para_MongoDB\costos_turisticos_africa.json",
    "america": "Datos_para_MongoDB\costos_turisticos_america.json",
    "asia":    "Datos_para_MongoDB\costos_turisticos_asia.json",
    "big_mac": "Datos_para_MongoDB\paises_mundo_big_mac.json",
}

# ─────────────────────────────────────────
# FUNCIÓN AUXILIAR
# ─────────────────────────────────────────

def cargar_json(ruta: str):
    """Lee un archivo JSON; soporta array raíz o un solo objeto."""
    with open(ruta, "r", encoding="utf-8") as f:
        data = json.load(f)
    if isinstance(data, list):
        return data
    elif isinstance(data, dict):
        # Puede ser un objeto con una clave que contiene la lista
        for v in data.values():
            if isinstance(v, list):
                return v
        return [data]
    return []

# ─────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────

print("PASO 2 — Importando JSONs a MongoDB")
print(f"\nConectando a: {MONGO_URI}")

try:
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("Conexión exitosa a MongoDB")
except Exception as e:
    print(f"No se pudo conectar a MongoDB: {e}")
    print("    Verifica que la URI sea correcta y que el servidor esté activo.")
    exit(1)

db = client[DB_NAME]

# ─────────────────────────────────────────
# CARGA
# ─────────────────────────────────────────

for coleccion, archivo in ARCHIVOS.items():
    print(f"\n[→] Colección: '{coleccion}'  |  Archivo: '{archivo}'")

    if not os.path.exists(archivo):
        print(f"Archivo no encontrado: {archivo}  (se omite)")
        continue

    documentos = cargar_json(archivo)
    print(f"    Documentos leídos: {len(documentos)}")

    if not documentos:
        print("    Lista vacía, se omite.")
        continue

    # Limpiar colección si ya existe (para idempotencia)
    db[coleccion].drop()

    try:
        resultado = db[coleccion].insert_many(documentos, ordered=False)
        print(f"Insertados: {len(resultado.inserted_ids)}")
    except BulkWriteError as bwe:
        print(f"Insertados con errores parciales: {bwe.details}")

# ─────────────────────────────────────────
# VERIFICACIÓN
# ─────────────────────────────────────────


print("VERIFICACION")

for coleccion in ARCHIVOS.keys():
    count = db[coleccion].count_documents({})
    print(f"  {coleccion:<10} → {count} documentos")

    # Mostrar muestra de campos del primer documento
    doc = db[coleccion].find_one({}, {"_id": 0})
    if doc:
        campos = list(doc.keys())
        print(f"Campos: {campos}")

client.close()
print("\nImportación a MongoDB completada.")
