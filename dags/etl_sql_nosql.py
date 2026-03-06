"""
DAG: etl_sql_nosql
==================
Laboratorio 05 — CC3089 Base de Datos 2
Ejercicio 1: Integración SQL + NoSQL con Apache Airflow

Schedule : cada 30 minutos  →  */30 * * * *
Fuente SQL  : SQLite (fuente_sql.db) montada en /opt/airflow/data/
Fuente NoSQL: MongoDB Atlas — leído desde variable de entorno CONNECTION_STRING (definida en .env)
Destino DW  : SQLite (dw.db) en /opt/airflow/data/

Tareas:
  t1_extraer_sql      → Lee tabla 'envejecimiento' de SQLite
  t2_extraer_costos   → Lee colecciones europa/africa/america/asia de MongoDB
  t3_extraer_bigmac   → Lee colección big_mac de MongoDB
  t4_integrar         → Merge en memoria con pandas
  t5_cargar_dw        → Escribe en datos_integrados en dw.db

Flujo:
  t1 ──┐
  t2 ──┼──▶  t4_integrar  ──▶  t5_cargar_dw
  t3 ──┘
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
from pymongo import MongoClient

from airflow.decorators import dag, task

# ─────────────────────────────────────────────────────────────────
# CONFIGURACIÓN — se leen desde el .env (cargado vía env_file en docker-compose)
# ─────────────────────────────────────────────────────────────────

# Tu .env define:  CONNECTION_STRING = "mongodb+srv://..."
MONGO_URI     = os.environ.get("CONNECTION_STRING", "").strip().strip('"').strip("'")
MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "turismo")
SQLITE_FUENTE = os.environ.get("SQLITE_FUENTE", "/opt/airflow/data/fuente_sql.db")
SQLITE_DW     = os.environ.get("SQLITE_DW",     "/opt/airflow/data/dw.db")

COLECCIONES_COSTOS = ["europa", "africa", "america", "asia"]

log = logging.getLogger("airflow.etl_sql_nosql")

# ─────────────────────────────────────────────────────────────────
# DAG — API de decoradores (Airflow 3.x)
# ─────────────────────────────────────────────────────────────────

@dag(
    dag_id="etl_sql_nosql",
    description="ETL: SQLite + MongoDB Atlas → dw.db cada 30 minutos",
    start_date=datetime(2026, 3, 1),
    schedule="*/30 * * * *",        # Airflow 3.x usa 'schedule', no 'schedule_interval'
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["lab05", "etl", "sql", "nosql"],
)
def etl_sql_nosql():

    # ──────────────────────────────────────────────────────────────
    # TASK 1 — Extraer desde SQLite
    # ──────────────────────────────────────────────────────────────
    @task()
    def t1_extraer_sql() -> str:
        log.info("T1 → Conectando a SQLite: %s", SQLITE_FUENTE)

        if not os.path.exists(SQLITE_FUENTE):
            raise FileNotFoundError(
                f"No se encontró fuente_sql.db en {SQLITE_FUENTE}.\n"
                "Asegúrate de colocar el archivo en ./data/ y de "
                "haber agregado el volumen en docker-compose.yaml"
            )

        conn = sqlite3.connect(SQLITE_FUENTE)
        df = pd.read_sql_query(
            """
            SELECT
                nombre_pais,
                continente,
                region,
                capital,
                poblacion,
                tasa_de_envejecimiento
            FROM envejecimiento
            WHERE nombre_pais IS NOT NULL
            """,
            conn,
        )
        conn.close()

        df["nombre_pais"] = df["nombre_pais"].str.strip()
        log.info("T1 → %d filas extraídas de SQLite", len(df))
        return df.to_json(orient="records")

    # ──────────────────────────────────────────────────────────────
    # TASK 2 — Extraer costos turísticos desde MongoDB
    # ──────────────────────────────────────────────────────────────
    @task()
    def t2_extraer_costos() -> str:
        if not MONGO_URI:
            raise ValueError(
                "CONNECTION_STRING no está definida en .env. "
                "Agrega tu URI de MongoDB Atlas."
            )

        log.info("T2 → Conectando a MongoDB Atlas...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client[MONGO_DB_NAME]

        registros = []
        for coleccion in COLECCIONES_COSTOS:
            for doc in db[coleccion].find({}, {"_id": 0}):
                try:
                    costos = doc.get("costos_diarios_estimados_en_dólares", {})
                    registros.append({
                        "pais":                           doc.get("país", doc.get("pais", "")).strip(),
                        "continente_mongo":               doc.get("continente", ""),
                        "region":                         doc.get("región", doc.get("region", "")),
                        "capital":                        doc.get("capital", ""),
                        "poblacion":                      doc.get("población", doc.get("poblacion")),
                        "costo_bajo_hospedaje":           costos.get("hospedaje", {}).get("precio_bajo_usd"),
                        "costo_promedio_hospedaje":       costos.get("hospedaje", {}).get("precio_promedio_usd"),
                        "costo_alto_hospedaje":           costos.get("hospedaje", {}).get("precio_alto_usd"),
                        "costo_bajo_comida":              costos.get("comida", {}).get("precio_bajo_usd"),
                        "costo_promedio_comida":          costos.get("comida", {}).get("precio_promedio_usd"),
                        "costo_alto_comida":              costos.get("comida", {}).get("precio_alto_usd"),
                        "costo_bajo_transporte":          costos.get("transporte", {}).get("precio_bajo_usd"),
                        "costo_promedio_transporte":      costos.get("transporte", {}).get("precio_promedio_usd"),
                        "costo_alto_transporte":          costos.get("transporte", {}).get("precio_alto_usd"),
                        "costo_bajo_entretenimiento":     costos.get("entretenimiento", {}).get("precio_bajo_usd"),
                        "costo_promedio_entretenimiento": costos.get("entretenimiento", {}).get("precio_promedio_usd"),
                        "costo_alto_entretenimiento":     costos.get("entretenimiento", {}).get("precio_alto_usd"),
                    })
                except Exception as ex:
                    log.warning("Error en doc de '%s': %s", coleccion, ex)

        client.close()
        log.info("T2 → %d documentos de costos extraídos", len(registros))
        return json.dumps(registros)

    # ──────────────────────────────────────────────────────────────
    # TASK 3 — Extraer Big Mac desde MongoDB
    # ──────────────────────────────────────────────────────────────
    @task()
    def t3_extraer_bigmac() -> str:
        log.info("T3 → Extrayendo colección big_mac de MongoDB...")
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=15000)
        db = client[MONGO_DB_NAME]

        registros = []
        for doc in db["big_mac"].find({}, {"_id": 0}):
            pais   = doc.get("país", doc.get("pais", "")).strip()
            precio = doc.get("precio_big_mac_usd")
            if pais:
                registros.append({"pais": pais, "precio_big_mac_usd": precio})

        client.close()
        log.info("T3 → %d documentos big_mac extraídos", len(registros))
        return json.dumps(registros)

    # ──────────────────────────────────────────────────────────────
    # TASK 4 — Integrar en memoria con pandas
    # ──────────────────────────────────────────────────────────────
    @task()
    def t4_integrar(sql_json: str, costos_json: str, bigmac_json: str) -> str:
        df_sql    = pd.read_json(sql_json,    orient="records")
        df_costos = pd.DataFrame(json.loads(costos_json))
        df_bigmac = pd.DataFrame(json.loads(bigmac_json))

        log.info("T4 → SQL: %d | Costos: %d | BigMac: %d",
                 len(df_sql), len(df_costos), len(df_bigmac))

        # Merge 1: costos + big mac por nombre de país
        df_temp = pd.merge(df_costos, df_bigmac, on="pais", how="inner")
        log.info("T4 → Merge costos+bigmac: %d filas", len(df_temp))

        # Merge 2: resultado + SQL
        df_final = pd.merge(
            df_temp,
            df_sql,
            left_on="pais",
            right_on="nombre_pais",
            how="inner",
        )
        log.info("T4 → Merge con SQL: %d filas", len(df_final))

        # Resolver continente: SQL tiene prioridad sobre Mongo
        if "continente" in df_final.columns and "continente_mongo" in df_final.columns:
            df_final["continente"] = df_final["continente"].combine_first(
                df_final["continente_mongo"]
            )
        elif "continente_mongo" in df_final.columns:
            df_final.rename(columns={"continente_mongo": "continente"}, inplace=True)

        df_final.drop(columns=["nombre_pais", "continente_mongo"], errors="ignore", inplace=True)

        if df_final.empty:
            raise ValueError(
                "El DataFrame integrado está vacío. "
                "Verifica que los nombres de países coincidan entre fuentes."
            )

        log.info("T4 → %d países integrados ✅", len(df_final))
        return df_final.to_json(orient="records")

    # ──────────────────────────────────────────────────────────────
    # TASK 5 — Cargar al Data Warehouse
    # ──────────────────────────────────────────────────────────────
    @task()
    def t5_cargar_dw(integrado_json: str):
        df = pd.read_json(integrado_json, orient="records")
        log.info("T5 → Cargando %d registros en %s...", len(df), SQLITE_DW)

        conn = sqlite3.connect(SQLITE_DW)

        # DROP + CREATE para idempotencia (cada ejecución reemplaza los datos)
        conn.execute("DROP TABLE IF EXISTS datos_integrados")
        conn.execute("""
        CREATE TABLE datos_integrados (
            pais                            TEXT PRIMARY KEY,
            continente                      TEXT,
            region                          TEXT,
            capital                         TEXT,
            poblacion                       REAL,
            tasa_de_envejecimiento          REAL,
            costo_bajo_hospedaje            REAL,
            costo_promedio_hospedaje        REAL,
            costo_alto_hospedaje            REAL,
            costo_bajo_comida               REAL,
            costo_promedio_comida           REAL,
            costo_alto_comida               REAL,
            costo_bajo_transporte           REAL,
            costo_promedio_transporte       REAL,
            costo_alto_transporte           REAL,
            costo_bajo_entretenimiento      REAL,
            costo_promedio_entretenimiento  REAL,
            costo_alto_entretenimiento      REAL,
            precio_big_mac_usd              REAL,
            ultima_actualizacion            TEXT
        )
        """)
        conn.commit()

        df["ultima_actualizacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        df.to_sql("datos_integrados", conn, if_exists="append", index=False)

        count = conn.execute("SELECT COUNT(*) FROM datos_integrados").fetchone()[0]
        log.info("T5 → %d registros cargados en dw.db ✅", count)
        conn.close()

    # ──────────────────────────────────────────────────────────────
    # DEFINIR DEPENDENCIAS DEL FLUJO
    # ──────────────────────────────────────────────────────────────
    sql_data    = t1_extraer_sql()
    costos_data = t2_extraer_costos()
    bigmac_data = t3_extraer_bigmac()

    integrado   = t4_integrar(sql_data, costos_data, bigmac_data)
    t5_cargar_dw(integrado)


# Instanciar el DAG
etl_sql_nosql()
