"""
PASO 3 - ETL Python: Integración SQL + MongoDB → Data Warehouse
===============================================================
Ejercicio 2 del Laboratorio 05 - CC3089 Base de Datos 2

Flujo:
  1. Extrae tasa_de_envejecimiento desde SQLite (fuente_sql.db)
  2. Extrae costos turísticos desde MongoDB (colecciones europa/africa/america/asia)
  3. Extrae precios Big Mac desde MongoDB (colección big_mac)
  4. Integra todo en memoria con pandas (merge por nombre de país)
  5. Carga los datos integrados en dw.db (data warehouse SQLite)
  6. Puede ejecutarse periódicamente usando 'schedule'

USO:
    # Ejecución única:
    python paso3_etl_python.py

    # Ejecución periódica (cada hora):
    python paso3_etl_python.py --schedule

REQUISITOS:
    pip install pandas pymongo schedule
"""

import sqlite3
import os
import sys
import logging
from datetime import datetime

import pandas as pd
from pymongo import MongoClient

from dotenv import load_dotenv, dotenv_values 
load_dotenv() 


# ─────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────

MONGO_URI = os.getenv("CONNECTION_STRING")
DB_MONGO = "turismo"
DB_FUENTE = "fuente_sql.db"
DB_DW = "dw.db"

COLECCIONES_COSTOS = ["europa", "africa", "america", "asia"]

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ETL")

# ─────────────────────────────────────────
# PASO 1 — EXTRACCIÓN DESDE SQLite
# ─────────────────────────────────────────

def extraer_sql() -> pd.DataFrame:
    log.info("Extrayendo datos de SQLite (%s)...", DB_FUENTE)
    conn = sqlite3.connect(DB_FUENTE)
    df = pd.read_sql_query(
        "SELECT nombre_pais, continente, region, capital, poblacion, tasa_de_envejecimiento FROM envejecimiento",
        conn
    )
    conn.close()
    # Limpieza
    df.columns = [c.strip() for c in df.columns]
    df["nombre_pais"] = df["nombre_pais"].str.strip()
    log.info("  SQL → %d filas extraídas", len(df))
    return df

# ─────────────────────────────────────────
# PASO 2 — EXTRACCIÓN DESDE MongoDB (costos)
# ─────────────────────────────────────────

def extraer_costos_mongo(db) -> pd.DataFrame:
    log.info("Extrayendo costos turísticos de MongoDB...")
    registros = []

    for coleccion in COLECCIONES_COSTOS:
        cursor = db[coleccion].find({}, {"_id": 0})
        for doc in cursor:
            try:
                costos = doc.get("costos_diarios_estimados_en_dólares", {})

                registro = {
                    "pais":                        doc.get("país", doc.get("pais", "")).strip(),
                    "continente_mongo":            doc.get("continente", ""),
                    "region":                      doc.get("región", doc.get("region", "")),
                    "capital":                     doc.get("capital", ""),
                    "poblacion":                   doc.get("población", doc.get("poblacion", None)),

                    # Hospedaje
                    "costo_bajo_hospedaje":        costos.get("hospedaje", {}).get("precio_bajo_usd"),
                    "costo_promedio_hospedaje":    costos.get("hospedaje", {}).get("precio_promedio_usd"),
                    "costo_alto_hospedaje":        costos.get("hospedaje", {}).get("precio_alto_usd"),

                    # Comida
                    "costo_bajo_comida":           costos.get("comida", {}).get("precio_bajo_usd"),
                    "costo_promedio_comida":       costos.get("comida", {}).get("precio_promedio_usd"),
                    "costo_alto_comida":           costos.get("comida", {}).get("precio_alto_usd"),

                    # Transporte
                    "costo_bajo_transporte":       costos.get("transporte", {}).get("precio_bajo_usd"),
                    "costo_promedio_transporte":   costos.get("transporte", {}).get("precio_promedio_usd"),
                    "costo_alto_transporte":       costos.get("transporte", {}).get("precio_alto_usd"),

                    # Entretenimiento
                    "costo_bajo_entretenimiento":       costos.get("entretenimiento", {}).get("precio_bajo_usd"),
                    "costo_promedio_entretenimiento":   costos.get("entretenimiento", {}).get("precio_promedio_usd"),
                    "costo_alto_entretenimiento":       costos.get("entretenimiento", {}).get("precio_alto_usd"),
                }
                registros.append(registro)
            except Exception as ex:
                log.warning("  Error procesando doc en '%s': %s", coleccion, ex)

    df = pd.DataFrame(registros)
    log.info("  MongoDB costos → %d documentos extraídos", len(df))
    return df

# ─────────────────────────────────────────
# PASO 3 — EXTRACCIÓN DESDE MongoDB (big mac)
# ─────────────────────────────────────────

def extraer_bigmac_mongo(db) -> pd.DataFrame:
    log.info("Extrayendo precios Big Mac de MongoDB...")
    docs = list(db["big_mac"].find({}, {"_id": 0}))
    df = pd.DataFrame(docs)

    # Normalizar nombre de columna 'país'
    if "país" in df.columns:
        df.rename(columns={"país": "pais"}, inplace=True)
    elif "pais" not in df.columns:
        log.error("  No se encontró columna 'país' o 'pais' en big_mac")
        return pd.DataFrame()

    df["pais"] = df["pais"].str.strip()
    log.info("  MongoDB big_mac → %d documentos extraídos", len(df))
    return df[["pais", "precio_big_mac_usd"]]

# ─────────────────────────────────────────
# PASO 4 — INTEGRACIÓN EN MEMORIA
# ─────────────────────────────────────────

def integrar(df_sql: pd.DataFrame, df_costos: pd.DataFrame, df_bigmac: pd.DataFrame) -> pd.DataFrame:
    log.info("Integrando datos en memoria...")

    # ESTRATEGIA:
    # - region, capital, poblacion, continente → MongoDB (están completos en los JSON)
    # - tasa_de_envejecimiento                 → CSV/SQLite (el CSV tiene 101/106 NULLs
    #                                            en los otros campos, solo este es útil)

    # Del SQL solo traemos nombre_pais + tasa_de_envejecimiento
    df_sql_slim = df_sql[["nombre_pais", "tasa_de_envejecimiento"]].copy()

    # Merge 1: costos + big mac (por país)
    df_temp = pd.merge(df_costos, df_bigmac, on="pais", how="inner")
    log.info("  Después de merge costos+bigmac: %d filas", len(df_temp))

    # Merge 2: resultado + solo tasa_de_envejecimiento del CSV
    df_final = pd.merge(
        df_temp,
        df_sql_slim,
        left_on="pais",
        right_on="nombre_pais",
        how="inner"
    )
    log.info("  Después de merge con SQL: %d filas", len(df_final))

    # Limpiar columna auxiliar del join
    df_final.drop(columns=["nombre_pais"], inplace=True, errors="ignore")

    # Renombrar continente_mongo → continente
    if "continente_mongo" in df_final.columns:
        df_final.rename(columns={"continente_mongo": "continente"}, inplace=True)

    # Eliminar cualquier _x / _y residual del merge
    cols_dup = [c for c in df_final.columns if c.endswith("_x") or c.endswith("_y")]
    if cols_dup:
        log.warning("  Columnas duplicadas eliminadas: %s", cols_dup)
        df_final.drop(columns=cols_dup, inplace=True)

    # Ordenar columnas finales
    columnas_orden = [
        "pais", "continente", "region", "capital", "poblacion",
        "tasa_de_envejecimiento",
        "costo_bajo_hospedaje", "costo_promedio_hospedaje", "costo_alto_hospedaje",
        "costo_bajo_comida", "costo_promedio_comida", "costo_alto_comida",
        "costo_bajo_transporte", "costo_promedio_transporte", "costo_alto_transporte",
        "costo_bajo_entretenimiento", "costo_promedio_entretenimiento", "costo_alto_entretenimiento",
        "precio_big_mac_usd",
    ]
    cols_presentes = [c for c in columnas_orden if c in df_final.columns]
    df_final = df_final[cols_presentes]

    return df_final

# ─────────────────────────────────────────
# PASO 5 — CARGA AL DATA WAREHOUSE
# ─────────────────────────────────────────

def crear_tabla_dw(conn: sqlite3.Connection):
    conn.execute("DROP TABLE IF EXISTS datos_integrados")
    conn.execute("""
    CREATE TABLE datos_integrados (
        pais                           TEXT PRIMARY KEY,
        continente                     TEXT,
        region                         TEXT,
        capital                        TEXT,
        poblacion                      REAL,
        tasa_de_envejecimiento         REAL,
        costo_bajo_hospedaje           REAL,
        costo_promedio_hospedaje       REAL,
        costo_alto_hospedaje           REAL,
        costo_bajo_comida              REAL,
        costo_promedio_comida          REAL,
        costo_alto_comida              REAL,
        costo_bajo_transporte          REAL,
        costo_promedio_transporte      REAL,
        costo_alto_transporte          REAL,
        costo_bajo_entretenimiento     REAL,
        costo_promedio_entretenimiento REAL,
        costo_alto_entretenimiento     REAL,
        precio_big_mac_usd             REAL,
        ultima_actualizacion           TEXT
    )
    """)
    conn.commit()

def cargar_dw(df: pd.DataFrame):
    log.info("Cargando %d registros en %s...", len(df), DB_DW)
    conn = sqlite3.connect(DB_DW)
    crear_tabla_dw(conn)

    df["ultima_actualizacion"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df.to_sql("datos_integrados", conn, if_exists="append", index=False)

    count = conn.execute("SELECT COUNT(*) FROM datos_integrados").fetchone()[0]
    log.info("  Data warehouse → %d registros totales", count)

    # Muestra rápida
    muestra = pd.read_sql("SELECT pais, continente, tasa_de_envejecimiento, precio_big_mac_usd FROM datos_integrados LIMIT 5", conn)
    print("\n  Muestra del data warehouse:")
    print(muestra.to_string(index=False))

    conn.close()

# ─────────────────────────────────────────
# FUNCIÓN PRINCIPAL ETL
# ─────────────────────────────────────────

def ejecutar_etl():
    print("\n" + "=" * 55)
    print(f"  ETL iniciado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 55)

    try:
        # Conexión MongoDB
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[DB_MONGO]

        # Extracción
        df_sql     = extraer_sql()
        df_costos  = extraer_costos_mongo(db)
        df_bigmac  = extraer_bigmac_mongo(db)

        # Integración
        df_final = integrar(df_sql, df_costos, df_bigmac)

        if df_final.empty:
            log.error("El DataFrame integrado está vacío. Revisa los nombres de los países.")
            return

        # Carga
        cargar_dw(df_final)

        client.close()
        log.info("ETL completado exitosamente.")

    except Exception as e:
        log.error("Error durante el ETL: %s", e, exc_info=True)

# ─────────────────────────────────────────
# EJECUCIÓN
# ─────────────────────────────────────────

if __name__ == "__main__":
    if "--schedule" in sys.argv:
        try:
            import schedule
            import time
            INTERVALO_HORAS = 1   # <- cambia el intervalo si lo deseas

            log.info("Modo programado: ETL cada %d hora(s)", INTERVALO_HORAS)
            ejecutar_etl()  # primera ejecución inmediata

            schedule.every(INTERVALO_HORAS).hours.do(ejecutar_etl)
            while True:
                schedule.run_pending()
                time.sleep(60)

        except ImportError:
            log.error("Instala 'schedule': pip install schedule")
    else:
        ejecutar_etl()
