"""
PASO 1 - Setup de Base de Datos SQLite (fuente_sql.db)
=====================================================
Este script crea la base de datos relacional fuente con las tablas:
  - envejecimiento   (desde pais_envejecimiento.csv)
  - poblacion_costos (desde pais_poblacion.csv)

USO:
    python paso1_setup_sqlite.py

REQUISITOS:
    - pip install pandas
    - Tener los archivos CSV en la misma carpeta (o ajustar las rutas abajo)
"""

import sqlite3
import pandas as pd
import os

# ─────────────────────────────────────────
# CONFIGURACIÓN - ajusta las rutas si es necesario
# ─────────────────────────────────────────
CSV_ENVEJECIMIENTO  = "Datos_para_SQL\pais_envejecimiento.csv"   # <- ruta al CSV
CSV_POBLACION       = "Datos_para_SQL\pais_poblacion.csv"         # <- ruta al CSV
DB_FUENTE           = "fuente_sql.db"              # <- nombre del archivo de salida

# ─────────────────────────────────────────
# 1. LEER Y LIMPIAR LOS CSV
# ─────────────────────────────────────────

print("=" * 55)
print("  PASO 1 — Creando fuente_sql.db")
print("=" * 55)

# --- Envejecimiento ---
print("\n[1/4] Leyendo pais_envejecimiento.csv ...")
df_env = pd.read_csv(CSV_ENVEJECIMIENTO, encoding="utf-8")
print(f"      Filas cargadas : {len(df_env)}")
print(f"      Columnas       : {list(df_env.columns)}")

# Limpieza básica
df_env.columns = [c.strip().lower().replace(" ", "_") for c in df_env.columns]
# Convertir vacíos a NULL
df_env = df_env.where(pd.notnull(df_env), None)
print(f"      Nulos por columna:\n{df_env.isnull().sum().to_string()}")

# --- Población / Costos ---
print("\n[2/4] Leyendo pais_poblacion.csv ...")
df_pob = pd.read_csv(CSV_POBLACION, encoding="utf-8")
print(f"      Filas cargadas : {len(df_pob)}")
print(f"      Columnas       : {list(df_pob.columns)}")

df_pob.columns = [c.strip().lower().replace(" ", "_") for c in df_pob.columns]
df_pob = df_pob.where(pd.notnull(df_pob), None)
print(f"      Nulos por columna:\n{df_pob.isnull().sum().to_string()}")

# ─────────────────────────────────────────
# 2. CREAR BASE DE DATOS Y TABLAS
# ─────────────────────────────────────────

print(f"\n[3/4] Creando {DB_FUENTE} ...")

if os.path.exists(DB_FUENTE):
    os.remove(DB_FUENTE)
    print(f"      (archivo anterior eliminado)")

conn = sqlite3.connect(DB_FUENTE)
cursor = conn.cursor()

# --- Tabla envejecimiento ---
cursor.execute("DROP TABLE IF EXISTS envejecimiento")
cursor.execute("""
CREATE TABLE envejecimiento (
    id_pais               INTEGER PRIMARY KEY,
    nombre_pais           TEXT,
    capital               TEXT,
    continente            TEXT,
    region                TEXT,
    poblacion             REAL,
    tasa_de_envejecimiento REAL
)
""")

# --- Tabla poblacion_costos ---
cursor.execute("DROP TABLE IF EXISTS poblacion_costos")
cursor.execute("""
CREATE TABLE poblacion_costos (
    _id                          TEXT PRIMARY KEY,
    continente                   TEXT,
    pais                         TEXT,
    poblacion                    REAL,
    costo_bajo_hospedaje         INTEGER,
    costo_promedio_comida        INTEGER,
    costo_bajo_transporte        INTEGER,
    costo_promedio_entretenimiento INTEGER
)
""")

conn.commit()
print("      Tablas creadas correctamente.")

# ─────────────────────────────────────────
# 3. INSERTAR DATOS
# ─────────────────────────────────────────

print("\n[4/4] Insertando datos ...")

# Mapear columnas del CSV a la tabla (en caso de que los nombres difieran)
# Envejecimiento
col_env_map = {
    "id_pais": "id_pais",
    "nombre_pais": "nombre_pais",
    "capital": "capital",
    "continente": "continente",
    "region": "region",
    "poblacion": "poblacion",
    "tasa_de_envejecimiento": "tasa_de_envejecimiento",
}
df_env_db = df_env.rename(columns={v: k for k, v in col_env_map.items()})
df_env_db = df_env_db[list(col_env_map.keys())]
df_env_db.to_sql("envejecimiento", conn, if_exists="append", index=False)
print(f"      envejecimiento   → {len(df_env_db)} filas insertadas")

# Población / Costos
col_pob_map = {
    "_id": "_id",
    "continente": "continente",
    "pais": "pais",
    "poblacion": "poblacion",
    "costo_bajo_hospedaje": "costo_bajo_hospedaje",
    "costo_promedio_comida": "costo_promedio_comida",
    "costo_bajo_transporte": "costo_bajo_transporte",
    "costo_promedio_entretenimiento": "costo_promedio_entretenimiento",
}
df_pob_db = df_pob.rename(columns={v: k for k, v in col_pob_map.items()})
# Solo conservar columnas que existen
cols_disponibles = [c for c in col_pob_map.keys() if c in df_pob_db.columns]
df_pob_db = df_pob_db[cols_disponibles]
df_pob_db.to_sql("poblacion_costos", conn, if_exists="append", index=False)
print(f"      poblacion_costos → {len(df_pob_db)} filas insertadas")

conn.commit()
conn.close()

# ─────────────────────────────────────────
# VERIFICACIÓN FINAL
# ─────────────────────────────────────────
conn = sqlite3.connect(DB_FUENTE)
for tabla in ["envejecimiento", "poblacion_costos"]:
    count = conn.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
    muestra = pd.read_sql(f"SELECT * FROM {tabla} LIMIT 3", conn)
    print(f"\n  Tabla '{tabla}': {count} registros")
    print(muestra.to_string(index=False))
conn.close()

print("\n✅  fuente_sql.db creada exitosamente.")
print(f"    Ubicación: {os.path.abspath(DB_FUENTE)}")
