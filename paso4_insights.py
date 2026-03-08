"""
PASO 4 - Ejercicio 3: Insights sobre el Data Warehouse
=======================================================
Laboratorio 05 — CC3089 Base de Datos 2

Corre las 3 queries de insights sobre dw.db y muestra los resultados
formateados listos para captura de pantalla en el informe.

USO:
    python paso4_insights.py

REQUISITOS:
    pip install pandas tabulate
"""

import sqlite3
import pandas as pd

# ─────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────
DB_DW = "dw.db"   # <- ajusta la ruta si es necesario

# ─────────────────────────────────────────
# CONEXIÓN
# ─────────────────────────────────────────
conn = sqlite3.connect(DB_DW)

def mostrar(titulo, descripcion, query, recomendacion):
    
    print(f"\n{titulo}")

    print(f"{descripcion}")
    print()
    df = pd.read_sql_query(query, conn)
    # Formatear decimales
    for col in df.select_dtypes(include="float").columns:
        df[col] = df[col].round(2)
    print(df.to_string(index=False))
    print(f"\nRecomendación: {recomendacion}")

# ═══════════════════════════════════════════════════════════════════
# INSIGHT 1
# Relación entre precio del Big Mac y costo promedio de hospedaje
# ═══════════════════════════════════════════════════════════════════
mostrar(
    titulo="INSIGHT 1 — Poder adquisitivo vs Costo de hospedaje",
    descripcion=(
        "¿Los países con Big Mac más caro también tienen hospedaje más caro?\n"
        "  El precio del Big Mac es un proxy del costo de vida general."
    ),
    query="""
        SELECT
            pais,
            continente,
            ROUND(precio_big_mac_usd, 2)            AS big_mac_usd,
            ROUND(costo_promedio_hospedaje, 2)       AS hospedaje_prom_usd,
            ROUND(costo_promedio_comida, 2)          AS comida_prom_usd,
            ROUND(
                costo_promedio_hospedaje
                + costo_promedio_comida
                + costo_promedio_transporte
                + costo_promedio_entretenimiento
            , 2)                                     AS costo_diario_total_usd
        FROM datos_integrados
        WHERE precio_big_mac_usd IS NOT NULL
          AND costo_promedio_hospedaje IS NOT NULL
        ORDER BY precio_big_mac_usd DESC
        LIMIT 12
    """,
    recomendacion=(
        "Los viajeros con presupuesto ajustado deberían evitar destinos con Big Mac "
        "caro, ya que el costo de hospedaje y comida tiende a ser proporcionalmente alto. "
        "Las agencias pueden usar el índice Big Mac como filtro rápido para segmentar "
        "paquetes turísticos por nivel de gasto."
    )
)

# ═══════════════════════════════════════════════════════════════════
# INSIGHT 2
# Países más envejecidos y su costo de entretenimiento
# ═══════════════════════════════════════════════════════════════════
mostrar(
    titulo="INSIGHT 2 — Envejecimiento poblacional vs Costo de entretenimiento",
    descripcion=(
        "¿Las naciones con mayor porcentaje de adultos mayores\n"
        "  ofrecen entretenimiento más caro (museos, ópera, turismo cultural)?"
    ),
    query="""
        SELECT
            pais,
            continente,
            region,
            ROUND(tasa_de_envejecimiento, 2)              AS tasa_envej_pct,
            ROUND(costo_bajo_entretenimiento, 2)          AS entret_bajo_usd,
            ROUND(costo_promedio_entretenimiento, 2)      AS entret_prom_usd,
            ROUND(costo_alto_entretenimiento, 2)          AS entret_alto_usd
        FROM datos_integrados
        WHERE tasa_de_envejecimiento IS NOT NULL
        ORDER BY tasa_de_envejecimiento DESC
        LIMIT 12
    """,
    recomendacion=(
        "Los países con alta tasa de envejecimiento (Europa principalmente) tienen "
        "oferta cultural rica pero con costos elevados. Las operadoras turísticas pueden "
        "diseñar paquetes culturales especializados para el segmento adulto mayor, "
        "aprovechando la demanda existente en esos mercados."
    )
)

# ═══════════════════════════════════════════════════════════════════
# INSIGHT 3
# Destinos más económicos vs tasa de envejecimiento
# ═══════════════════════════════════════════════════════════════════
mostrar(
    titulo="INSIGHT 3 — Destinos más económicos vs Tasa de envejecimiento",
    descripcion=(
        "¿Los países más baratos para viajar son jóvenes o envejecidos?\n"
        "  Costo diario mínimo = hospedaje_bajo + comida_baja + transporte_bajo + entret_bajo"
    ),
    query="""
        SELECT
            pais,
            continente,
            region,
            ROUND(
                costo_bajo_hospedaje + costo_bajo_comida
                + costo_bajo_transporte + costo_bajo_entretenimiento
            , 2)                                          AS costo_diario_minimo_usd,
            ROUND(
                costo_promedio_hospedaje + costo_promedio_comida
                + costo_promedio_transporte + costo_promedio_entretenimiento
            , 2)                                          AS costo_diario_promedio_usd,
            ROUND(tasa_de_envejecimiento, 2)              AS tasa_envej_pct,
            ROUND(precio_big_mac_usd, 2)                  AS big_mac_usd
        FROM datos_integrados
        WHERE costo_bajo_hospedaje IS NOT NULL
          AND tasa_de_envejecimiento IS NOT NULL
        ORDER BY costo_diario_minimo_usd ASC
        LIMIT 12
    """,
    recomendacion=(
        "Los destinos más económicos son principalmente países jóvenes de Asia, "
        "África y América Latina. Esto representa una oportunidad para posicionarlos "
        "como destinos emergentes de turismo mochilero y de aventura, donde el bajo "
        "costo de vida permite estadías más largas con menor presupuesto."
    )
)

# ═══════════════════════════════════════════════════════════════════
# BONUS — Resumen por continente
# ═══════════════════════════════════════════════════════════════════

print("  BONUS — Resumen agregado por continente")
df_bonus = pd.read_sql_query("""
    SELECT
        continente,
        COUNT(*)                                              AS paises,
        ROUND(AVG(tasa_de_envejecimiento), 2)                AS envej_prom_pct,
        ROUND(AVG(precio_big_mac_usd), 2)                    AS big_mac_prom_usd,
        ROUND(AVG(
            costo_promedio_hospedaje + costo_promedio_comida
            + costo_promedio_transporte + costo_promedio_entretenimiento
        ), 2)                                                 AS costo_diario_prom_usd,
        ROUND(AVG(
            costo_bajo_hospedaje + costo_bajo_comida
            + costo_bajo_transporte + costo_bajo_entretenimiento
        ), 2)                                                 AS costo_diario_min_usd
    FROM datos_integrados
    WHERE continente IS NOT NULL
    GROUP BY continente
    ORDER BY costo_diario_prom_usd DESC
""", conn)
for col in df_bonus.select_dtypes(include="float").columns:
    df_bonus[col] = df_bonus[col].round(2)
print(df_bonus.to_string(index=False))

conn.close()
print("Análisis completado")