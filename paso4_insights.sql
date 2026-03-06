-- ================================================================
-- PASO 4 - Queries de Insights sobre el Data Warehouse
-- Ejercicio 3 del Laboratorio 05 - CC3089 Base de Datos 2
-- ================================================================
-- Base de datos: dw.db (SQLite)
-- Tabla: datos_integrados
-- ================================================================


-- ================================================================
-- INSIGHT 1: Relación entre poder adquisitivo (Big Mac) y costo
--            promedio de hospedaje
-- ================================================================
-- ¿Los países donde el Big Mac es más caro también tienen
-- hospedaje más caro? Esto indica el costo de vida general.
-- Recomendación: un viajero con presupuesto limitado debería
-- evitar países con Big Mac caro, ya que el costo de hospedaje
-- también será elevado.
-- ================================================================

SELECT
    pais,
    continente,
    ROUND(precio_big_mac_usd, 2)          AS precio_big_mac_usd,
    ROUND(costo_promedio_hospedaje, 2)    AS costo_promedio_hospedaje_usd,
    ROUND(costo_promedio_comida, 2)       AS costo_promedio_comida_usd,
    ROUND(
        costo_promedio_hospedaje + costo_promedio_comida
        + costo_promedio_transporte + costo_promedio_entretenimiento
    , 2)                                  AS costo_diario_promedio_usd
FROM datos_integrados
WHERE precio_big_mac_usd IS NOT NULL
  AND costo_promedio_hospedaje IS NOT NULL
ORDER BY precio_big_mac_usd DESC
LIMIT 15;


-- ================================================================
-- INSIGHT 2: Países más envejecidos y su costo de entretenimiento
-- ================================================================
-- Analiza si las naciones con mayor porcentaje de población
-- adulta mayor tienen una oferta de entretenimiento más cara
-- (museos, ópera, turismo cultural) vs destinos más jóvenes
-- con entretenimiento de menor costo.
-- Recomendación: turoperadoras podrían diseñar paquetes
-- específicos para adultos mayores en países con alta tasa de
-- envejecimiento y servicios enfocados en ese segmento.
-- ================================================================

SELECT
    pais,
    continente,
    ROUND(tasa_de_envejecimiento, 2)              AS tasa_envejecimiento_pct,
    ROUND(costo_promedio_entretenimiento, 2)      AS costo_prom_entretenimiento_usd,
    ROUND(costo_bajo_entretenimiento, 2)          AS costo_bajo_entretenimiento_usd,
    ROUND(poblacion / 1000000.0, 2)               AS poblacion_millones
FROM datos_integrados
WHERE tasa_de_envejecimiento IS NOT NULL
ORDER BY tasa_de_envejecimiento DESC
LIMIT 15;


-- ================================================================
-- INSIGHT 3: Destinos turísticos más económicos
--            (costo diario bajo total) vs tasa de envejecimiento
-- ================================================================
-- Identifica los países más baratos para viajar según el costo
-- diario estimado en su nivel más bajo. Cruza con la tasa de
-- envejecimiento para saber si son destinos "jóvenes" o
-- "envejecidos". Países jóvenes con costos bajos pueden ser
-- interesantes para el turismo de aventura y mochilero.
-- Recomendación: agencias de viajes podrían posicionar estos
-- destinos como opciones económicas y emergentes, con potencial
-- de crecimiento turístico.
-- ================================================================

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
    ROUND(tasa_de_envejecimiento, 2)              AS tasa_envejecimiento_pct,
    ROUND(precio_big_mac_usd, 2)                  AS precio_big_mac_usd
FROM datos_integrados
WHERE costo_bajo_hospedaje IS NOT NULL
  AND costo_bajo_comida IS NOT NULL
  AND costo_bajo_transporte IS NOT NULL
  AND costo_bajo_entretenimiento IS NOT NULL
ORDER BY costo_diario_minimo_usd ASC
LIMIT 15;


-- ================================================================
-- BONUS: Resumen por continente
-- ================================================================
-- Vista agregada por continente: promedio de costos y
-- tasa de envejecimiento. Útil para comparaciones regionales.
-- ================================================================

SELECT
    continente,
    COUNT(*)                                          AS num_paises,
    ROUND(AVG(tasa_de_envejecimiento), 2)             AS tasa_envej_promedio,
    ROUND(AVG(precio_big_mac_usd), 2)                 AS big_mac_promedio_usd,
    ROUND(AVG(
        costo_promedio_hospedaje + costo_promedio_comida
        + costo_promedio_transporte + costo_promedio_entretenimiento
    ), 2)                                             AS costo_diario_prom_usd,
    ROUND(AVG(
        costo_bajo_hospedaje + costo_bajo_comida
        + costo_bajo_transporte + costo_bajo_entretenimiento
    ), 2)                                             AS costo_diario_min_prom_usd
FROM datos_integrados
WHERE continente IS NOT NULL
GROUP BY continente
ORDER BY costo_diario_prom_usd DESC;
