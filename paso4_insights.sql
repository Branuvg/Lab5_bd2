
-- INSIGHT 1: Relación entre poder adquisitivo (Big Mac) y costo promedio de hospedaje

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

-- INSIGHT 2: Países más envejecidos y su costo de entretenimiento

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


-- INSIGHT 3: Destinos turísticos más económicos (costo diario bajo total) vs tasa de envejecimiento

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

-- BONUS: Resumen por continente

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
