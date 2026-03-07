# Guía de Setup — Airflow ETL (Laboratorio 05)

## Estructura de carpetas requerida

```
tu-proyecto/
├── docker-compose.yml          ← el que te entregamos
├── dags/
│   └── etl_sql_nosql.py        ← el DAG del ETL
├── logs/                       ← (se llena automático)
├── plugins/                    ← (vacía por ahora)
└── data/
    ├── fuente_sql.db           ← ⬅ COLOCA AQUÍ tu base SQLite fuente
    └── dw.db                   ← se crea automáticamente al correr el DAG
```

---

## Instalar Docker y correr docker-compose

```bash
docker-compose up -d
```

---

## Abrir Airflow

Abre el navegador en: **http://localhost:8080**
- Usuario: `airflow`
- Contraseña: `airflow`

---

## Activar el DAG

1. En la UI ve a **DAGs**
2. Busca `etl_sql_nosql`
3. Actívalo con el toggle (azul = activo)
4. El DAG correrá automáticamente cada 30 minutos
5. Para probarlo de inmediato: botón **▶ Trigger DAG**

---

## Verificar el resultado

- Ir a la carpeta de data y buscar dw.db

---

## Flujo del DAG (cómo se ve en Airflow)

```
t1_extraer_sql   ──┐
t2_extraer_costos──┼──▶  t4_integrar  ──▶  t5_cargar_dw
t3_extraer_bigmac──┘
```

| Task | Qué hace |
|------|----------|
| `t1_extraer_sql` | Lee `envejecimiento` de SQLite |
| `t2_extraer_costos` | Lee 4 colecciones de MongoDB Atlas |
| `t3_extraer_bigmac` | Lee colección `big_mac` de MongoDB |
| `t4_integrar` | Merge en memoria con pandas |
| `t5_cargar_dw` | Escribe `datos_integrados` en `dw.db` |

---