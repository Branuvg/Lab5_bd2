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

## Paso 1 — Crear carpetas

```bash
mkdir -p dags logs plugins data
```

---

## Paso 2 — Copiar tu fuente_sql.db

```bash
# Copia fuente_sql.db a la carpeta data/
cp /ruta/de/tu/fuente_sql.db ./data/
```

---

## Paso 3 — Configurar tu URI de MongoDB Atlas

Abre `docker-compose.yml` y edita esta línea:

```yaml
MONGO_URI: "mongodb+srv://USUARIO:PASSWORD@cluster.mongodb.net/?retryWrites=true&w=majority"
```

Reemplaza `USUARIO`, `PASSWORD` y `cluster` con tus credenciales reales de Atlas.

> ⚠️ Asegúrate de que en MongoDB Atlas → Network Access tengas 0.0.0.0/0
> para permitir conexiones desde el contenedor Docker.

---

## Paso 4 — Inicializar Airflow (solo la primera vez)

```bash
# En Linux/Mac: fijar el UID de tu usuario
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Inicializar base de datos y crear usuario admin
docker-compose up airflow-init
```

Espera a que termine. Verás `exited with code 0` al finalizar.

---

## Paso 5 — Levantar Airflow

```bash
docker-compose up -d
```

Abre el navegador en: **http://localhost:8080**
- Usuario: `admin`
- Contraseña: `admin`

---

## Paso 6 — Activar el DAG

1. En la UI ve a **DAGs**
2. Busca `etl_sql_nosql`
3. Actívalo con el toggle (azul = activo)
4. El DAG correrá automáticamente cada 30 minutos
5. Para probarlo de inmediato: botón **▶ Trigger DAG**

---

## Paso 7 — Verificar el resultado

```bash
# Ver los datos cargados en dw.db desde tu máquina local
sqlite3 ./data/dw.db "SELECT pais, continente, tasa_de_envejecimiento, precio_big_mac_usd FROM datos_integrados LIMIT 10;"
```

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

## Solución de problemas comunes

**El DAG no aparece:**
```bash
docker-compose logs airflow-scheduler | tail -30
```

**Error de conexión a MongoDB:**
- Verifica la URI en docker-compose.yml
- Verifica Network Access en Atlas (debe permitir 0.0.0.0/0)

**Error: fuente_sql.db no encontrada:**
- Confirma que el archivo está en `./data/fuente_sql.db`

**Reiniciar todo desde cero:**
```bash
docker-compose down -v
docker-compose up airflow-init
docker-compose up -d
```
