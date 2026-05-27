# 🚗 Dashboard de Accidentes Viales en México (1997–2024)

Plataforma completa de análisis de accidentes de tránsito en México. Incluye un pipeline ETL distribuido con **Ray**, una **API REST con FastAPI**, y un **dashboard web interactivo** con mapas, gráficas y filtros por año.

---

## 🏗️ Arquitectura General

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATOS DE ENTRADA                            │
│          data/parquet_fixed/atus_anual_YYYY.parquet             │
│                  (28 archivos, ~2.5 GB)                         │
└───────────────────────────┬─────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────────┐
│                       etl_ray.py                                │
│               Pipeline ETL distribuido con Ray                  │
│   Divide los 28 años entre 3 workers · Corre en paralelo        │
└────────┬───────────────────┬────────────────────────┬───────────┘
         │                   │                         │
    ┌────▼────┐         ┌────▼────┐              ┌────▼────┐
    │Worker 1 │         │Worker 2 │              │Worker 3 │
    │ ~9 años │         │ ~9 años │              │ ~9 años │
    └────┬────┘         └────┬────┘              └────┬────┘
         └──────────────────┬┘──────────────────────┘
                            │ limpieza.py (por cada año)
                            │ · Estandariza columnas
                            │ · Calcula TOTAL_MUERTOS / HERIDOS
                            │ · Elimina nulos críticos
                            ▼
              data/preprocesado/dashboard_data.json
                            │
         ┌──────────────────┼──────────────────┐
         │                  │                  │
    ┌────▼─────┐     ┌──────▼──────┐    ┌──────▼──────┐
    │  api.py  │     │  index.html │    │  Ray Dash.  │
    │ FastAPI  │────▶│  Dashboard  │    │ :8265       │
    │  :8000   │     │  (Leaflet + │    └─────────────┘
    └──────────┘     │   Charts)   │
                     └─────────────┘
```

---

## 📁 Estructura del Proyecto

```
proyecto_final3/
├── etl_ray.py          # Pipeline ETL distribuido (punto de entrada principal)
├── limpieza.py         # Limpieza y estandarización de datos por año
├── api.py              # API REST con FastAPI
├── index.html          # Dashboard web (Leaflet + Chart.js)
├── fix_parquet.py      # Utilidad para reparar archivos parquet corruptos
├── Dockerfile          # Imagen Docker con Python 3.11, Ray, FastAPI
├── docker-compose.yml  # Cluster: ray-head + 3 workers + api-server
├── requirements.txt    # Dependencias pinadas
└── old/                # Versiones anteriores (referencia)
    ├── main.py
    ├── analytics.py
    ├── workers.py
    └── distribucion.py
```

---

## 🚀 Inicio Rápido (Docker — Recomendado)

### 1. Pre-requisito: colocar los datos

```
data/
└── parquet_fixed/
    ├── atus_anual_1997.parquet
    ├── atus_anual_1998.parquet
    └── ... (hasta 2024)
```

> Si tus archivos originales están en `data/parquet/`, primero ejecútalos por `fix_parquet.py` para repararlos.

### 2. Levantar el cluster

```bash
docker-compose up --build
```

Esto levanta automáticamente:

| Contenedor      | Rol                                           | Puerto |
|-----------------|-----------------------------------------------|--------|
| `ray-head`      | Nodo head de Ray + ejecuta `etl_ray.py`       | 6379   |
| `ray-worker-1`  | Worker de procesamiento                       | —      |
| `ray-worker-2`  | Worker de procesamiento                       | —      |
| `ray-worker-3`  | Worker de procesamiento                       | —      |
| `api-fastapi`   | Servidor FastAPI (sirve datos al dashboard)   | 8000   |

### 3. Ver el dashboard

Una vez que el ETL termine (revisa los logs), abre en tu navegador:

```
index.html   →  Abrelo directamente en el navegador (doble clic)
```

El HTML consume la API en `http://localhost:8000`.

### 4. Monitor de Ray (opcional)

```
http://localhost:8265
```

### 5. Apagar

```bash
docker-compose down          # Detener contenedores
docker-compose down -v       # + limpiar volúmenes
```

---

## ⚙️ Uso Local (sin Docker)

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar el ETL (requiere Ray corriendo localmente)
python etl_ray.py

# Levantar la API
uvicorn api:app --host 0.0.0.0 --port 8000

# Abrir index.html en el navegador
```

---

## 🔄 Pipeline ETL (`etl_ray.py`)

### Cómo funciona el procesamiento distribuido

1. Se detectan todos los archivos `atus_anual_*.parquet` en `data/parquet_fixed/`
2. Los 28 archivos (uno por año) se dividen en 3 lotes
3. Cada lote se envía a un worker `@ray.remote` que corre en paralelo
4. Cada worker llama a `limpieza.py` por cada año de su lote y produce:
   - Accidentes, muertos, heridos y causa principal **por estado**
   - Distribución de accidentes **por hora**
   - Top 10 **causas globales**
5. Los resultados se consolidan y se guardan en `data/preprocesado/dashboard_data.json`

### División de lotes (round-robin)

```
Worker 1: años [1997, 2000, 2003, 2006, 2009, 2012, 2015, 2018, 2021, 2024]
Worker 2: años [1998, 2001, 2004, 2007, 2010, 2013, 2016, 2019, 2022]
Worker 3: años [1999, 2002, 2005, 2008, 2011, 2014, 2017, 2020, 2023]
```

---

## 🌐 API REST (`api.py`)

Base URL: `http://localhost:8000`

| Endpoint                   | Descripción                                    |
|----------------------------|------------------------------------------------|
| `GET /api/anios`           | Lista de años disponibles en el dataset        |
| `GET /api/dashboard/{anio}`| Datos completos de un año: estados, horas, causas |

### Ejemplo de respuesta — `/api/dashboard/2023`

```json
{
  "anio": 2023,
  "estados": [
    {
      "ID_ENTIDAD": 15,
      "nombre_entidad": "MEXICO",
      "total_accidentes": 45200,
      "total_muertos": 1230,
      "total_heridos": 38000,
      "tasa_mortalidad": 2.72,
      "causa_principal": "CONDUCTOR"
    }
  ],
  "horas": [
    { "ID_HORA": 8, "total_accidentes": 22000 }
  ],
  "causas": [
    { "CAUSAACCI": "CONDUCTOR", "total_accidentes": 180000 }
  ]
}
```

---

## 🧹 Limpieza de Datos (`limpieza.py`)

Aplicada por cada worker a cada archivo parquet:

1. Estandariza nombres de columnas a **mayúsculas**
2. Convierte 12 columnas de lesiones a tipo numérico
3. Calcula `TOTAL_MUERTOS` y `TOTAL_HERIDOS` sumando las columnas individuales
4. Elimina filas con nulos **solo en columnas críticas** (`ID_ENTIDAD`, `ID_HORA`, `CAUSAACCI`)

### Columnas principales del dataset

| Columna         | Descripción                       |
|-----------------|-----------------------------------|
| `ID_ENTIDAD`    | Clave del estado (1–32)           |
| `ID_MUNICIPIO`  | Clave del municipio               |
| `ID_HORA`       | Hora del accidente (0–23)         |
| `ANIO`          | Año                               |
| `MES`           | Mes (1–12)                        |
| `CAUSAACCI`     | Causa del accidente               |
| `TOTAL_MUERTOS` | Calculado en limpieza.py          |
| `TOTAL_HERIDOS` | Calculado en limpieza.py          |

---

## 📊 Dashboard Web (`index.html`)

Visualizaciones disponibles (filtradas por año con slider):

- **Mapa coroplético** de México con accidentes por estado (Leaflet.js)
- **Gráfica de líneas** — distribución de accidentes por hora del día
- **Gráfica de barras** — top 10 causas de accidentes
- **Tabla de estados** — accidentes, víctimas, tasa de mortalidad y causa principal

El HTML es un archivo estático que consume la API local. No requiere build ni bundler.

---

## 📦 Requisitos

| Método  | Requisitos                                   |
|---------|----------------------------------------------|
| Docker  | Docker Engine 20+ y Docker Compose v2        |
| Local   | Python 3.11+, ver `requirements.txt`         |

Dependencias principales: `ray==2.55.1`, `pandas==3.0.3`, `pyarrow==24.0.0`, `fastapi==0.136.3`, `uvicorn==0.48.0`

---

## 🛠️ Solución de Problemas

**El ETL no encuentra archivos parquet**
```
❌ No se encontraron archivos parquet.
```
Verifica que los archivos estén en `data/parquet_fixed/` con el nombre `atus_anual_YYYY.parquet`. Si están en `data/parquet/`, ejecuta primero `fix_parquet.py`.

**El dashboard no carga datos**
Asegúrate de que el contenedor `api-fastapi` esté corriendo y que el ETL haya finalizado (debe existir `data/preprocesado/dashboard_data.json`).

**Ver logs del ETL en tiempo real**
```bash
docker-compose logs -f ray-head
```