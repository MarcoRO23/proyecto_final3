# 📊 Análisis de Accidentes Viales - Procesamiento Distribuido con Ray

Proyecto de análisis de accidentes viales de México (1997-2024) usando procesamiento distribuido con **Ray** en Docker.

## 🏗️ Arquitectura

```
┌─────────────────────────────────────────────────────────┐
│                    main.py                              │
│        (Ejecuta análisis para todos los años)           │
└──────────────────┬──────────────────────────────────────┘
                   │
┌──────────────────▼──────────────────────────────────────┐
│              analytics.py (Orquestador)                 │
│    Divide trabajo entre 3 workers de Ray                │
└──────────────────┬──────────────────────────────────────┘
                   │
    ┌──────────────┼──────────────┐
    │              │              │
┌───▼──┐      ┌───▼──┐      ┌───▼──┐
│Worker│      │Worker│      │Worker│
│  1   │      │  2   │      │  3   │
└───┬──┘      └───┬──┘      └───┬──┘
    │              │              │
    └──────────────┼──────────────┘
                   │
        ┌──────────▼──────────┐
        │   workers.py        │
        │ (@ray.remote)       │
        │ - Procesamiento     │
        │   distribuido       │
        └─────────────────────┘
                   │
        ┌──────────▼──────────┐
        │   limpieza.py       │
        │ - Carga parquets    │
        │ - Standardización   │
        │ - Agregaciones      │
        └─────────────────────┘
```

## 📁 Estructura del Proyecto

| Archivo | Función |
|---------|---------|
| **limpieza.py** | Carga parquets, estandariza columnas, calcula totales, elimina nulos críticos |
| **workers.py** | Funciones `@ray.remote` que se ejecutan distribuidas en 3 workers |
| **analytics.py** | Orquesta tareas: divide entidades, llama workers, combina resultados |
| **main.py** | Punto de entrada: carga Ray, procesa todos los años (1997-2024) |
| **Dockerfile** | Imagen Docker con Python 3.11, Ray, Pandas, PyArrow |
| **docker-compose.yml** | Cluster Ray: 1 head + 3 workers |

## 🚀 Instalación y Uso

### Opción 1: Docker (Recomendado - Procesamiento Distribuido)

```bash
# Levantar el cluster Ray y ejecutar análisis
docker-compose up

# En otra terminal, ver logs en vivo
docker-compose logs -f ray-head

# Detener
docker-compose down
```

**Qué pasa automáticamente:**
1. Ray head inicia en puerto 6379
2. 3 workers se conectan al head
3. `main.py` ejecuta análisis para años 1997-2024
4. 32 entidades se distribuyen entre 3 workers

**Acceder al dashboard de Ray:**
- http://localhost:8265

### Opción 2: Localmente (sin Docker)

```bash
# Instalar dependencias
pip install -r requirements.txt

# Usar funciones directamente
python -c "
from limpieza import limpiar_datos
import analytics

df = limpiar_datos('data/parquet/atus_anual_2023.parquet')
print('Top 5 estados:', analytics.estados_mas_accidentes(df)[:5])
print('Top 5 municipios:', analytics.municipios_siniestralidad(df)[:5])
"
```

## 📊 API de Funciones

```python
from limpieza import limpiar_datos
import analytics

# Cargar datos de un año
df = limpiar_datos('data/parquet/atus_anual_2023.parquet')

# ✅ DISTRIBUIDAS EN 3 WORKERS
estados = analytics.estados_mas_accidentes(df)           # Top entidades por accidentes
municipios = analytics.municipios_siniestralidad(df)     # Top municipios por entidad
victimas = analytics.victimas_por_entidad(df)            # Víctimas (muertos + heridos) por entidad

# ⚪ NO DISTRIBUIDAS (ejecutan en head)
horas = analytics.horarios_accidentes(df)                # Accidentes por hora (0-23)
causas = analytics.causas_frecuentes(df)                 # Top causas
meses = analytics.meses_incidencia(df)                   # Accidentes por año-mes
```

### Retorno de Datos

```python
# estados_mas_accidentes() / municipios_siniestralidad()
[(entidad_id, count), (entidad_id, count), ...]

# horarios_accidentes() / causas_frecuentes()
[(hora/causa, count), (hora/causa, count), ...]

# victimas_por_entidad() / meses_incidencia()
[
  {'entidad': 1, 'muertos': 100, 'heridos': 500, 'total': 600},
  {'entidad': 2, 'muertos': 80, 'heridos': 400, 'total': 480},
  ...
]
```

## 🔧 Procesamiento Distribuido

### Cómo funciona Ray

1. **32 entidades** se dividen en 3 grupos
2. Cada grupo se envía a un worker con `@ray.remote`
3. Workers procesan **en paralelo**
4. Resultados se combinan en el head

### División de Entidades

```
Group 1 (Worker 1): [1, 4, 7, 10, 13, 16, 19, 22, 25, 28, 31]
Group 2 (Worker 2): [2, 5, 8, 11, 14, 17, 20, 23, 26, 29, 32]
Group 3 (Worker 3): [3, 6, 9, 12, 15, 18, 21, 24, 27, 30]
```

## 📈 Datos

- **Ubicación:** `data/parquet/atus_anual_*.parquet`
- **Años:** 1997 - 2024 (28 archivos)
- **Formato:** Parquet (PyArrow)
- **Tamaño:** ~2.5GB total

### Columnas principales

```
ID_ENTIDAD       → Estado (1-32)
ID_MUNICIPIO     → Municipio
ID_HORA          → Hora del accidente (0-23)
ANIO             → Año
MES              → Mes (1-12)
CAUSAACCI        → Causa del accidente
TOTAL_MUERTOS    → Calculado en limpieza.py
TOTAL_HERIDOS    → Calculado en limpieza.py
```

## 🧹 Limpieza de Datos

`limpieza.py` hace:

1. **Carga** parquet desde `data/parquet/`
2. **Estandariza** columnas a mayúsculas
3. **Convierte** 12 columnas de lesiones a numéricas
4. **Calcula** `TOTAL_MUERTOS` y `TOTAL_HERIDOS`
5. **Elimina nulos** solo en columnas críticas (no en todas)

```python
df = limpiar_datos('data/parquet/atus_anual_2023.parquet')
print(len(df))  # ~390,000 accidentes en 2023
```

## 📋 Requisitos

- Docker & Docker Compose
- O: Python 3.11+, pandas 3.0.3, ray 2.55.1, pyarrow 24.0.0

## 🛠️ Detener y Limpiar

```bash
# Detener contenedores
docker-compose down

# Detener y limpiar volúmenes
docker-compose down -v

# Ver logs históricos
docker-compose logs ray-head
```

## 👥 Para tus compañeros

1. Clonan el repo
2. `docker-compose up`
3. Ver resultados: `docker-compose logs ray-head`
4. Dashboard: http://localhost:8265

¡Listo! 🎯
