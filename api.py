from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import json
import os

app = FastAPI(title="API Accidentes Viales México")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DATOS_DASHBOARD = []
ruta_datos = "data/preprocesado/dashboard_data.json"

if os.path.exists(ruta_datos):
    with open(ruta_datos, "r", encoding="utf-8") as f:
        DATOS_DASHBOARD = json.load(f)

NOMBRES_ESTADOS = {
    1: "AGUASCALIENTES",
    2: "BAJA CALIFORNIA",
    3: "BAJA CALIFORNIA SUR",
    4: "CAMPECHE",
    5: "COAHUILA",
    6: "COLIMA",
    7: "CHIAPAS",
    8: "CHIHUAHUA",
    9: "CIUDAD DE MEXICO",
    10: "DURANGO",
    11: "GUANAJUATO",
    12: "GUERRERO",
    13: "HIDALGO",
    14: "JALISCO",
    15: "MEXICO",
    16: "MICHOACAN",
    17: "MORELOS",
    18: "NAYARIT",
    19: "NUEVO LEON",
    20: "OAXACA",
    21: "PUEBLA",
    22: "QUERETARO",
    23: "QUINTANA ROO",
    24: "SAN LUIS POTOSI",
    25: "SINALOA",
    26: "SONORA",
    27: "TABASCO",
    28: "TAMAULIPAS",
    29: "TLAXCALA",
    30: "VERACRUZ",
    31: "YUCATAN",
    32: "ZACATECAS",
}


@app.get("/api/anios")
def obtener_anios():
    return {"anios": [d["anio"] for d in DATOS_DASHBOARD]}


@app.get("/api/dashboard/{anio}")
def obtener_dashboard(anio: int):
    # Buscar el año solicitado
    data_anio = next((d for d in DATOS_DASHBOARD if d["anio"] == anio), None)
    if not data_anio:
        return {"error": "Año no encontrado"}

    # Enriquecer los estados con sus nombres oficiales
    for estado in data_anio["estados"]:
        # === SOLUCIÓN: Convertir forzosamente el ID a entero ===
        try:
            # Convierte cosas como "01", "1", o 1.0 en un entero 1
            id_limpio = int(float(estado["ID_ENTIDAD"]))
        except (ValueError, TypeError):
            # Si viene vacío o corrupto, asignamos un -1 para que mande a Desconocido
            id_limpio = -1

        estado["nombre_entidad"] = NOMBRES_ESTADOS.get(id_limpio, "Desconocido")

    return data_anio
