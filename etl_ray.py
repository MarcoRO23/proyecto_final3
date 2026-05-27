import ray
import pandas as pd
import glob
import json
import os
from limpieza import limpiar_datos

ray.init(address="auto")


def _dividir_tareas(lista, num_workers=3):
    grupos = [[] for _ in range(num_workers)]
    for i, item in enumerate(lista):
        grupos[i % num_workers].append(item)
    return grupos


@ray.remote
def procesar_lote_archivos(rutas_lote, worker_id):
    print(f"👷 Worker {worker_id} iniciando procesamiento de {len(rutas_lote)} años...")
    resultados_lote = []

    for ruta in rutas_lote:
        anio = int(ruta.split("_")[-1].split(".")[0])
        df = limpiar_datos(ruta)

        # 1. Agrupación por Estado (Para el Mapa y Tablas)
        estados = (
            df.groupby("ID_ENTIDAD")
            .agg(
                total_accidentes=("ID_ENTIDAD", "count"),
                total_muertos=("TOTAL_MUERTOS", "sum"),
                total_heridos=("TOTAL_HERIDOS", "sum"),
            )
            .reset_index()
        )

        causas_estado = (
            df.groupby(["ID_ENTIDAD", "CAUSAACCI"]).size().reset_index(name="count")
        )
        causas_top = causas_estado.sort_values(
            "count", ascending=False
        ).drop_duplicates(["ID_ENTIDAD"])

        estados = estados.merge(
            causas_top[["ID_ENTIDAD", "CAUSAACCI"]], on="ID_ENTIDAD", how="left"
        )
        estados.rename(columns={"CAUSAACCI": "causa_principal"}, inplace=True)
        estados["tasa_mortalidad"] = round(
            (estados["total_muertos"] / estados["total_accidentes"]) * 100, 2
        )
        estados["worker_id"] = worker_id

        # 2. Agrupación por Horas (Para el gráfico de líneas)
        horas = (
            df.groupby("ID_HORA")
            .size()
            .reset_index(name="total_accidentes")
            .to_dict(orient="records")
        )

        # 3. Agrupación de Top Causas Globales (Para el gráfico de barras horizontales)
        causas_globales = (
            df.groupby("CAUSAACCI").size().reset_index(name="total_accidentes")
        )
        causas_globales = (
            causas_globales.sort_values("total_accidentes", ascending=False)
            .head(10)
            .to_dict(orient="records")
        )

        # Empaquetar todo el año en un solo objeto
        resultados_lote.append(
            {
                "anio": anio,
                "estados": estados.to_dict(orient="records"),
                "horas": horas,
                "causas": causas_globales,
            }
        )

    return resultados_lote


def ejecutar_etl(num_workers=3):
    print("🚀 Iniciando procesamiento distribuido con Ray (Dashboard Completo)...")
    rutas = glob.glob("data/parquet/atus_anual_*.parquet")

    if not rutas:
        print("❌ No se encontraron archivos parquet.")
        return

    lotes_de_archivos = _dividir_tareas(rutas, num_workers)
    tareas = [
        procesar_lote_archivos.remote(lote, i + 1)
        for i, lote in enumerate(lotes_de_archivos)
        if lote
    ]
    resultados_workers = ray.get(tareas)

    # Aplanar la lista de años
    datos_finales = [
        anio_data for sublist in resultados_workers for anio_data in sublist
    ]
    # Ordenar por año para mayor limpieza
    datos_finales = sorted(datos_finales, key=lambda x: x["anio"])

    os.makedirs("data/preprocesado", exist_ok=True)
    ruta_salida = "data/preprocesado/dashboard_data.json"

    with open(ruta_salida, "w", encoding="utf-8") as f:
        json.dump(datos_finales, f, ensure_ascii=False)

    print(f"✅ ETL completado. Datos del dashboard listos en: {ruta_salida}")


if __name__ == "__main__":
    ejecutar_etl(num_workers=3)
