import pandas as pd
import ray
from workers import (
    procesar_entidades_estados,
    procesar_municipios,
    procesar_victimas,
    procesar_horas,
    procesar_causas,
    procesar_meses,
)


def _dividir_entidades(num_workers=3):
    """Divide 32 entidades en grupos para cada worker"""
    entidades = list(range(1, 33))
    grupos = [[] for _ in range(num_workers)]
    for i, ent in enumerate(entidades):
        grupos[i % num_workers].append(ent)
    return grupos


def estados_mas_accidentes(df):
    """¿Qué estados concentran más accidentes? (DISTRIBUIDO EN 3 WORKERS)"""
    grupos = _dividir_entidades(3)
    tareas = [
        procesar_entidades_estados.remote(df, grupo, i)
        for i, grupo in enumerate(grupos)
    ]
    resultados = ray.get(tareas)

    combinado = {}
    for r in resultados:
        combinado.update(r["data"])

    return sorted(combinado.items(), key=lambda x: x[1], reverse=True)


def municipios_siniestralidad(df):
    """¿Qué municipios presentan mayor siniestralidad? (DISTRIBUIDO EN 3 WORKERS)"""
    grupos = _dividir_entidades(3)
    tareas = [
        procesar_municipios.remote(df, grupo, i) for i, grupo in enumerate(grupos)
    ]
    resultados = ray.get(tareas)

    datos = [item for r in resultados for item in r["data"]]
    df_temp = pd.DataFrame(datos)
    df_temp = df_temp.sort_values("total_accidentes", ascending=False)
    return df_temp.to_dict(orient="records")


def horarios_accidentes(df):
    """¿En qué horarios ocurren más accidentes?"""
    data = ray.get(procesar_horas.remote(df, 1))["data"]
    return sorted(data.items(), key=lambda x: x[1], reverse=True)


def causas_frecuentes(df):
    """¿Qué causas son más frecuentes?"""
    data = ray.get(procesar_causas.remote(df, 1))["data"]
    return sorted(data.items(), key=lambda x: x[1], reverse=True)


def meses_incidencia(df):
    """¿Qué meses presentan mayor incidencia?"""
    data = ray.get(procesar_meses.remote(df, 1))["data"]
    df_temp = pd.DataFrame(data)
    df_temp = df_temp.sort_values(["ANIO", "MES"])
    return df_temp.to_dict(orient="records")


def victimas_por_entidad(df):
    """¿Dónde hay más víctimas heridas o fallecidas? (DISTRIBUIDO EN 3 WORKERS)"""
    grupos = _dividir_entidades(3)
    tareas = [procesar_victimas.remote(df, grupo, i) for i, grupo in enumerate(grupos)]
    resultados = ray.get(tareas)

    datos = [item for r in resultados for item in r["data"]]
    df_temp = pd.DataFrame(datos)
    df_temp = df_temp.sort_values("TOTAL_VICTIMAS", ascending=False)
    return df_temp.to_dict(orient="records")
