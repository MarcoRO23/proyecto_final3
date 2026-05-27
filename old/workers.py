import pandas as pd
import ray


@ray.remote
def procesar_entidades_estados(df, entidades, worker_id):
    """Procesa ranking de estados para un grupo de entidades"""
    subset = df[df["ID_ENTIDAD"].isin(entidades)]
    resultado = subset.groupby("ID_ENTIDAD").size().to_dict()
    return {"worker": worker_id, "data": resultado}


@ray.remote
def procesar_municipios(df, entidades, worker_id):
    """Procesa municipios para un grupo de entidades"""
    subset = df[df["ID_ENTIDAD"].isin(entidades)]
    resultado = (
        subset.groupby(["ID_MUNICIPIO", "ID_ENTIDAD"])
        .size()
        .reset_index(name="total_accidentes")
    )
    return {"worker": worker_id, "data": resultado.to_dict(orient="records")}


@ray.remote
def procesar_victimas(df, entidades, worker_id):
    """Procesa víctimas para un grupo de entidades"""
    subset = df[df["ID_ENTIDAD"].isin(entidades)]
    resultado = (
        subset.groupby("ID_ENTIDAD")
        .agg({"TOTAL_MUERTOS": "sum", "TOTAL_HERIDOS": "sum"})
        .reset_index()
    )
    resultado["TOTAL_VICTIMAS"] = (
        resultado["TOTAL_MUERTOS"] + resultado["TOTAL_HERIDOS"]
    )
    return {"worker": worker_id, "data": resultado.to_dict(orient="records")}


@ray.remote
def procesar_horas(df, worker_id):
    """Procesa horarios"""
    resultado = df.groupby("ID_HORA").size().to_dict()
    return {"worker": worker_id, "data": resultado}


@ray.remote
def procesar_causas(df, worker_id):
    """Procesa causas"""
    resultado = df.groupby("CAUSAACCI").size().to_dict()
    return {"worker": worker_id, "data": resultado}


@ray.remote
def procesar_meses(df, worker_id):
    """Procesa meses"""
    resultado = df.groupby(["ANIO", "MES"]).size().reset_index(name="total_accidentes")
    return {"worker": worker_id, "data": resultado.to_dict(orient="records")}

