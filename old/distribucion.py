import ray
import time


@ray.remote
def procesar_entidades(df_particion, worker_id):
    inicio = time.time()

    # =========================
    # NIVEL 1: por entidad (ya existente)
    # =========================
    total = len(df_particion)

    fatales = df_particion[df_particion["TOTAL_MUERTOS"] > 0].shape[0]

    # =========================
    # NIVEL 2: por municipio
    # =========================
    por_municipio = df_particion.groupby("ID_MUNICIPIO").size().to_dict()

    # =========================
    # NIVEL 3: por año-mes
    # =========================
    por_mes = df_particion.groupby(["ANIO", "MES"]).size().to_dict()

    fin = time.time()

    return {
        "worker": worker_id,
        "registros": total,
        "fatales": fatales,
        "por_municipio": por_municipio,
        "por_mes": por_mes,
        "tiempo": round(fin - inicio, 4),
    }


def ejecutar_cluster(df, n_workers=3):
    entidades = sorted(df["ID_ENTIDAD"].unique())

    grupos = [entidades[i::n_workers] for i in range(n_workers)]

    tareas = []

    for i, grupo in enumerate(grupos):
        particion = df[df["ID_ENTIDAD"].isin(grupo)]
        tareas.append(procesar_entidades.remote(particion, i + 1))

    return ray.get(tareas)

