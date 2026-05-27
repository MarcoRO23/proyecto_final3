import pandas as pd


def limpiar_datos(ruta):
    df = pd.read_parquet(ruta)

    # Estandarización
    df.columns = df.columns.str.upper()

    # Convertir columnas numéricas
    columnas_numericas = [
        "CONDMUERTO",
        "PASAMUERTO",
        "PEATMUERTO",
        "CICLMUERTO",
        "OTROMUERTO",
        "NEMUERTO",
        "CONDHERIDO",
        "PASAHERIDO",
        "PEATHERIDO",
        "CICLHERIDO",
        "OTROHERIDO",
        "NEHERIDO",
    ]

    for col in columnas_numericas:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Totales
    df["TOTAL_MUERTOS"] = (
        df["CONDMUERTO"]
        + df["PASAMUERTO"]
        + df["PEATMUERTO"]
        + df["CICLMUERTO"]
        + df["OTROMUERTO"]
        + df["NEMUERTO"]
    )

    df["TOTAL_HERIDOS"] = (
        df["CONDHERIDO"]
        + df["PASAHERIDO"]
        + df["PEATHERIDO"]
        + df["CICLHERIDO"]
        + df["OTROHERIDO"]
        + df["NEHERIDO"]
    )

    # Eliminar nulos SOLO EN COLUMNAS CRÍTICAS (no en todas)
    df = df.dropna(
        subset=["ID_ENTIDAD", "ID_MUNICIPIO", "ANIO", "MES", "ID_HORA", "CAUSAACCI"]
    )

    return df

