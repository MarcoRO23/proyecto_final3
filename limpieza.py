import pandas as pd


def limpiar_datos(ruta):
    df = pd.read_parquet(ruta)
    df.columns = df.columns.str.upper()

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

    muertos_cols = [c for c in columnas_numericas if "MUERTO" in c and c in df.columns]
    heridos_cols = [c for c in columnas_numericas if "HERIDO" in c and c in df.columns]

    df["TOTAL_MUERTOS"] = df[muertos_cols].sum(axis=1) if muertos_cols else 0
    df["TOTAL_HERIDOS"] = df[heridos_cols].sum(axis=1) if heridos_cols else 0

    if "ID_ENTIDAD" in df.columns:
        df["ID_ENTIDAD"] = pd.to_numeric(df["ID_ENTIDAD"], errors="coerce")
        df = df.dropna(subset=["ID_ENTIDAD"])
        df["ID_ENTIDAD"] = df["ID_ENTIDAD"].astype(int)

    return df
