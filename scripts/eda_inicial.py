import polars as pl
from config import STAGING_DIR
import os
import json


def diagnosticar(nombre: str) -> dict:
    ruta = os.path.join(STAGING_DIR, f"{nombre}.parquet")
    df = pl.read_parquet(ruta)

    filas = df.height
    columnas = df.width

    tipos = {col: str(dtype) for col, dtype in zip(df.columns, df.dtypes)}

    nulos_conteo = df.null_count().to_dicts()[0]
    nulos_pct = {
        col: round((cnt / filas) * 100, 2) if filas > 0 else 0.0
        for col, cnt in nulos_conteo.items()
    }

    duplicados = filas - df.unique().height

    rango_fechas = None
    if "date" in df.columns:
        fecha_min = df.select(pl.col("date").min()).item()
        fecha_max = df.select(pl.col("date").max()).item()
        rango_fechas = {"min": str(fecha_min), "max": str(fecha_max)}

    resultado = {
        "tabla": nombre,
        "filas": filas,
        "columnas": columnas,
        "tipos_de_dato": tipos,
        "nulos_conteo": nulos_conteo,
        "nulos_porcentaje": nulos_pct,
        "duplicados": duplicados,
        "rango_fechas": rango_fechas,
    }

    print(f"\n=== {nombre} ===")
    print(f"Filas: {filas} | Columnas: {columnas}")
    print(f"Tipos de dato: {tipos}")
    print(f"Duplicados: {duplicados}")
    print(f"Nulos (conteo): {nulos_conteo}")
    print(f"Nulos (%): {nulos_pct}")
    if rango_fechas:
        print(f"Rango de fechas: {rango_fechas['min']} a {rango_fechas['max']}")

    return resultado


def eda_inicial():
    tablas = ["train", "stores", "transactions", "oil", "holidays"]
    reporte = [diagnosticar(t) for t in tablas]

    salida = os.path.join(STAGING_DIR, "reporte_eda_inicial.json")
    with open(salida, "w", encoding="utf-8") as f:
        json.dump(reporte, f, indent=2, ensure_ascii=False)

    print(f"\nReporte guardado en {salida}")
    return reporte


if __name__ == "__main__":
    eda_inicial()