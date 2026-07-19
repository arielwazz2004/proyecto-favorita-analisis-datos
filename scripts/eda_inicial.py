import polars as pl
from config import STAGING_DIR
import os

def diagnosticar(nombre):
    ruta = os.path.join(STAGING_DIR, f"{nombre}.parquet")
    df = pl.read_parquet(ruta)

    nulos = df.null_count().to_dicts()[0]
    duplicados = df.height - df.unique().height

    print(f"\n=== {nombre} ===")
    print(f"Filas: {df.height} | Columnas: {df.width}")
    print(f"Duplicados: {duplicados}")
    print(f"Nulos por columna: {nulos}")

def eda_inicial():
    tablas = ["train", "stores", "transactions", "oil", "holidays"]
    for t in tablas:
        diagnosticar(t)

if __name__ == "__main__":
    eda_inicial()