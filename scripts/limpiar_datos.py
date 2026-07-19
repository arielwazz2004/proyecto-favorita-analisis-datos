import polars as pl
from config import STAGING_DIR
import os

def limpiar_oil():
    df = pl.read_parquet(os.path.join(STAGING_DIR, "oil.parquet"))
    df = df.sort("date").with_columns(
        pl.col("dcoilwtico").interpolate()
    )

    df = df.with_columns(pl.col("dcoilwtico").fill_null(strategy="forward"))
    df = df.with_columns(pl.col("dcoilwtico").fill_null(strategy="backward"))
    return df

def limpiar_datos():
    tablas = ["train", "stores", "transactions", "holidays"]

  
    for nombre in tablas:
        df = pl.read_parquet(os.path.join(STAGING_DIR, f"{nombre}.parquet"))
        df.write_parquet(os.path.join(STAGING_DIR, f"{nombre}_clean.parquet"))
        print(f"{nombre}_clean: {df.height} filas (sin cambios)")

   
    oil_limpio = limpiar_oil()
    oil_limpio.write_parquet(os.path.join(STAGING_DIR, "oil_clean.parquet"))
    print(f"oil_clean: {oil_limpio.height} filas (nulos interpolados)")

if __name__ == "__main__":
    limpiar_datos()