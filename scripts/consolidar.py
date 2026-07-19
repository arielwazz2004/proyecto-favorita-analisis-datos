import polars as pl
from config import STAGING_DIR
import os

def cargar_clean(nombre):
    return pl.read_parquet(os.path.join(STAGING_DIR, f"{nombre}_clean.parquet"))

def consolidar():
    train = cargar_clean("train")
    stores = cargar_clean("stores")
    transactions = cargar_clean("transactions")
    oil = cargar_clean("oil")
    holidays = cargar_clean("holidays")

   
    df = train.join(stores, on="store_nbr", how="left")

   
    df = df.join(transactions, on=["store_nbr", "date"], how="left")

   
    df = df.join(oil, on="date", how="left")

   
    holidays_flag = (
        holidays.select(["date"])
        .unique()
        .with_columns(pl.lit(True).alias("es_feriado"))
    )
    df = df.join(holidays_flag, on="date", how="left")
    df = df.with_columns(pl.col("es_feriado").fill_null(False))

    salida = os.path.join(STAGING_DIR, "consolidado.parquet")
    df.write_parquet(salida)
    print(f"Consolidado: {df.height} filas, {df.width} columnas -> {salida}")

if __name__ == "__main__":
    consolidar()