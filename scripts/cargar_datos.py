import polars as pl
from config import DATA_DIR, STAGING_DIR
import os

def cargar_datos():
    archivos = {
        "train": "train.csv",
        "stores": "stores.csv",
        "transactions": "transactions.csv",
        "oil": "oil.csv",
        "holidays": "holidays_events.csv",
    }

    for nombre, archivo in archivos.items():
        ruta = os.path.join(DATA_DIR, archivo)
        df = pl.read_csv(ruta, try_parse_dates=True)

        salida = os.path.join(STAGING_DIR, f"{nombre}.parquet")
        df.write_parquet(salida)

        print(f"{nombre}: {df.height} filas, {df.width} columnas -> guardado en {salida}")

if __name__ == "__main__":
    cargar_datos()