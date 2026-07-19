import polars as pl
from config import STAGING_DIR, POSTGRES_URI
import os
import glob

import gc

def exportar_tabla(ruta_parquet, nombre_tabla, chunk_size=50_000):
    df = pl.read_parquet(ruta_parquet)
    total = df.height

    if total <= chunk_size:
        df.write_database(
            table_name=nombre_tabla,
            connection=POSTGRES_URI,
            if_table_exists="replace",
        )
    else:
        for i, start in enumerate(range(0, total, chunk_size)):
            lote = df.slice(start, chunk_size)
            modo = "replace" if i == 0 else "append"
            lote.write_database(
                table_name=nombre_tabla,
                connection=POSTGRES_URI,
                if_table_exists=modo,
            )
            del lote
            gc.collect()
            print(f"  lote {i+1}: filas {start} a {min(start+chunk_size, total)}")

    del df
    gc.collect()
    print(f"{nombre_tabla}: {total} filas exportadas")
def exportar_postgres():

    exportar_tabla(os.path.join(STAGING_DIR, "consolidado.parquet"), "ventas_consolidado")

    for ruta in glob.glob(os.path.join(STAGING_DIR, "eda_*.parquet")):
        nombre_tabla = os.path.basename(ruta).replace(".parquet", "")
        exportar_tabla(ruta, nombre_tabla)

    print("Exportación completa.")

if __name__ == "__main__":
    exportar_postgres()