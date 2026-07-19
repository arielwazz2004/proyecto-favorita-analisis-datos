import polars as pl
from config import STAGING_DIR
import os

def cargar_consolidado():
    return pl.read_parquet(os.path.join(STAGING_DIR, "consolidado.parquet"))

def eda_profundo():
    df = cargar_consolidado()

    #1 Ventas totales por familia de producto
    ventas_familia = (
        df.group_by("family")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )

    #2 Ventas totales por tienda
    ventas_tienda = (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )

    #3 Ventas totales por ciudad
    ventas_ciudad = (
        df.group_by("city")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )

    #4 Venta promedio en feriado vs día normal
    efecto_feriados = (
        df.group_by("es_feriado")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )

    #5 Venta promedio con y sin promoción
    efecto_promociones = (
        df.with_columns((pl.col("onpromotion") > 0).alias("en_promocion"))
        .group_by("en_promocion")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )

    resultados = {
        "ventas_por_familia": ventas_familia,
        "ventas_por_tienda": ventas_tienda,
        "ventas_por_ciudad": ventas_ciudad,
        "efecto_feriados": efecto_feriados,
        "efecto_promociones": efecto_promociones,
    }

    for nombre, tabla in resultados.items():
        salida = os.path.join(STAGING_DIR, f"eda_{nombre}.parquet")
        tabla.write_parquet(salida)
        print(f"\n=== {nombre} ===")
        print(tabla)

if __name__ == "__main__":
    eda_profundo()