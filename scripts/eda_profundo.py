import polars as pl
from config import STAGING_DIR
import os


def cargar_consolidado() -> pl.DataFrame:
    return pl.read_parquet(os.path.join(STAGING_DIR, "consolidado.parquet"))


def cargar_holidays_clean() -> pl.DataFrame:
    return pl.read_parquet(os.path.join(STAGING_DIR, "holidays_clean.parquet"))


# ---------------------------------------------------------------------------
# VENTAS GENERALES
# ---------------------------------------------------------------------------

def ventas_por_familia(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("family")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )


def ventas_por_tienda(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )


def top_bottom_tiendas(ventas_tienda: pl.DataFrame) -> dict:
    top10 = ventas_tienda.sort("ventas_totales", descending=True).head(10)
    bottom10 = ventas_tienda.sort("ventas_totales", descending=False).head(10)
    return {"top10": top10, "bottom10": bottom10}


def ventas_por_ciudad(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("city")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort("venta_promedio", descending=True)
    )


def ventas_por_provincia(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("state")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort("venta_promedio", descending=True)
    )


def evolucion_mensual(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by("anio_mes")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("anio_mes")
    )


def evolucion_anual(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("date").dt.year().alias("anio"))
        .group_by("anio")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("anio")
    )


# ---------------------------------------------------------------------------
# ESTACIONALIDAD Y FERIADOS
# ---------------------------------------------------------------------------

def fechas_feriados_nacionales(holidays: pl.DataFrame) -> pl.DataFrame:
    return (
        holidays.filter(
            (pl.col("locale") == "National") & (pl.col("transferred") == False)
        )
        .select("date")
        .unique()
    )


def efecto_feriados_nacionales(df: pl.DataFrame, fechas_nac: pl.DataFrame) -> pl.DataFrame:
    fechas_set = fechas_nac.with_columns(pl.lit(True).alias("es_feriado_nacional"))
    df2 = df.join(fechas_set, on="date", how="left")
    df2 = df2.with_columns(pl.col("es_feriado_nacional").fill_null(False))
    return (
        df2.group_by("es_feriado_nacional")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )


def ventas_alrededor_feriados(df: pl.DataFrame, fechas_nac: pl.DataFrame) -> pl.DataFrame:
    resultados = []
    fechas_lista = fechas_nac.to_series().to_list()

    for fecha in fechas_lista:
        ventana = df.filter(
            (pl.col("date") >= fecha - pl.duration(days=3))
            & (pl.col("date") <= fecha + pl.duration(days=3))
        )
        if ventana.height == 0:
            continue
        resumen = (
            ventana.with_columns(
                (pl.col("date") - pl.lit(fecha)).dt.total_days().alias("offset_dias")
            )
            .group_by(["family", "offset_dias"])
            .agg(pl.col("sales").sum().alias("ventas_totales"))
        )
        resumen = resumen.with_columns(pl.lit(str(fecha)).alias("feriado"))
        resultados.append(resumen)

    if not resultados:
        return pl.DataFrame()
    return pl.concat(resultados)


def familias_sensibles_feriados(ventas_alrededor: pl.DataFrame) -> pl.DataFrame:
    if ventas_alrededor.height == 0:
        return pl.DataFrame()

    venta_feriado = (
        ventas_alrededor.filter(pl.col("offset_dias") == 0)
        .group_by("family")
        .agg(pl.col("ventas_totales").mean().alias("venta_dia_feriado"))
    )
    venta_normal = (
        ventas_alrededor.filter(pl.col("offset_dias") != 0)
        .group_by("family")
        .agg(pl.col("ventas_totales").mean().alias("venta_dias_normales"))
    )
    comparacion = venta_feriado.join(venta_normal, on="family", how="inner")
    comparacion = comparacion.with_columns(
        (pl.col("venta_dia_feriado") / pl.col("venta_dias_normales")).alias("ratio_sensibilidad")
    )
    return comparacion.sort("ratio_sensibilidad", descending=True)


# ---------------------------------------------------------------------------
# PROMOCIONES
# ---------------------------------------------------------------------------

def efecto_promociones_general(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns((pl.col("onpromotion") > 0).alias("en_promocion"))
        .group_by("en_promocion")
        .agg(pl.col("sales").mean().alias("venta_promedio"))
    )


def efecto_promociones_por_familia(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns((pl.col("onpromotion") > 0).alias("en_promocion"))
        .group_by(["family", "en_promocion"])
        .agg(pl.col("sales").mean().alias("venta_promedio"))
        .sort(["family", "en_promocion"])
    )


# ---------------------------------------------------------------------------
# PETRÓLEO Y ECONOMÍA
# ---------------------------------------------------------------------------

def serie_ventas_petroleo_mensual(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by("anio_mes")
        .agg(
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"),
        )
        .sort("anio_mes")
        .drop_nulls()
    )


def correlacion_petroleo_ventas(serie_mensual: pl.DataFrame) -> float:
    if serie_mensual.height < 2:
        return None
    corr = serie_mensual.select(
        pl.corr("ventas_totales", "precio_petroleo_promedio")
    ).item()
    return corr


def lag_petroleo_ventas_2015_2016(df: pl.DataFrame) -> pl.DataFrame:
    periodo = df.filter(
        (pl.col("date").dt.year() >= 2015) & (pl.col("date").dt.year() <= 2016)
    )
    return serie_ventas_petroleo_mensual(periodo)


def ciudades_sensibles_petroleo(df: pl.DataFrame) -> pl.DataFrame:
    mensual_ciudad = (
        df.with_columns(pl.col("date").dt.strftime("%Y-%m").alias("anio_mes"))
        .group_by(["city", "anio_mes"])
        .agg(
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"),
        )
        .drop_nulls()
    )

    resultados = []
    for ciudad in mensual_ciudad.select("city").unique().to_series().to_list():
        sub = mensual_ciudad.filter(pl.col("city") == ciudad)
        if sub.height < 3:
            continue
        corr = sub.select(pl.corr("ventas_totales", "precio_petroleo_promedio")).item()
        resultados.append({"city": ciudad, "correlacion_petroleo_ventas": corr})

    return pl.DataFrame(resultados).sort("correlacion_petroleo_ventas")


# ---------------------------------------------------------------------------
# TRANSACCIONES
# ---------------------------------------------------------------------------

def transacciones_vs_ventas(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df.group_by("store_nbr")
        .agg(
            pl.col("transactions").sum().alias("transacciones_totales"),
            pl.col("sales").sum().alias("ventas_totales"),
        )
        .with_columns(
            (pl.col("ventas_totales") / pl.col("transacciones_totales")).alias("ticket_promedio")
        )
        .sort("ticket_promedio", descending=True)
    )


# ---------------------------------------------------------------------------
# ORQUESTADOR
# ---------------------------------------------------------------------------

def eda_profundo():
    df = cargar_consolidado()
    holidays = cargar_holidays_clean()
    fechas_nac = fechas_feriados_nacionales(holidays)

    v_familia = ventas_por_familia(df)
    v_tienda = ventas_por_tienda(df)
    tb = top_bottom_tiendas(v_tienda)
    v_ciudad = ventas_por_ciudad(df)
    v_provincia = ventas_por_provincia(df)
    ev_mensual = evolucion_mensual(df)
    ev_anual = evolucion_anual(df)

    efecto_feriado_nac = efecto_feriados_nacionales(df, fechas_nac)
    ventana_feriados = ventas_alrededor_feriados(df, fechas_nac)
    sensibilidad_feriados = familias_sensibles_feriados(ventana_feriados)

    promo_general = efecto_promociones_general(df)
    promo_familia = efecto_promociones_por_familia(df)

    serie_petroleo = serie_ventas_petroleo_mensual(df)
    corr_petroleo = correlacion_petroleo_ventas(serie_petroleo)
    lag_15_16 = lag_petroleo_ventas_2015_2016(df)
    ciudades_petroleo = ciudades_sensibles_petroleo(df)

    ticket_tiendas = transacciones_vs_ventas(df)

    resultados = {
        "ventas_por_familia": v_familia,
        "ventas_por_tienda": v_tienda,
        "top10_tiendas": tb["top10"],
        "bottom10_tiendas": tb["bottom10"],
        "ventas_por_ciudad": v_ciudad,
        "ventas_por_provincia": v_provincia,
        "evolucion_mensual": ev_mensual,
        "evolucion_anual": ev_anual,
        "efecto_feriados_nacionales": efecto_feriado_nac,
        "sensibilidad_familias_feriados": sensibilidad_feriados,
        "efecto_promociones_general": promo_general,
        "efecto_promociones_por_familia": promo_familia,
        "serie_ventas_petroleo_mensual": serie_petroleo,
        "lag_petroleo_ventas_2015_2016": lag_15_16,
        "ciudades_sensibles_petroleo": ciudades_petroleo,
        "ticket_promedio_por_tienda": ticket_tiendas,
    }

    print(f"\nCorrelación general precio petróleo vs ventas mensuales: {corr_petroleo:.4f}"
          if corr_petroleo is not None else "\nCorrelación petróleo-ventas: sin datos suficientes")

    for nombre, tabla in resultados.items():
        if tabla is None or tabla.height == 0:
            print(f"[eda_profundo] {nombre}: sin datos (omitido)")
            continue
        salida = os.path.join(STAGING_DIR, f"eda_{nombre}.parquet")
        tabla.write_parquet(salida)
        print(f"[eda_profundo] {nombre}: {tabla.height} filas -> {salida}")

    return {nombre: (tabla.height if tabla is not None else 0) for nombre, tabla in resultados.items()}


if __name__ == "__main__":
    eda_profundo()