import sys
import os
import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.expanduser("~/proyecto-favorita/scripts"))

from cargar_datos import cargar_datos
from eda_inicial import eda_inicial
from limpiar_datos import limpiar_datos
from consolidar import consolidar
from eda_profundo import eda_profundo
from exportar_postgres import exportar_postgres


def registrar_fallo(context):
    tarea = context.get("task_instance")
    logging.error(
        f"[favorita_pipeline] FALLO en la tarea '{tarea.task_id}' "
        f"— ejecución: {context.get('execution_date')}"
    )


default_args = {
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": registrar_fallo,
}

with DAG(
    dag_id="favorita_pipeline",
    description="Pipeline de ventas Corporación Favorita",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["favorita", "proyecto"],
) as dag:

    tarea_1 = PythonOperator(
        task_id="cargar_datos",
        python_callable=cargar_datos,
    )

    tarea_2 = PythonOperator(
        task_id="eda_inicial",
        python_callable=eda_inicial,
    )

    tarea_3 = PythonOperator(
        task_id="limpiar_datos",
        python_callable=limpiar_datos,
    )

    tarea_4 = PythonOperator(
        task_id="consolidar",
        python_callable=consolidar,
    )

    tarea_5 = PythonOperator(
        task_id="eda_profundo",
        python_callable=eda_profundo,
    )

    tarea_6 = PythonOperator(
        task_id="exportar_postgres",
        python_callable=exportar_postgres,
    )

    tarea_1 >> tarea_2 >> tarea_3 >> tarea_4 >> tarea_5 >> tarea_6