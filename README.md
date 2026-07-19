# Pipeline ETL - Corporación Favorita 2026A

## 1. Descripción del proyecto
Pipeline automatizado de ingeniería de datos para la extracción, transformación, análisis y persistencia del dataset de ventas de Corporación Favorita. Implementado sobre una VM Ubuntu 22.04, utiliza **Apache Airflow** para la orquestación, **Polars** para el procesamiento eficiente de 3M+ filas, **PostgreSQL** para almacenamiento estructurado y **Power BI** para visualización en tiempo real mediante conexión DirectQuery. Desarrollado colaborativamente por los integrantes con el control de flujo de trabajo en GitHub.

## 2. Descripción de los archivos del dataset y su rol en el pipeline
| Archivo | Filas Aprox. | Rol en el Pipeline |
| :--- | :--- | :--- |
| `train_.csv` | ~1,050,000 | Dataset principal. Ventas diarias por tienda, familia y promoción. Se concatena y procesa con LazyFrame. |
| `stores.csv` | 54 | Metadata geográfica y de tipo de tienda. Join por `store_nbr`. |
| `oil.csv` | ~1,100 | Precio diario del petróleo. Nulos interpolados linealmente. Join por `date`. |
| `holidays_events.csv` | ~300 | Feriados nacionales/locales y eventos. Genera flags `is_holiday`. Join por `date`. |
| `transactions.csv` | ~120k | Transacciones diarias por tienda. Indicador de tráfico. Join por `date`+`store_nbr`. |

## 3. Diagrama de arquitectura de la solución


## 4. Descripción del DAG: tareas, dependencias y configuración


## 5. Proceso del pipeline: descripción de cada etapa con capturas de Airflow


## 6. Métricas del pipeline


## 7. Capturas del dashboard de Power BI


## 8. Despliegue: instrucciones para reproducir el ambiente


## 9. Conclusiones y recomendaciones


## 10. Equipo y Repositorio