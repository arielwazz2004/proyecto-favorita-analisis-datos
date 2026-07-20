# 🥑 Pipeline ETL - Corporación Favorita 2026A

Pipeline automatizado de ingeniería de datos para la extracción, transformación, análisis y persistencia del dataset de ventas de Corporación Favorita. Implementado sobre una VM Ubuntu, utiliza **Apache Airflow** para la orquestación, **Polars** para el procesamiento eficiente de más de 3 millones de filas, **PostgreSQL** para almacenamiento estructurado, y **Power BI** para visualización en tiempo real mediante conexión DirectQuery.

---

## 1. Descripción del proyecto

Proyecto integrador que aplica todas las etapas del proceso de análisis de datos sobre el dataset **Store Sales — Time Series Forecasting** (Kaggle, Corporación Favorita). El pipeline carga, limpia, consolida y analiza más de 3 millones de registros de ventas diarias de 54 tiendas ecuatorianas, persistiendo los resultados en PostgreSQL para su consumo en tiempo real desde un dashboard de Power BI.

El flujo completo está orquestado por un DAG de Airflow (`favorita_pipeline`) con 6 tareas secuenciales, y todo el procesamiento de datos se realiza con Polars, sin usar pandas como motor de transformación.

---

## 2. Descripción de los archivos del dataset y su rol en el pipeline

| Archivo | Registros | Rol en el pipeline |
|---|---|---|
| `train.csv` | 3,000,888 | Archivo principal: ventas diarias por tienda, familia de producto y promoción. Base del dataset consolidado. |
| `stores.csv` | 54 | Metadata de tiendas (ciudad, provincia, tipo, clúster). Join por `store_nbr`. |
| `transactions.csv` | 83,488 | Número de transacciones diarias por tienda. Join por `store_nbr` + `date`. Usado para calcular ticket promedio. |
| `oil.csv` | 1,218 | Precio diario del petróleo. Contiene 43 valores nulos (fines de semana/feriados), corregidos con interpolación lineal. Join por `date`. |
| `holidays_events.csv` | 350 | Feriados nacionales, regionales y locales, con bandera de transferencia. Usado para el análisis de estacionalidad. Join por `date`. |
| `test.csv` | — | **No utilizado.** Corresponde al conjunto de predicción de la competencia de Kaggle; este proyecto es descriptivo, no predictivo. |

---

## 3. Diagrama de arquitectura de la solución

```
 CSV locales (carpeta data/)
          │
 ⏰ Apache Airflow — orquesta el DAG favorita_pipeline
          │
 ⚡ Polars — carga, limpieza, consolidación y EDA
          │
 🗄️ PostgreSQL (base "favorita") — 19 tablas persistidas
          │
 📊 Power BI — conexión DirectQuery en tiempo real, 8 visualizaciones
```

### 🖥️ Infraestructura

- **Entorno de ejecución:** WSL2 (Windows Subsystem for Linux) sobre Windows, distribución Ubuntu 24.04 LTS.
- **Recursos asignados:** 6 GB de RAM y 2 vCPUs (configurados vía `.wslconfig`).
- **Orquestador:** Apache Airflow 2.10.4, instalado con `pip` en un entorno virtual de Python.
- **Base de datos:** PostgreSQL instalado localmente dentro de la misma VM.
- **Control de versiones:** Repositorio en GitHub con `dags/`, `scripts/`, `manifest.json` y este README.

---

## 4. Descripción del DAG: tareas, dependencias y configuración

**DAG:** favorita_pipeline

**Tareas:**

1. **cargar_datos** — Lee los 5 CSV con Polars (`pl.read_csv`) y los guarda en formato parquet intermedio.
2. **eda_inicial** — Diagnóstico de calidad: filas, columnas, tipos de dato, nulos (conteo y %), duplicados y rango de fechas. Guarda el resultado en `reporte_eda_inicial.json`.
3. **limpiar_datos** — Interpolación lineal de `dcoilwtico` en `oil.csv` (con relleno forward/backward para los extremos de la serie). El resto de tablas no requirió limpieza adicional (0 duplicados, 0 nulos).
4. **consolidar** — Une las 5 tablas mediante joins por `store_nbr`, `date` y `family`, generando el dataset unificado.
5. **eda_profundo** — Análisis estadístico descriptivo completo: ventas por familia/tienda/ciudad/provincia, evolución temporal, feriados, promociones, correlación con el petróleo, y transacciones.
6. **exportar_postgres** — Escribe el dataset consolidado y las 16 tablas de estadísticos del EDA en PostgreSQL, en lotes de 50,000 filas para optimizar el uso de memoria.

**Dependencias:** flujo lineal estricto `tarea_1 >> tarea_2 >> tarea_3 >> tarea_4 >> tarea_5 >> tarea_6`. Sin ramas paralelas; si una tarea falla, las siguientes no se ejecutan.

**Configuración:**
```python
schedule = None       
start_date = datetime(2026, 7, 1)
catchup = False
retries = 1
retry_delay = timedelta(minutes=5)
on_failure_callback = registrar_fallo  
```

---

## 5. Proceso del pipeline: descripción de cada etapa con capturas de Airflow

- **Carga:** lectura de los 5 CSV, confirmando 3,000,888 filas en `train`, 54 en `stores`, 83,488 en `transactions`, 1,218 en `oil` y 350 en `holidays`.
- **EDA inicial:** identifica que la única tabla con valores faltantes es `oil` (43 nulos, 3.53%); ninguna tabla presenta filas duplicadas.
- **Limpieza:** aplica interpolación lineal sobre la serie de precios del petróleo, dejando el dataset sin nulos.
- **Consolidación:** genera un dataset unificado de 3,000,888 filas y 13 columnas.
- **EDA profundo:** produce 16 tablas de estadísticos (ventas por familia, top/bottom 10 tiendas, evolución mensual/anual, feriados nacionales, sensibilidad por familia, promociones, correlación petróleo-ventas, ticket promedio, etc.).
- **Exportación:** persiste el consolidado y las 16 tablas de EDA en PostgreSQL.
- 
<img width="894" height="552" alt="image" src="https://github.com/user-attachments/assets/a995123f-ac51-404e-814f-def01b4e5b38" />

<img width="856" height="445" alt="image" src="https://github.com/user-attachments/assets/16ed6f19-1032-43e8-93d2-2c2597ac0299" />

---

## 6. Métricas del pipeline

| Etapa | Registros procesados | Registros modificados/corregidos |
|---|---|---|
| cargar_datos | 3,000,888 (train) + 54 + 83,488 + 1,218 + 350 | — |
| eda_inicial | 5 tablas diagnosticadas | — (solo lectura) |
| limpiar_datos | 5 tablas | 43 valores nulos en `oil` corregidos por interpolación lineal. 0 duplicados eliminados (no existían). |
| consolidar | 3,000,888 filas de salida | 13 columnas resultantes |
| eda_profundo | 3,000,888 filas de entrada | 16 tablas de estadísticos generadas |
| exportar_postgres | 3,000,888 filas (tabla principal) + 16 tablas de EDA | Exportado en 61 lotes de 50,000 filas |

**Tiempo total de ejecución del pipeline:** `00:09:30`

<img width="1193" height="725" alt="image" src="https://github.com/user-attachments/assets/6baece84-43fb-4097-bd60-11eac0563a4f" />

<img width="1156" height="650" alt="image" src="https://github.com/user-attachments/assets/559208db-c201-446a-bd36-776f9057c2ce" />

---

## 7. Capturas del dashboard de Power BI

Conexión configurada en modo **DirectQuery** contra la base de datos PostgreSQL local (`favorita`), sin importar una copia estática de los datos.

**Evidencias de EDA Profundo mediante Power BI:**

1. 🛒 Ventas totales por familia de producto — *GROCERY I* lidera con ~343 millones.
<img width="1600" height="720" alt="image" src="https://github.com/user-attachments/assets/7af38671-c050-4a03-b063-530db09bf60c" />
<img width="1600" height="664" alt="image" src="https://github.com/user-attachments/assets/667cb96c-0782-49a5-8135-7cd256bc5388" />
2. 📈 Evolución mensual de ventas 2013-2017 — pico máximo en diciembre 2016 (~29 millones).
<img width="1600" height="726" alt="image" src="https://github.com/user-attachments/assets/b662d5d6-a6c6-492c-aa51-dc61eb4319e3" />
3. 🗺️ Mapa de ventas por ciudad — *Quito* concentra ~556 millones, muy por encima de Guayaquil.
<img width="1600" height="723" alt="image" src="https://github.com/user-attachments/assets/43f44c91-4a2d-4eb7-98ca-e247126fc66e" />
4. 🎉 Impacto de feriados nacionales — venta promedio de 419 en feriado vs 352 en día normal.
<img width="1600" height="741" alt="image" src="https://github.com/user-attachments/assets/73e949c2-c804-4470-9570-29f263c01736" />
5. 🛢️ Correlación precio del petróleo vs ventas mensuales (dispersión) — correlación de **-0.75**.
<img width="1600" height="729" alt="image" src="https://github.com/user-attachments/assets/30d28454-8669-4aa2-bbc3-cfe5d14bcea3" />
6. 🏷️ Comparativo ventas con y sin promoción — 1,137.7 vs 158.2 (más de 7 veces mayor con promoción).
<img width="1600" height="734" alt="image" src="https://github.com/user-attachments/assets/424361aa-82cb-4c8e-9f6f-db60a751079c" />
7. 🏪 Ranking de tiendas por ventas totales.
<img width="1521" height="790" alt="image" src="https://github.com/user-attachments/assets/9159fd96-4fbe-48a7-8083-46d9e5598da2" />
8. 🥇 Familia de productos con mayor volumen: *GROCERY I*.
<img width="377" height="171" alt="image" src="https://github.com/user-attachments/assets/06e4d970-add4-4424-bb88-743b548065e0" />

<img width="1396" height="738" alt="image" src="https://github.com/user-attachments/assets/c75cb091-e556-4f14-aa87-efbb563f7277" />


---

## 8. Despliegue: instrucciones para reproducir el ambiente

### 8.1 Preparar el entorno base (WSL2 + Ubuntu)

```bash
wsl --install -d Ubuntu
```

Configurar recursos en `C:\Users\ariel\.wslconfig`:
```ini
[wsl2]
memory=6GB
processors=2
```

### 8.2 Actualizar el sistema e instalar dependencias

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-venv python3-pip git postgresql postgresql-contrib -y
sudo service postgresql start
```

### 8.3 Configurar PostgreSQL

```bash
sudo -u postgres psql
ALTER USER postgres PASSWORD 'favorita2026';
CREATE DATABASE favorita;
\q
```

### 8.4 Clonar el repositorio y crear el entorno virtual

```bash
git clone https://github.com/arielwazz2004/proyecto-favorita-analisis-datos.git
cd proyecto-favorita-analisis-datos
python3 -m venv airflow_venv
source airflow_venv/bin/activate
```

### 8.5 Instalar Airflow y dependencias

```bash
AIRFLOW_VERSION=2.10.4
PYTHON_VERSION="$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
pip install polars pandas pyarrow psycopg2-binary sqlalchemy
```

### 8.6 Copiar los CSV del dataset

Colocar los 5 archivos CSV (descargados de Kaggle) en la carpeta `data/` dentro del proyecto (esta carpeta no se sube al repositorio).

### 8.7 Inicializar y levantar Airflow

```bash
export AIRFLOW_HOME=~/proyecto-favorita-analisis-datos/airflow
airflow db migrate
airflow users create --username admin --firstname Nombre --lastname Apellido --role Admin --email correo@epn.edu.ec --password admin123

mkdir -p ~/proyecto-favorita-analisis-datos/airflow/dags
ln -s ~/proyecto-favorita-analisis-datos/dags/favorita_pipeline.py ~/proyecto-favorita-analisis-datos/airflow/dags/favorita_pipeline.py

airflow standalone
```

### 8.8 Ejecutar el pipeline

Acceder a `http://localhost:8080`, activar el DAG `favorita_pipeline` y disparar la ejecución manualmente con el botón play.

### 8.9 Conectar Power BI

En Power BI Desktop: **Obtener datos → Base de datos PostgreSQL**, servidor `localhost:5432` (o la IP de WSL si `localhost` no resuelve, obtenible con `hostname -I`), base de datos `favorita`, modo **DirectQuery**.

---

## 9. Conclusiones y recomendaciones

### 9.1 Conclusiones

- **GROCERY I y BEVERAGES concentran la mayor parte del volumen de ventas**, sugiriendo que la gestión de inventario y abastecimiento debería priorizar estas categorías.
- **Las promociones tienen un efecto comercial muy fuerte**: la venta promedio en productos con promoción es más de 7 veces mayor que sin ella, validando su uso como palanca de ventas.
- **Existe una correlación negativa fuerte (-0.75) entre el precio del petróleo y las ventas totales mensuales**, reflejando la dependencia de la economía ecuatoriana al crudo.
- **Quito concentra más de la mitad de las ventas totales** del país, lo que podría orientar decisiones de expansión hacia otras ciudades con menor participación.
- **Los feriados nacionales incrementan la venta promedio diaria** de forma medible (419 vs 352), confirmando el efecto de estacionalidad esperado.
- **Polars permitió procesar más de 3 millones de registros de forma eficiente** dentro de un entorno con recursos limitados (6 GB de RAM), gracias a la exportación por lotes en la etapa final del pipeline.

### 9.2 Recomendaciones

- Migrar la estrategia de exportación en PostgreSQL de sobrescritura total (`if_table_exists="replace"`) hacia una estructura con particionamiento por fecha, para mejorar el rendimiento de las consultas DirectQuery desde Power BI.
- Evaluar el uso de índices sobre `date` y `store_nbr` en la tabla `ventas_consolidado` para acelerar las consultas del dashboard.
- Para futuras iteraciones, considerar el uso de GitHub Actions para disparar el pipeline automáticamente al detectar cambios en `manifest.json`, reduciendo la dependencia de la ejecución manual.

---

## 10. Equipo y Repositorio

### 10.1 Integrantes

- **Erick Ariel Campoverde Pallo** — erick.campoverde@epn.edu.ec
- **Sebastián Chanchay** — sebastian.chanchay@epn.edu.ec

### 10.2 Estructura del repositorio

```
├── dags/
│   └── favorita_pipeline.py
├── scripts/
│   ├── config.py
│   ├── cargar_datos.py
│   ├── eda_inicial.py
│   ├── limpiar_datos.py
│   ├── consolidar.py
│   ├── eda_profundo.py
│   └── exportar_postgres.py
├── capturas/
│   └── (capturas de Airflow y Power BI)
├── manifest.json
├── .gitignore
└── README.md
```

Los archivos de datos (`data/`, `staging/`) y el entorno virtual (`airflow_venv/`, `airflow/`) están excluidos del repositorio mediante `.gitignore`.

**Repositorio:** https://github.com/arielwazz2004/proyecto-favorita-analisis-datos
