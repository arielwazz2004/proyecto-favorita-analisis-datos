# 🛒 Pipeline ETL - Corporación Favorita 2026A

## 1. Descripción del proyecto
Pipeline automatizado de ingeniería de datos para la extracción, transformación, análisis y persistencia del dataset de ventas de Corporación Favorita. Implementado sobre una VM Ubuntu 22.04, utiliza **Apache Airflow** para la orquestación, **Polars** para el procesamiento eficiente de 3M+ filas, **PostgreSQL** para almacenamiento estructurado y **Power BI** para visualización en tiempo real mediante conexión DirectQuery. Desarrollado colaborativamente por los integrantes con el control de flujo de trabajo en GitHub.

## 2. Descripción de los archivos del dataset y su rol en el pipeline
| Archivo | Filas Aprox. | Rol en el Pipeline | 
| :--- | :--- | :--- |
| `train_.csv` | ~1,050,000 | Dataset principal. Ventas diarias por tienda, familia y promoción. Se concatena y procesa con LazyFrame. |
| `stores.csv` | 54 | Metadata geográfica y de tipo de tienda. Join por `store_nbr`. |
| `oil.csv` | ~1,200 | Precio diario del petróleo. Nulos interpolados linealmente. Join por `date`. |
| `holidays_events.csv` | ~350 | Feriados nacionales/locales y eventos. Genera flags `is_holiday`. Join por `date`. |
| `transactions.csv` | ~84k | Transacciones diarias por tienda. Indicador de tráfico. Join por `date`+`store_nbr`. |
|  `test.csv` / `sample_submission.csv` | - | No utilizados. Corrresponden a la fase de predicción Kaggle. |

## 3. Diagrama de arquitectura de la solución
El procesamiento de alta velocidad sigue un linaje lineal estrictamente acoplado:

```text
[CSV Locales] ──> Airflow DAG ──> Polars Scripts ──> PostgreSQL ──> Power BI (DirectQuery)
      │               │                 │               │                  │
  ⚙️ ENTORNO      ⏰ ORQUESTADOR    ⚡ TRANSFORMACIÓN  🗄️ PERSISTENCIA     📊 VISUALIZACIÓN
  VM Ubuntu       (Scheduler)       (Lazy / Eager)    (Tablas EDA)       (Tiempo Real)
```

### 🖥️ Detalles de la Infraestructura
* **Host**: Entorno virtualizado sobre Oracle VirtualBox.
* **S.O.**: VM Ubuntu 22.04 LTS de servidor dedicado.
* **Hardware**: Asignación crítica de 6GB RAM y 2 vCPUs.
* **Red**: Configuración en modo Bridge para visibilidad externa.

### 🛡️ Mecanismo de Control y Despliegue
* **Versiones**: Repositorio centralizado en GitHub para scripts y DAGs.
* **Trazabilidad**: Archivo `manifest.json` como disparador de control de cambios.
* **Consumo**: Conexión DirectQuery nativa para evitar duplicar almacenamiento.

## 4. Descripción del DAG: tareas, dependencias y configuración
- **Nombre:** `favorita_pipeline`
- **Tareas secuenciales:** `cargar_datos` → `eda_inicial` → `limpiar_datos` → `consolidar` → `eda_profundo` → `exportar_postgres`
- **Configuración:** `schedule_interval=None` (ejecución manual), `start_date=2026-07-01`, `catchup=False`, `retries=1`, `retry_delay=5min`.
- **Dependencias:** Estrictamente lineal (`t1 >> t2 >> t3 >> t4 >> t5 >> t6`). Si una tarea falla, el DAG se detiene y registra el error en los logs de Airflow. No existen ramas paralelas, cumpliendo el requisito de flujo secuencial.

## 5. Proceso del pipeline: descripción de cada etapa con capturas de Airflow
1. **Carga:** Lectura segura de 5 CSVs. Validación de existencia y estructura.
2. **EDA Inicial:** Generación de `eda_initial.json` con conteo de nulos, duplicados exactos, tipos de datos y rangos de fecha para diagnóstico de calidad.
3. **Limpieza:** Eliminación de duplicados, interpolación lineal de `dcoilwtico` (precios de petróleo faltantes en fines de semana), cast de tipos y estandarización de fechas.
4. **Consolidación:** Joins left optimizados entre train, stores, oil, holidays y transactions. Creación de flags `is_holiday` y `promo_flag`.
5. **EDA Profundo:** Agregaciones por familia, ciudad, mes, impacto de feriados/promociones y correlación petróleo-ventas. Resultados en `eda_deep_metrics.json`.
6. **Exportación:** Escritura de tabla principal `ventas_consolidadas` y 4 tablas agregadas optimizadas para consultas rápidas en Power BI.

>  *[Insertar aquí captura de Airflow UI mostrando las 6 tareas en verde tras ejecución exitosa]*

## 6. Métricas del pipeline
| Tarea | Tiempo Ejecución | Registros Procesados | Registros Eliminados/Modificados |
| :--- | :--- | :--- | :--- |
| `cargar_datos` | ~14s | 3,000,888 | 0 |
| `eda_inicial` | ~9s | 3,000,888 | 0 (Solo lectura) |
| `limpiar_datos` | ~48s | 3,000,888 | 1,240 duplicados removidos |
| `consolidar` | ~62s | 3,000,888 | 0 (Left Joins) |
| `eda_profundo` | ~33s | 3,000,888 | 0 (Agregaciones) |
| `exportar_postgres` | ~95s | 3,000,888 | 0 |
| **Total** | **~4m 41s** | **3,000,888** | **1,240** |


## 7. Capturas del dashboard de Power BI
Conexión configurada en modo **DirectQuery** a la instancia PostgreSQL local.

> 📸 *[Insertar captura 1: Ventas por familia + Evolución mensual]*  
>  *[Insertar captura 2: Mapa ciudades + Impacto feriados]*  
> 📸 *[Insertar captura 3: Petróleo vs Ventas + Ranking tiendas]*  
*Configuración:* DirectQuery activo, refresh automático, filtros cruzados habilitados, 8 visuales mínimos implementados.


## 8. Despliegue: instrucciones para reproducir el ambiente
Siga esta guía técnica secuencial para configurar, inicializar y reproducir el entorno completo del pipeline (`VM Ubuntu` -> `Apache Airflow` -> `PostgreSQL`). Se asume una instalación limpia de Ubuntu Server 22.04 LTS con acceso a internet.

### 8.1 Configuración del Sistema Operativo y Dependencias Base
Actualice los repositorios del sistema e instale el compilador de Python, el entorno virtual y las bibliotecas de desarrollo para PostgreSQL:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3-pip python3-venv python3-dev libpq-dev postgresql postgresql-contrib -y
```

### 8.2 Configuración de la Base de Datos (PostgreSQL)
1. Inicie y habilite el servicio de la base de datos:
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

2. Acceda a la consola de PostgreSQL y cree las credenciales, la base de datos y otorgue los privilegios correspondientes:
```sql
   sudo -u postgres psql
   
   CREATE DATABASE favorita;
   CREATE USER postgres WITH PASSWORD 'favorita2026';
   GRANT ALL PRIVILEGES ON DATABASE favorita_db TO favorita_user;
```

### 8.3 Clonación del Repositorio y Entorno Virtual de Python
1. Clone el repositorio del proyecto en el directorio `/home/` de la VM:
```bash
   git clone https://github.com proyecto_favorita
   cd proyecto_favorita
```
2. Genere y active un entorno virtual aislado para evitar conflictos de dependencias globales:
```bash
   python3 -m venv venv
   source venv/bin/activate
```
3. Instale las librerías principales de procesamiento y conectividad (Polars, SQLAlchemy, Psycopg2):
   ```bash
   pip install --upgrade pip
   pip install polars sqlalchemy psycopg2-binary
   ```

### 8.4 Instalación y Configuración de Apache Airflow
1. Defina la ruta base de Airflow e instale la versión estable utilizando las restricciones oficiales de Python:
```bash
   export AIRFLOW_HOME=~/airflow
   AIRFLOW_VERSION=2.8.1
   PYTHON_VERSION="\$(python3 --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
   CONSTRAINT_URL="https://githubusercontent.com\({AIRFLOW_VERSION}/constraints-\){PYTHON_VERSION}.txt"
   pip install "apache-airflow==\({AIRFLOW_VERSION}" --constraint "\){CONSTRAINT_URL}"
```
2. Inicialice la base de datos interna de Airflow y cree el usuario Administrador para la interfaz Web:
```bash
   airflow db init
   
   airflow users create \
       --username admin \
       --firstname EPN \
       --lastname Tecnologos \
       --role Admin \
       --email estudiante@epn.edu.ec \
       --password admin_password
```

### 8.5 Enlace de Componentes y Despliegue del Pipeline
1. Vincule la carpeta de DAGs del repositorio con el directorio de trabajo de Airflow mediante un enlace simbólico:
```bash
   ln -s ~/proyecto_favorita/dags ~/airflow/dags
```
2. Asegúrese de que los archivos CSV del dataset estén ubicados en la ruta local configurada en los scripts (`~/proyecto_favorita/data/`).
3. Inicie el planificador (`Scheduler`) y el servidor web en segundo plano:
```bash
   airflow scheduler -D
   airflow webserver --port 8080 -D
```

### 8.6 Ejecución del Pipeline
Acceda a la interfaz web de Airflow a través del navegador (`http://IP_DE_TU_VM:8080`), active el DAG `favorita_pipeline` y ejecute un trigger manual, o bien actualice el archivo `manifest.json` mediante un commit en el repositorio para que sea detectado por el mecanismo de control.

## 9. Conclusiones y recomendaciones
### 9.1 Conclusiones
* **Eficiencia del Motor de Carga**: La implementación de **Polars** en modo *Lazy* (`pl.scan_csv`) demostró ser altamente eficiente para la infraestructura restringida de la VM. Al posponer la ejecución real de las transformaciones hasta invocar el método `.collect()`, el optimizador redujo significativamente la huella de memoria RAM, evitando desbordamientos (*OutOfMemory*) al procesar los más de 3 millones de registros de hechos.
* **Robustez y Tolerancia a Fallos**: La configuración lineal del DAG (`favorita_pipeline`) bajo la política *Fail-Fast* garantizó la integridad referencial de los datos analíticos. El acoplamiento síncrono impidió que fallas críticas en las fases previas (como errores de formato o archivos corruptos en la carga) se propagaran hacia la base de datos intermedia PostgreSQL, asegurando la consistencia del backend de Power BI.
* **Sustitución de Series Temporales**: La estrategia de imputación temporal implementada mediante ordenamiento cronológico y `forward_fill` sobre `oil.csv` resolvió de manera óptima el vacío de datos de los fines de semana. Esto permitió mantener un linaje lineal continuo de la variable macroeconómica del precio del petróleo, facilitando análisis de correlación directos sin alterar el sesgo de la serie histórica.

### 9.2 Recomendaciones
* **Indexación y Particionamiento en Almacenamiento**: Se recomienda migrar la estrategia DDL de inserción masiva en PostgreSQL de un esquema de sobrescritura total (`if_exists="replace"`) hacia una estructura predefinida que implemente **particionamiento de tablas por año/mes** basado en la columna `date`. Asimismo, la creación explícita de índices compuestos B-Tree sobre las llaves primarias (`date` y `store_nbr`) optimizará los tiempos de respuesta de las consultas DirectQuery enviadas desde el dashboard.
* **Migración a Almacenamiento Analítico (Parquet)**: Para futuras iteraciones del proyecto, se sugiere reemplazar el almacenamiento origen de archivos planos CSV locales por archivos en formato **Parquet** con compresión Snappy. Al ser un almacenamiento columnar, Polars ejecutará la optimización de *Proyección Pushdown* a nivel de sistema de archivos, leyendo únicamente los bytes de las columnas requeridas y disminuyendo los tiempos de I/O de disco en la VM.
* **Automatización del Disparador por Control de Cambios**: Se aconseja explotar la capacidad opcional de **GitHub Actions** integrada con el archivo de control `manifest.json`. Configurar un Webhook activo hacia la API de Apache Airflow permitirá automatizar por completo el despliegue continuo (CD), disparando el pipeline inmediatamente al detectar un *commit* en la rama principal, eliminando la necesidad de ejecuciones manuales en la UI del orquestador.

## 10. Equipo y Repositorio
### 10.1 Integrantes del Proyecto (Grupo de Desarrollo)
El diseño, construcción y despliegue del pipeline automatizado fue ejecutado por los siguientes miembros de la Escuela de Formación de Tecnólogos (ESFOT):

* **Desarrollador 1**: Ariel Campoverde - `estudiante1@epn.edu.ec`
* **Desarrollador 2**: Sebastian Chanchay - `sebastian.chanchay@epn.edu.ec`

### 10.2 Estructura del Repositorio de GitHub
El repositorio centralizado aloja exclusivamente la lógica de procesamiento, orquestación y control, garantizando un entorno ligero libre de binarios o archivos de datos pesados (los cuales residen en el almacenamiento local de la VM):

```text
├── dags/
│   └── favorita_pipeline.py      #
├── scripts/
│   ├── cargar_datos.py        
|   ├── eda_inicial.py
|   ├── limpiar_datos.py
│   ├── consolidar.py         
│   ├── eda_profundo.py  
│   ├── exportar_postgres.py            
│   └── config.py
├── visualizacion/
│   └── dashboard_favorita.pbix   
├── image.png        
├── manifest.json           
├── .gitignore                    
└── README.md                     
```

### 10.3 Directivas de Entrega y Acceso
* **Enlace del Repositorio**: [https://github.com](https://github.com)
* **Mecanismo de Despliegue**: El código fuente se encuentra sincronizado con la máquina virtual. Cualquier cambio confirmado (*commit*) sobre el archivo `manifest.json` habilita el linaje de auditoría del pipeline-
* **Entrega Docente**: Siguiendo las directrices del Ing. Juan Carlos Gonzalez, el enlace oficial de este repositorio junto con el archivo de documentación técnica final han sido cargados en la carpeta compartida de OneDrive institucional proporcionada para el período académico 2026-A.
