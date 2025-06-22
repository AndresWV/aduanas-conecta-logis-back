# Pipeline y API de Datos de Exportaciones

**Autor:** Andrés Wallberg - andres.wv99@gmail.com 

## Descripción General

Este proyecto es una solución integral de **Ingeniería de Datos** diseñada para cumplir con los requisitos de una prueba técnica para un rol de **DataOps**. El sistema implementa un pipeline de datos robusto y orquestado que extrae, transforma y carga (ETL) registros de exportación del Servicio Nacional de Aduanas de Chile.

La solución está construida con herramientas modernas y profesionales, incluyendo:
* **Python** como lenguaje principal.
* **Prefect** para la orquestación del pipeline, permitiendo una ejecución de tareas clara, dependiente y monitorizable.
* **Pandas** para la manipulación y **curación avanzada de datos**, manejando inconsistencias, valores nulos y errores de formato.
* **DuckDB** como base de datos analítica en modo `in-process`, ideal para un rendimiento rápido en consultas complejas.
* **FastAPI** para servir los datos procesados a través de una API RESTful, rápida y con documentación automática.
* **Poetry** para una gestión de dependencias y entornos de desarrollo limpia y reproducible.

Adicionalmente, el pipeline implementa una capa de **modelamiento de datos**, creando vistas analíticas sobre los datos limpios para simplificar y optimizar las consultas de negocio.

## Características Principales

* **Orquestación Moderna:** El flujo ETL está definido y orquestado con Prefect, facilitando su ejecución y monitoreo.
* **Curación de Datos Robusta:** El pipeline no solo transforma los datos, sino que los "cura":
    * Valida los registros basándose en columnas críticas (`NUMEROIDENT`).
    * Maneja una gran variedad de inconsistencias en los datos de origen (fechas, números, texto).
    * **Segrega los datos inválidos** a un archivo `rejected_records.txt` para su posterior análisis, asegurando que solo los datos de alta calidad lleguen al Data Warehouse.
* **Capa de Modelamiento (Vistas Analíticas):** Sobre las tablas base, se crea una tabla intermedia (`datos_idty`) y un conjunto de **Vistas SQL** que contienen la lógica de negocio pre-calculada. Esto desacopla la lógica de la API y optimiza las consultas.
* **API de Alto Rendimiento:** La API RESTful construida con FastAPI sirve los datos desde las vistas analíticas, proveyendo respuestas rápidas y eficientes a preguntas de negocio complejas.
* **Operación vía API:** Todo el sistema, incluyendo la ejecución de la ETL, se puede operar a través de la API, lo que permite una fácil integración con otros sistemas o paneles de administración.
* **Entorno Reproducible:** El uso de Poetry y un archivo `pyproject.toml` garantiza que cualquier desarrollador pueda replicar el entorno y las dependencias de forma exacta y sin conflictos.

## Estructura del Proyecto

El código está organizado por responsabilidades (API, ETL) dentro de un paquete de Python estándar para mantener el orden y la escalabilidad.

```
aduanas-conecta-logis-back/
├── aduanas_conecta_logis_back/
│   ├── __init__.py
│   ├── api/
│   │   ├── __init__.py
│   │   └── main.py              # Lógica de la API FastAPI
│   └── etl/
│       ├── __init__.py
│       ├── analyze.py           # Tarea de análisis de calidad de datos
│       ├── config.py            # Módulo de configuración central
│       ├── extract.py           # Tarea de extracción de datos
│       ├── load.py              # Tareas de carga (datos buenos y rechazados)
│       ├── main.py              # Flujo principal de Prefect (el orquestador)
│       ├── modeling.py          # Tarea de modelamiento (creación de vistas)
│       └── transform.py         # Tarea de curación y transformación
│
├── data/
│   ├── bultosAbril2025.txt
│   ├── bultosMarzo2025.txt
│   ├── exportacionesAbril2025.txt
│   ├── exportacionesMarzo2025.txt
│   └── datawarehouse.db         # Base de datos generada por la ETL
│
├── rejected_records.txt         # Archivo de salida con datos inválidos
├── .env                         # Archivo de configuración de entorno
├── pyproject.toml               # Dependencias del proyecto para Poetry
└── README.md
```

## Configuración del Entorno

### Requisitos Previos
* Python 3.9+
* Poetry instalado en tu sistema.

### Pasos de Instalación
1.  Clona este repositorio en tu máquina local.
2.  Navega hasta el directorio raíz del proyecto (`aduanas-conecta-logis-back`).
3.  **Configura tu entorno local:**
    * Crea un archivo llamado `.env` en la raíz del proyecto.
    * Añade el siguiente contenido al archivo `.env`. Esto permite que el proyecto sea flexible sin hardcodear nombres de archivos.
        ```ini
        DATA_FOLDER=data
        DATABASE_FILENAME=datawarehouse.db
        ```
4.  **Instala las dependencias:**
    Ejecuta el siguiente comando. Poetry creará un entorno virtual aislado y descargará todas las librerías necesarias.
    ```bash
    poetry install
    ```

## Flujo de Operación

Este sistema está diseñado para ser operado a través de la API, que actúa como el panel de control central.

### Paso 1: Iniciar el Servidor de la API
Este comando levanta el servidor web que expone todos los endpoints y espera órdenes. Ejecútalo desde la raíz del proyecto.
```bash
poetry run uvicorn aduanas_conecta_logis_back.api.main:app --reload
```
El servidor estará disponible en **`http://127.0.0.1:8000`**.

### Paso 2: Ejecutar el Pipeline ETL
1.  Abre tu navegador y ve a la documentación interactiva de la API: **[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)**.
2.  Busca la sección **ETL** (de color azul) y expande el endpoint `POST /api/etl/trigger`.
3.  Haz clic en `Try it out` y luego en el botón azul `Execute`.

La API te responderá inmediatamente, pero el proceso ETL completo comenzará a ejecutarse en la terminal donde iniciaste el servidor. Podrás ver los logs de Prefect en tiempo real.

### Paso 3: Verificar y Consumir los Resultados
Una vez que el flujo termine con el mensaje `¡Flujo ETL completado exitosamente!` en la terminal:

1.  **Verifica los artefactos:**
    * El archivo `data/datawarehouse.db` habrá sido creado o actualizado.
    * El archivo `rejected_records.txt` contendrá las filas que no pasaron la validación.
2.  **Consulta los datos a través de la API:**
    * En la misma página de la documentación (`/docs`), utiliza los endpoints `GET` (de color verde) para hacer preguntas de negocio.
    * Por ejemplo, expande `GET /api/trends/fob-daily`, introduce un rango de fechas como `2025-04-01` a `2025-04-30` y presiona `Execute` para ver los resultados.