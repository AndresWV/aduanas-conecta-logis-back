# Backend de Datos - Aduanas Conecta Logística

**Nombre del Proyecto:** `aduanas-conecta-logis-back`
**Autor:** [Tu Nombre]

## Descripción
Este proyecto corresponde al backend y al pipeline de datos para la prueba técnica de Data Engineering. Implementa un pipeline que extrae registros de exportación del portal de datos de Chile, los procesa, analiza su calidad y los carga en una base de datos DuckDB.

Adicionalmente, expone los resultados a través de una API REST construida con FastAPI, lista para ser consumida por una interfaz web.

Este proyecto utiliza **Poetry** para la gestión de dependencias y entornos virtuales.

## Estructura del Proyecto
```
aduanas-conecta-logis-back/
├── data/
│   └── datawarehouse.db  # Generado tras la ejecución del pipeline
├── main.py               # Backend con FastAPI (Parte 1.3)
├── pipeline.py           # Pipeline con Prefect (Parte 2)
├── queries.sql           # Consultas SQL de referencia (Parte 1.2)
├── pyproject.toml        # Archivo de configuración y dependencias de Poetry
├── poetry.lock           # Archivo de dependencias exactas
└── README.md
```

## Requisitos Previos
- Python 3.9 o superior
- [Poetry](https://python-poetry.org/docs/#installation) instalado en tu sistema.

## Instalación
1.  Clona este repositorio o descomprime los archivos en una carpeta.
2.  Navega hasta el directorio raíz del proyecto (`aduanas-conecta-logis-back`).
3.  Instala todas las dependencias usando Poetry. Este comando creará un entorno virtual y descargará las librerías especificadas en `pyproject.toml`.
    ```bash
    poetry install
    ```

## Ejecución

Todos los comandos deben ser ejecutados con `poetry run` desde la raíz del proyecto para asegurar que se utiliza el entorno virtual correcto.

### 1. Ejecutar el Pipeline de Datos (Prefect)
Este comando ejecuta el pipeline ETL completo: extrae, transforma, valida la calidad y carga los datos en el archivo `data/datawarehouse.db`.
```bash
poetry run python pipeline.py
```

### 2. Iniciar el Servidor del Backend
Este comando inicia la API REST que sirve los datos desde la base de datos.
```bash
poetry run uvicorn main:app --reload
```
La API estará disponible en `http://127.0.0.1:8000`. Puedes explorar sus endpoints y probarlos interactivamente a través de la documentación autogenerada por Swagger UI en:

[http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)