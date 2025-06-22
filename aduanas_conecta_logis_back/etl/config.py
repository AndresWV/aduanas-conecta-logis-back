from pathlib import Path
import os
from dotenv import load_dotenv


load_dotenv()

base_dir_str = os.getenv("PROJECT_BASE_DIR")
if not base_dir_str:
    raise ValueError("La variable de entorno PROJECT_BASE_DIR no está definida en el archivo .env")

BASE_DIR = Path(base_dir_str)
DATA_DIR = BASE_DIR / os.getenv("DATA_FOLDER", "data") # "data" es un valor por defecto
DB_PATH = DATA_DIR / os.getenv("DATABASE_FILENAME", "datawarehouse.db")


# Para 'bultos': La relación es directa y clara.
BULTOS_COLS_MAP = {
    # Nombre Requerido: Posición en el archivo (empezando en 0)
    "NUMEROIDENT": 0,
    "FECHAACEPT": 1,
    "CANTIDADBULTO": 4
}

# Para 'exportaciones': El archivo es complejo. Hacemos suposiciones razonables
# para una prueba técnica, enfocándonos en la primera parte de cada línea,
# antes del separador '~'. Esto nos da datos más consistentes.
EXPORTACIONES_COLS_MAP = {
    # Nombre Requerido: Posición en el archivo
    "FECHAACEPT": 0,
    "NUMEROIDENT": 1,
    "NRO_EXPORTADOR": 28,  # Posición del RUT/ID del exportador. Es un identificador estable.
    "PESOBRUTOTOTAL": 24,  # Peso bruto total declarado para el envío.
    "FOBUNITARIO": 23,     # Usamos FOBTOTAL como proxy para FOBUNITARIO. Ver justificación.
    "PESOBRUTOITEM": 24,   # Usamos PESOBRUTOTOTAL como proxy. Ver justificación.
    "CODIGOARANCEL": 64    # Posición estimada del código arancelario después del '~'. Es la más variable.
}

# --- Configuración de Fuentes de Datos para la ETL ---
DATA_SOURCES = {
    "exportaciones": {
        "files": [
            DATA_DIR / "exportacionesAbril2025.txt",
            DATA_DIR / "exportacionesMarzo2025.txt"
        ],
        "cols_map": EXPORTACIONES_COLS_MAP,
        "separator": ";",
        "decimal_separator": "," # Importante para valores como '22155,04'
    },
    "bultos": {
        "files": [
            DATA_DIR / "bultosAbril2025.txt",
            DATA_DIR / "bultosMarzo2025.txt"
        ],
        "cols_map": BULTOS_COLS_MAP,
        "separator": ";",
        "decimal_separator": "." # No hay decimales, pero se mantiene por consistencia
    }
}

# --- Configuración de la Base de Datos Destino ---
TABLE_NAMES = {
    "exportaciones": "exportaciones",
    "bultos": "bultos_exportaciones"
}