# aduanas_conecta_logis_back/etl/config.py (VERSIÓN FINAL CON METADATA)

import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- Rutas Dinámicas y Relativas ---
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / os.getenv("DATA_FOLDER", "data")
DB_PATH = DATA_DIR / os.getenv("DATABASE_FILENAME", "datawarehouse.db")

# --- Mapeo de Columnas Requeridas a Posiciones Reales (Según Metadata) ---

BULTOS_COLS_MAP = {
    "NUMEROIDENT": 0,    # Corresponde a 'NUMERO_IDENTIFICACION_DUS' en titulos.csv
    "FECHAACEPT": 1,     # Corresponde a 'FECHA_ACEPTACION_DUS' en titulos.csv
    "CANTIDADBULTO": 4   # Corresponde a 'CANTIDAD_BULTOS' en titulos.csv
}

EXPORTACIONES_COLS_MAP = {
    "FECHAACEPT": 0,       # Posición 0: FECHA_ACEPTACION_DUS
    "NUMEROIDENT": 1,      # Posición 1: NUMERO_IDENTIFICACION_DUS
    "NRO_EXPORTADOR": 28,  # Posición 28: RUT_EXPORTADOR
    "CODIGOARANCEL": 64,   # Posición 64: CODIGO_ARANCEL_ITEM
    "FOBUNITARIO": 66,     # Posición 66: VALOR_FOB_ITEM
    "PESOBRUTOITEM": 65,   # Posición 65: PESO_BRUTO_ITEM
    "PESOBRUTOTOTAL": 24   # Posición 24: PESO_BRUTO_TOTAL_DUS
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
        "decimal_separator": ","
    },
    "bultos": {
        "files": [
            DATA_DIR / "bultosAbril2025.txt",
            DATA_DIR / "bultosMarzo2025.txt"
        ],
        "cols_map": BULTOS_COLS_MAP,
        "separator": ";",
        "decimal_separator": "."
    }
}

# --- Configuración de la Base de Datos Destino ---
TABLE_NAMES = {
    "exportaciones": "exportaciones",
    "bultos": "bultos_exportaciones"
}