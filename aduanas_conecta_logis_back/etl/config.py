
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / os.getenv("DATA_FOLDER", "data")
DB_PATH = DATA_DIR / os.getenv("DATABASE_FILENAME", "datawarehouse.db")
REJECTED_DATA_PATH = BASE_DIR / "rejected_records.txt"
BULTOS_COLS_MAP = {
    "NUMEROIDENT": 0, "FECHAACEPT": 1, "CANTIDADBULTO": 4
}
EXPORTACIONES_COLS_MAP = {
    "FECHAACEPT": 0, "NUMEROIDENT": 1, "NRO_EXPORTADOR": 28, "PESOBRUTOTOTAL": 24,
    "FOBUNITARIO": 66, "PESOBRUTOITEM": 65, "CODIGOARANCEL": 64
}

DATA_SOURCES = {
    "exportaciones": { "files": [DATA_DIR/"exportacionesAbril2025.txt", DATA_DIR/"exportacionesMarzo2025.txt"], "cols_map": EXPORTACIONES_COLS_MAP, "separator": ";", "decimal_separator": "," },
    "bultos": { "files": [DATA_DIR/"bultosAbril2025.txt", DATA_DIR/"bultosMarzo2025.txt"], "cols_map": BULTOS_COLS_MAP, "separator": ";", "decimal_separator": "." }
}

TABLE_NAMES = { "exportaciones": "exportaciones", "bultos": "bultos_exportaciones" }