import duckdb
import pandas as pd
from pathlib import Path
from prefect import task, get_run_logger

@task(name="Load Data to DuckDB")
def load_to_duckdb(df: pd.DataFrame, db_path: Path, table_name: str):
    """
    Carga un DataFrame en una tabla de DuckDB, reemplazándola si ya existe.
    """
    logger = get_run_logger()
    
    # Asegurarse de que el directorio de la base de datos existe
    db_path.parent.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Cargando {len(df)} filas en la tabla '{table_name}' en '{db_path}'...")
    try:
        con = duckdb.connect(database=str(db_path), read_only=False)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        con.close()
        logger.info(f"Carga a la tabla '{table_name}' completada exitosamente.")
    except Exception as e:
        logger.error(f"Error al cargar datos en DuckDB: {e}")
        raise