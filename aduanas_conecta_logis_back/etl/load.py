
import duckdb
import pandas as pd
from pathlib import Path
from prefect import task, get_run_logger

@task(name="Load Good Data to DuckDB")
def load_to_duckdb(df: pd.DataFrame, db_path: Path, table_name: str):
    """Carga un DataFrame de datos válidos en una tabla de DuckDB."""
    logger = get_run_logger()
    if df.empty:
        logger.warning(f"No hay datos válidos para cargar en la tabla '{table_name}'. Saltando.")
        return
        
    db_path.parent.mkdir(parents=True, exist_ok=True)
    logger.info(f"Cargando {len(df)} filas en la tabla '{table_name}' en '{db_path}'...")
    try:
        con = duckdb.connect(database=str(db_path), read_only=False)
        con.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        con.close()
        logger.info(f"Carga a la tabla '{table_name}' completada.")
    except Exception as e:
        logger.error(f"Error al cargar datos en DuckDB: {e}")
        raise

@task(name="Save Rejected Data to TXT")
def save_rejected_records(df: pd.DataFrame, file_path: Path, separator: str = ';'):
    """Guarda los registros rechazados en un archivo de texto."""
    logger = get_run_logger()
    if df.empty:
        logger.info("No se encontraron registros para guardar en el archivo de rechazos.")
        return

    logger.info(f"Guardando {len(df)} filas rechazadas en el archivo: {file_path}")
    try:
        # Asegurarse de que el directorio de salida exista
        file_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(file_path, sep=separator, index=False, header=False, mode='a') # mode='a' para añadir (append)
        logger.info("Archivo de rechazos guardado exitosamente.")
    except Exception as e:
        logger.error(f"Error al guardar el archivo de rechazos: {e}")
        raise