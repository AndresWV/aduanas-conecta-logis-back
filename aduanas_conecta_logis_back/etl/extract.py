
from pathlib import Path
from typing import List, Dict
import pandas as pd
from prefect import task, get_run_logger

@task(name="Extract Data from Local Files")
def extract_from_files(
    file_paths: List[Path],
    all_column_names: List[str],
    use_cols_map: Dict[str, int],
    separator: str,
    decimal_separator: str
) -> pd.DataFrame:
    """
    Lee archivos de texto locales sin encabezado, asigna nombres a las columnas,
    selecciona las columnas necesarias y las combina.
    """
    logger = get_run_logger()
    dataframes = []

    # Extraemos los nombres y posiciones de las columnas que nos interesan
    final_col_names = list(use_cols_map.keys())
    col_positions = list(use_cols_map.values())

    for file_path in file_paths:
        if not file_path.exists():
            logger.warning(f"Archivo no encontrado: {file_path}. Saltando.")
            continue
        
        logger.info(f"Leyendo archivo: {file_path.name}")
        try:
            # Leemos solo las posiciones de columna que necesitamos
            df = pd.read_csv(
                file_path,
                sep=separator,
                header=None,
                usecols=col_positions,
                decimal=decimal_separator,
                encoding='latin-1', # Codificación común para archivos de sistemas antiguos
                on_bad_lines='warn'  # Avisa de líneas malformadas pero no detiene el proceso
            )
            # Asignamos los nombres correctos a las columnas leídas
            df.columns = final_col_names
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Error al leer el archivo {file_path.name}: {e}")

    if not dataframes:
        raise ValueError("No se pudieron leer datos de los archivos de origen.")

    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Extracción completada. {len(combined_df)} filas combinadas.")
    return combined_df