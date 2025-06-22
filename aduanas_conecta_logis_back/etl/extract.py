
from pathlib import Path
from typing import List, Dict
import pandas as pd
from prefect import task, get_run_logger

@task(name="Extract Data from Local Files")
def extract_from_files(
    file_paths: List[Path],
    cols_map: Dict[str, int],
    separator: str,
    decimal_separator: str
) -> pd.DataFrame:
    logger = get_run_logger()
    dataframes = []

    final_col_names = list(cols_map.keys())
    col_positions = list(cols_map.values())

    for file_path in file_paths:
        if not file_path.exists():
            logger.warning(f"Archivo no encontrado: {file_path}. Saltando.")
            continue
        try:
            df = pd.read_csv(
                file_path,
                sep=separator,
                header=None,
                usecols=col_positions,
                names=final_col_names,
                decimal=decimal_separator,
                encoding='latin-1',
                on_bad_lines='warn',
                dtype=str 
            )
            dataframes.append(df)
        except Exception as e:
            logger.error(f"Error al leer el archivo {file_path.name}: {e}", exc_info=True)

    if not dataframes:
        raise ValueError("No se pudieron leer datos de los archivos de origen.")

    combined_df = pd.concat(dataframes, ignore_index=True)
    logger.info(f"Extracción completada. {len(combined_df)} filas combinadas.")
    return combined_df