# aduanas_conecta_logis_back/etl/transform.py (VERSIÓN DE CURACIÓN DEFINITIVA)

import pandas as pd
from prefect import task, get_run_logger

@task(name="Clean and Transform Data")
def clean_and_transform(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Aplica una curación de datos robusta: valida el ID, repara el resto y enriquece.
    """
    logger = get_run_logger()
    logger.info(f"Iniciando curación de datos para '{dataset_name}'. Filas iniciales: {len(df)}")
    
    df_clean = df.copy()

    # 1. Limpieza Básica
    df_clean.drop_duplicates(inplace=True)
    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].str.strip()

    # 2. Validación de la Columna Crítica: NUMEROIDENT
    if "NUMEROIDENT" in df_clean.columns:
        initial_rows = len(df_clean)
        df_clean["NUMEROIDENT"] = pd.to_numeric(df_clean["NUMEROIDENT"], errors='coerce')
        df_clean.dropna(subset=["NUMEROIDENT"], inplace=True)
        df_clean["NUMEROIDENT"] = df_clean["NUMEROIDENT"].astype(int)
        rows_removed = initial_rows - len(df_clean)
        if rows_removed > 0:
            logger.info(f"Se eliminaron {rows_removed} filas por tener un NUMEROIDENT inválido.")

    # 3. Reparación y Conversión Tolerante del Resto de Columnas
    if "FECHAACEPT" in df_clean.columns:
        df_clean["FECHAACEPT"] = df_clean["FECHAACEPT"].str.zfill(8)
        df_clean["FECHAACEPT"] = pd.to_datetime(df_clean["FECHAACEPT"], format='%d%m%Y', errors='coerce')
        # Si la fecha es mala, la llenamos con una fecha por defecto para no perder la fila
        if df_clean["FECHAACEPT"].isnull().any():
            default_date = pd.to_datetime('1900-01-01')
            logger.warning(f"Se encontraron fechas inválidas. Se reemplazarán con {default_date.date()}.")
            df_clean["FECHAACEPT"].fillna(default_date, inplace=True)

    numeric_cols = [
        'FOBUNITARIO', 'PESOBRUTOTOTAL', 'PESOBRUTOITEM', 'CANTIDADBULTO', 
        'NRO_EXPORTADOR', 'CODIGOARANCEL'
    ]
    for col in numeric_cols:
        if col in df_clean.columns:
            df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
            df_clean[col].fillna(0, inplace=True) # Llenamos con 0 cualquier error de conversión
            # Convertimos a entero si no tiene decimales
            if df_clean[col].dtype == 'float64' and (df_clean[col] % 1 == 0).all():
                df_clean[col] = df_clean[col].astype(int)

    # 4. Enriquecimiento de Datos
    df_clean['hora_lectura_archivo'] = pd.Timestamp.now()
    if "FECHAACEPT" in df_clean.columns:
        df_clean['año'] = df_clean['FECHAACEPT'].dt.year
        df_clean['mes'] = df_clean['FECHAACEPT'].dt.month
    df_clean['hora_procesamiento'] = pd.Timestamp.now()

    logger.info(f"Curación y transformación completadas. Filas finales: {len(df_clean)}")
    return df_clean