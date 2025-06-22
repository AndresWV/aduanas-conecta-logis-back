import pandas as pd
from prefect import task, get_run_logger
from typing import Tuple

@task(name="Clean, Validate, and Split Data")
def clean_and_transform_split(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Aplica curación de datos y luego divide el DataFrame en dos:
    uno con los registros válidos y otro con los registros rechazados.
    """
    logger = get_run_logger()
    logger.info(f"Iniciando curación y validación. Filas iniciales: {len(df)}")
    
    # 1. Limpieza Básica
    # Eliminamos duplicados primero sobre el dataframe original
    df.drop_duplicates(inplace=True)
    # Limpiar espacios en blanco en todas las columnas
    df_clean = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # 2. Conversión de Tipos con Manejo de Errores
    # Guardamos los resultados de la conversión en nuevas columnas temporales
    if "FECHAACEPT" in df_clean.columns:
        df_clean["FECHAACEPT_norm"] = df_clean["FECHAACEPT"].str.zfill(8)
        df_clean["FECHAACEPT_clean"] = pd.to_datetime(df_clean["FECHAACEPT_norm"], format='%d%m%Y', errors='coerce')

    if "NUMEROIDENT" in df_clean.columns:
        df_clean["NUMEROIDENT_clean"] = pd.to_numeric(df_clean["NUMEROIDENT"], errors='coerce')

    # 3. Identificar las Filas Malas
    # Una fila es mala si su ID o su Fecha no se pudieron convertir (son Nulos)
    bad_rows_mask = df_clean["NUMEROIDENT_clean"].isnull() | df_clean["FECHAACEPT_clean"].isnull()
    
    # 4. Separar los dataframes usando la máscara.
    # Ahora los índices coinciden perfectamente.
    df_rejected = df[bad_rows_mask]
    df_good = df_clean[~bad_rows_mask].copy()

    logger.info(f"{len(df_rejected)} filas fueron rechazadas por datos críticos inválidos.")
    
    # 5. Limpieza final solo para los datos buenos
    df_good["NUMEROIDENT"] = df_good["NUMEROIDENT_clean"].astype(int)
    df_good["FECHAACEPT"] = df_good["FECHAACEPT_clean"]

    numeric_cols = ['FOBUNITARIO', 'PESOBRUTOTOTAL', 'PESOBRUTOITEM', 'CANTIDADBULTO', 'NRO_EXPORTADOR', 'CODIGOARANCEL']
    for col in numeric_cols:
        if col in df_good.columns:
            df_good[col] = pd.to_numeric(df_good[col], errors='coerce').fillna(0)
            if df_good[col].dtype == 'float64' and (df_good[col] % 1 == 0).all():
                df_good[col] = df_good[col].astype(int)

    # 6. Enriquecimiento y selección final de columnas
    df_good['hora_lectura_archivo'] = pd.Timestamp.now()
    df_good['año'] = df_good['FECHAACEPT'].dt.year
    df_good['mes'] = df_good['FECHAACEPT'].dt.month
    df_good['hora_procesamiento'] = pd.Timestamp.now()
    
    # Nos quedamos solo con las columnas originales y las de enriquecimiento
    final_good_columns = list(df.columns) + ['hora_lectura_archivo', 'año', 'mes', 'hora_procesamiento']
    df_good = df_good[final_good_columns]

    logger.info(f"Curación completada. Filas válidas: {len(df_good)}. Filas rechazadas: {len(df_rejected)}.")
    
    return df_good, df_rejected