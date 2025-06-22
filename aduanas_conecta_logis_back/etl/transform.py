import pandas as pd
from prefect import task, get_run_logger

def _remove_outliers_iqr(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """Función auxiliar para remover outliers usando el método IQR."""
    if df[column].dtype not in ['int64', 'float64']:
        return df # No se puede calcular IQR en columnas no numéricas
    
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    initial_rows = len(df)
    df_filtered = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    rows_removed = initial_rows - len(df_filtered)
    if rows_removed > 0:
        get_run_logger().info(f"Se eliminaron {rows_removed} filas consideradas outliers en '{column}'.")
    return df_filtered

@task(name="Clean and Transform Data")
def clean_and_transform(df: pd.DataFrame, dataset_name: str) -> pd.DataFrame:
    """
    Aplica una serie de pasos de limpieza y transformación a un DataFrame.
    """
    logger = get_run_logger()
    logger.info(f"Iniciando limpieza para '{dataset_name}'. Filas iniciales: {len(df)}")
    
    df_clean = df.copy()

    # 1. Eliminar duplicados
    df_clean.drop_duplicates(inplace=True)

    # 2. Formatear fechas (DDMMYYYY)
    if "FECHAACEPT" in df_clean.columns:
        df_clean["FECHAACEPT"] = pd.to_datetime(
            df_clean["FECHAACEPT"], format='%d%m%Y', errors='coerce'
        )
        # Eliminar filas donde la conversión de fecha falló
        df_clean.dropna(subset=["FECHAACEPT"], inplace=True)
        
    # 3. Eliminar filas con valores nulos restantes
    df_clean.dropna(inplace=True)

    # 4. Remover outliers (solo para exportaciones en FOBUNITARIO)
    if dataset_name == "exportaciones" and "FOBUNITARIO" in df_clean.columns:
        df_clean = _remove_outliers_iqr(df_clean, "FOBUNITARIO")

    # 5. Enriquecer con nuevas variables
    df_clean['hora_lectura_archivo'] = pd.Timestamp.now()
    if "FECHAACEPT" in df_clean.columns:
        df_clean['año'] = df_clean['FECHAACEPT'].dt.year
        df_clean['mes'] = df_clean['FECHAACEPT'].dt.month
    df_clean['hora_procesamiento'] = pd.Timestamp.now()

    logger.info(f"Transformación completada para '{dataset_name}'. Filas finales: {len(df_clean)}")
    return df_clean