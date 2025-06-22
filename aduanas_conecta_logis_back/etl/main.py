
from prefect import flow, get_run_logger
import os
from .config import DATA_SOURCES, DB_PATH, TABLE_NAMES, REJECTED_DATA_PATH
from .extract import extract_from_files
from .transform import clean_and_transform_split
from .load import load_to_duckdb, save_rejected_records
from .analyze import generate_quality_report, print_report_and_recommendations
from .modeling import create_analytical_models

@flow(name="ETL Pipeline - Aduanas a DuckDB")
def etl_parent_flow():
    """
    Flujo principal que orquesta la extracción, transformación, carga, modelado y
    análisis de los datos de exportaciones y bultos.
    """
    logger = get_run_logger()
    logger.info("Iniciando el flujo principal de la ETL...")
    
    if os.path.exists(REJECTED_DATA_PATH):
        try:
            os.remove(REJECTED_DATA_PATH)
            logger.info(f"Archivo de rechazos anterior eliminado: {REJECTED_DATA_PATH}")
        except OSError as e:
            logger.error(f"No se pudo eliminar el archivo de rechazos anterior: {e}")

    try:
        # --- PASO 1: Extracción y Transformación ---
        export_config = DATA_SOURCES["exportaciones"]
        df_exportaciones_raw = extract_from_files(
            file_paths=export_config["files"],
            cols_map=export_config["cols_map"],
            separator=export_config["separator"],
            decimal_separator=export_config["decimal_separator"]
        )
        df_exp_good, df_exp_rejected = clean_and_transform_split(df=df_exportaciones_raw)
        
        bultos_config = DATA_SOURCES["bultos"]
        df_bultos_raw = extract_from_files(
            file_paths=bultos_config["files"],
            cols_map=bultos_config["cols_map"],
            separator=bultos_config["separator"],
            decimal_separator=bultos_config["decimal_separator"]
        )
        df_bul_good, df_bul_rejected = clean_and_transform_split(df=df_bultos_raw)
        load_exp_task = load_to_duckdb(df=df_exp_good, db_path=DB_PATH, table_name=TABLE_NAMES["exportaciones"])
        save_rejected_records(df=df_exp_rejected, file_path=REJECTED_DATA_PATH, separator=export_config["separator"])
        
        load_bul_task = load_to_duckdb(df=df_bul_good, db_path=DB_PATH, table_name=TABLE_NAMES["bultos"])
        save_rejected_records(df=df_bul_rejected, file_path=REJECTED_DATA_PATH, separator=bultos_config["separator"])
        modeling_task = create_analytical_models(
            db_path=DB_PATH,
            wait_for=[load_exp_task, load_bul_task]
        )
        
        report = generate_quality_report(
            db_path=DB_PATH,
            table_name=TABLE_NAMES["exportaciones"],
            columns_to_check=list(export_config["cols_map"].keys()),
            wait_for=[modeling_task]
        )
        print_report_and_recommendations(report)
        
        logger.info("¡Flujo ETL completado exitosamente!")

    except Exception as e:
        logger.error(f"El flujo ETL falló con un error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    etl_parent_flow()