# aduanas_conecta_logis_back/etl/main.py (VERSIÓN FINAL)

from prefect import flow, get_run_logger
from .config import DATA_SOURCES, DB_PATH, TABLE_NAMES
from .extract import extract_from_files
from .transform import clean_and_transform
from .load import load_to_duckdb
from .analyze import generate_quality_report, print_report_and_recommendations

@flow(name="ETL Pipeline - Aduanas a DuckDB")
def etl_parent_flow():
    logger = get_run_logger()
    logger.info("Iniciando el flujo principal de la ETL...")
    
    try:
        # --- Proceso para Exportaciones ---
        export_config = DATA_SOURCES["exportaciones"]
        df_exportaciones_raw = extract_from_files(
            file_paths=export_config["files"],
            cols_map=export_config["cols_map"],
            separator=export_config["separator"],
            decimal_separator=export_config["decimal_separator"]
        )
        df_exportaciones_clean = clean_and_transform(
            df=df_exportaciones_raw, dataset_name="exportaciones"
        )
        load_task_export = load_to_duckdb(
            df=df_exportaciones_clean,
            db_path=DB_PATH,
            table_name=TABLE_NAMES["exportaciones"]
        )

        # --- Proceso para Bultos ---
        bultos_config = DATA_SOURCES["bultos"]
        df_bultos_raw = extract_from_files(
            file_paths=bultos_config["files"],
            cols_map=bultos_config["cols_map"],
            separator=bultos_config["separator"],
            decimal_separator=bultos_config["decimal_separator"]
        )
        df_bultos_clean = clean_and_transform(
            df=df_bultos_raw, dataset_name="bultos"
        )
        load_to_duckdb(
            df=df_bultos_clean, db_path=DB_PATH, table_name=TABLE_NAMES["bultos"]
        )

        # --- Paso Final: Análisis de Calidad ---
        logger.info("Cargas completadas. Iniciando análisis de calidad.")
        export_report = generate_quality_report(
            db_path=DB_PATH,
            table_name=TABLE_NAMES["exportaciones"],
            columns_to_check=list(export_config["cols_map"].keys()),
            wait_for=[load_task_export]
        )
        print_report_and_recommendations(export_report)
        
        logger.info("¡Flujo ETL completado exitosamente!")

    except Exception as e:
        logger.error(f"El flujo ETL falló con un error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    etl_parent_flow()