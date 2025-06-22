
import duckdb
from pathlib import Path
from prefect import task, get_run_logger
from typing import Dict, Any, List

def _build_quality_query(table_name: str, columns_to_check: List[str]) -> str:
    """
    Construye una única consulta SQL para calcular todas las métricas de calidad de datos.
    """
    selections = ["COUNT(*) AS total_rows"]
    
    for col in columns_to_check:
        # Métrica de Completitud (conteo de no nulos)
        selections.append(f"COUNT({col}) AS non_null_{col}")
        
        # Métrica de Unicidad (conteo de distintos)
        selections.append(f"COUNT(DISTINCT {col}) AS distinct_{col}")

        # Métricas de Validez (ejemplos específicos)
        if col == "FECHAACEPT":
            selections.append(f"SUM(CASE WHEN strftime({col}, '%Y') = '2025' THEN 1 ELSE 0 END) AS valid_year_{col}")
        if col == "FOBUNITARIO":
            selections.append(f"SUM(CASE WHEN {col} >= 0 THEN 1 ELSE 0 END) AS valid_positive_{col}")
            
    query = f"SELECT {', '.join(selections)} FROM {table_name};"
    return query

@task(name="Generate Data Quality Report")
def generate_quality_report(db_path: Path, table_name: str, columns_to_check: List[str]) -> Dict[str, Any]:
    """
    Se conecta a la base de datos y genera un reporte de calidad de datos
    utilizando una única y eficiente consulta SQL.
    """
    logger = get_run_logger()
    logger.info(f"Generando reporte de calidad para la tabla '{table_name}'...")
    
    report = {"table": table_name, "columns": {}}
    
    try:
        con = duckdb.connect(database=str(db_path), read_only=True)
        
        query = _build_quality_query(table_name, columns_to_check)
        logger.info(f"Ejecutando consulta de calidad:\n{query}")
        
        results = con.execute(query).fetchone()
        
        # Obtenemos los nombres de las columnas del resultado para mapear los valores
        col_names = [desc[0] for desc in con.description]
        result_dict = dict(zip(col_names, results))
        
        con.close()

        total_rows = result_dict.get("total_rows", 0)
        if total_rows == 0:
            logger.warning("La tabla está vacía. No se puede generar el reporte.")
            return report
        
        report["total_rows"] = total_rows

        for col in columns_to_check:
            non_null_count = result_dict.get(f"non_null_{col}", 0)
            distinct_count = result_dict.get(f"distinct_{col}", 0)
            
            completeness = (non_null_count / total_rows) * 100
            uniqueness = (distinct_count / total_rows) * 100
            
            validity_metrics = {}
            if f"valid_year_{col}" in result_dict:
                valid_count = result_dict[f"valid_year_{col}"]
                validity_metrics["percentage_in_2025"] = f"{(valid_count / total_rows) * 100:.2f}%"
            if f"valid_positive_{col}" in result_dict:
                valid_count = result_dict[f"valid_positive_{col}"]
                validity_metrics["percentage_positive"] = f"{(valid_count / total_rows) * 100:.2f}%"

            report["columns"][col] = {
                "completeness": f"{completeness:.2f}%",
                "uniqueness": f"{uniqueness:.2f}%",
                "validity": validity_metrics or "No checks defined"
            }
            
    except Exception as e:
        logger.error(f"Fallo al generar el reporte de calidad para '{table_name}': {e}", exc_info=True)
        
    return report

def print_report_and_recommendations(report: Dict[str, Any]):
    """Imprime el reporte de calidad de forma legible y añade recomendaciones."""
    logger = get_run_logger()
    logger.info("--- INICIO REPORTE DE CALIDAD DE DATOS ---")
    
    table = report.get('table', 'N/A')
    total_rows = report.get('total_rows', 'N/A')
    
    logger.info(f"Tabla: {table}")
    logger.info(f"Total de Filas Analizadas: {total_rows}")
    
    if not report.get("columns"):
        logger.warning("No se generaron métricas para las columnas.")
        logger.info("--- FIN REPORTE DE CALIDAD DE DATOS ---")
        return
        
    recommendations = []
    for col, metrics in report.get("columns", {}).items():
        logger.info(f"\n  Columna: {col}")
        completeness = metrics.get('completeness', '0.00%')
        uniqueness = metrics.get('uniqueness', '0.00%')
        validity = metrics.get('validity', {})

        logger.info(f"    - Completitud: {completeness}")
        logger.info(f"    - Unicidad: {uniqueness}")
        if isinstance(validity, dict) and validity:
            logger.info("    - Chequeos de Validez:")
            for check, value in validity.items():
                logger.info(f"      - {check}: {value}")

        # Lógica para generar recomendaciones
        completeness_val = float(completeness.replace('%',''))
        if completeness_val < 98.0:
            recommendations.append(f"- {col}: Completitud baja ({completeness}). Investigar el origen de valores nulos o problemas de parseo.")
        
        if col == "NUMEROIDENT" and float(uniqueness.replace('%','')) < 100.0:
            recommendations.append(f"- {col}: Se esperan valores únicos, pero la unicidad es del {uniqueness}. Revisar posibles duplicados en la fuente.")
            
    logger.info("\n--- Recomendaciones Basadas en Hallazgos ---")
    if recommendations:
        for rec in recommendations:
            logger.info(rec)
    else:
        logger.info("¡No se encontraron problemas críticos de calidad de datos! Los datos parecen consistentes.")
    logger.info("--- FIN REPORTE DE CALIDAD DE DATOS ---")