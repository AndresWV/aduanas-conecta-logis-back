
import duckdb
from pathlib import Path
from prefect import task, get_run_logger

@task(name="Create Analytical Models (Table and Views)")
def create_analytical_models(db_path: Path):
    """
    Crea la tabla intermedia 'datos_idty' y las vistas analíticas,
    utilizando los tipos de datos correctos para los identificadores grandes.
    """
    logger = get_run_logger()
    logger.info("Iniciando la creación de modelos de datos analíticos (tabla y vistas)...")
    
    try:
        con = duckdb.connect(database=str(db_path), read_only=False)

        # --- 1. Crear la tabla intermedia 'datos_idty' ---
        create_table_sql = """
        CREATE OR REPLACE TABLE datos_idty AS
        SELECT
            FECHAACEPT,
            NUMEROIDENT,
            CAST(NRO_EXPORTADOR AS BIGINT) AS NRO_EXPORTADOR,
            CAST(CODIGOARANCEL AS BIGINT) AS CODIGOARANCEL,
            CAST(FOBUNITARIO AS DOUBLE) AS FOBUNITARIO,
            CAST(PESOBRUTOITEM AS DOUBLE) AS PESOBRUTOITEM
        FROM exportaciones;
        """
        con.execute(create_table_sql)
        logger.info("Tabla 'datos_idty' creada exitosamente.")

        # --- 2. Crear las Vistas (Views) Analíticas ---
        # Vista para tendencias diarias de FOB
        create_view_trends_sql = """
        CREATE OR REPLACE VIEW V_TENDENCIAS_DIARIAS AS
        WITH daily_avg AS (
            SELECT CAST(FECHAACEPT AS DATE) AS period_date, AVG(FOBUNITARIO) AS average_fob
            FROM datos_idty GROUP BY period_date
        ),
        daily_trends AS (
            SELECT strftime(period_date, '%Y-%m-%d') AS period, average_fob,
                   LAG(average_fob, 1) OVER (ORDER BY period_date) AS prev_day_avg
            FROM daily_avg
        )
        SELECT period, average_fob,
               CASE WHEN prev_day_avg > 0 THEN ((average_fob - prev_day_avg) / prev_day_avg) * 100 ELSE NULL END AS change_from_previous
        FROM daily_trends;
        """
        con.execute(create_view_trends_sql)
        logger.info("Vista 'V_TENDENCIAS_DIARIAS' creada exitosamente.")
        
        # Vista para el ranking semanal de exportadores
        create_view_ranking_sql = """
        CREATE OR REPLACE VIEW V_RANKING_SEMANAL AS
        WITH weekly_fob AS (
            SELECT strftime(FECHAACEPT, '%Y-%W') AS week, NRO_EXPORTADOR, SUM(FOBUNITARIO) AS total_fob
            FROM datos_idty GROUP BY week, NRO_EXPORTADOR
        )
        SELECT week, RANK() OVER (PARTITION BY week ORDER BY total_fob DESC) AS rank, NRO_EXPORTADOR, total_fob
        FROM weekly_fob;
        """
        con.execute(create_view_ranking_sql)
        logger.info("Vista 'V_RANKING_SEMANAL' creada exitosamente.")

        # Vista para el peso promedio por bulto
        create_view_avg_weight_sql = """
        CREATE OR REPLACE VIEW V_PESO_PROMEDIO_BULTO AS
        SELECT
            SUM(d.PESOBRUTOITEM) / NULLIF(SUM(b.CANTIDADBULTO), 0) AS average_weight_per_bulto,
            CAST(d.FECHAACEPT AS DATE) AS fecha_aceptacion
        FROM datos_idty AS d
        JOIN bultos_exportaciones AS b ON d.NUMEROIDENT = b.NUMEROIDENT
        GROUP BY fecha_aceptacion;
        """
        con.execute(create_view_avg_weight_sql)
        logger.info("Vista 'V_PESO_PROMEDIO_BULTO' creada exitosamente.")

        con.close()
        logger.info("Todos los modelos de datos analíticos han sido creados.")

    except Exception as e:
        logger.error(f"Error creando los modelos de datos analíticos: {e}", exc_info=True)
        raise