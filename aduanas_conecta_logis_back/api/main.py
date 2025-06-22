
import os
from pathlib import Path
from datetime import date
from typing import List, Optional

import duckdb
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

load_dotenv()

app = FastAPI(
    title="API de Datos de Aduanas",
    description="Provee endpoints para consultar datos de exportaciones procesados por la ETL.",
    version="1.0.0"
)

# Configurar CORS para permitir que el frontend (Next.js) se conecte
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Para desarrollo; en producción, limita esto a tu dominio del frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Configuración de la Base de Datos ---
BASE_DIR_STR = os.getenv("PROJECT_BASE_DIR")
if not BASE_DIR_STR:
    raise ValueError("PROJECT_BASE_DIR no está definido en el archivo .env")

DB_PATH = Path(BASE_DIR_STR) / os.getenv("DATA_FOLDER", "data") / os.getenv("DATABASE_FILENAME", "datawarehouse.db")

def get_db_connection():
    """Establece y devuelve una conexión a la base de datos DuckDB."""
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503, 
            detail=f"Base de datos no encontrada en {DB_PATH}. Por favor, ejecute la ETL primero."
        )
    try:
        # Conectar en modo de solo lectura para seguridad
        return duckdb.connect(database=str(DB_PATH), read_only=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al conectar con la base de datos: {e}")

# --- Modelos de Datos (Pydantic) ---
# Definen la estructura de las respuestas de la API para validación y documentación automática

class TrendData(BaseModel):
    period: str = Field(..., description="El día o la semana del dato (formato YYYY-MM-DD o YYYY-WW).")
    average_fob: float = Field(..., description="Valor FOB unitario promedio para el período.")
    change_from_previous: Optional[float] = Field(None, description="Variación porcentual respecto al período anterior.")

class ExporterRanking(BaseModel):
    week: str = Field(..., description="Semana del ranking (formato YYYY-WW).")
    rank: int = Field(..., description="Posición en el ranking semanal.")
    nro_exportador: str = Field(..., description="Identificador del exportador.")
    total_fob: float = Field(..., description="Valor FOB total para esa semana.")

class AverageWeight(BaseModel):
    average_weight_per_bulto: Optional[float] = Field(description="Peso bruto promedio por bulto en el rango de fechas.")

# --- Endpoints de la API ---

@app.get("/api/trends/fob-daily", response_model=List[TrendData], tags=["Tendencias"])
def get_daily_fob_trends(
    start_date: date = Query(..., description="Fecha de inicio (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Fecha de fin (YYYY-MM-DD)")
):
    """
    Obtiene las tendencias diarias del valor FOB unitario promedio,
    incluyendo la variación porcentual diaria.
    """
    query = f"""
        WITH daily_avg AS (
            SELECT
                CAST(FECHAACEPT AS DATE) AS period_date,
                AVG(FOBUNITARIO) AS average_fob
            FROM exportaciones
            WHERE CAST(FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY period_date
        ),
        daily_trends AS (
            SELECT
                strftime(period_date, '%Y-%m-%d') AS period,
                average_fob,
                LAG(average_fob, 1, 0) OVER (ORDER BY period_date) AS prev_day_avg
            FROM daily_avg
        )
        SELECT
            period,
            average_fob,
            CASE
                WHEN prev_day_avg > 0 THEN ((average_fob - prev_day_avg) / prev_day_avg) * 100
                ELSE NULL
            END AS change_from_previous
        FROM daily_trends
        ORDER BY period;
    """
    con = get_db_connection()
    result = con.execute(query).fetchdf().to_dict(orient="records")
    con.close()
    return result

@app.get("/api/rankings/exporters-weekly", response_model=List[ExporterRanking], tags=["Rankings"])
def get_weekly_exporter_rankings(
    start_date: date = Query(..., description="Fecha de inicio (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Fecha de fin (YYYY-MM-DD)")
):
    """
    Genera un ranking semanal de exportadores basado en el valor FOB total.
    """
    query = f"""
        WITH weekly_fob AS (
            SELECT
                strftime(FECHAACEPT, '%Y-%W') AS week,
                CAST(NRO_EXPORTADOR AS VARCHAR) AS nro_exportador,
                SUM(FOBUNITARIO) AS total_fob
            FROM exportaciones
            WHERE CAST(FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY week, nro_exportador
        )
        SELECT
            week,
            RANK() OVER (PARTITION BY week ORDER BY total_fob DESC) AS rank,
            nro_exportador,
            total_fob
        FROM weekly_fob
        ORDER BY week, rank;
    """
    con = get_db_connection()
    result = con.execute(query).fetchdf().to_dict(orient="records")
    con.close()
    return result

@app.get("/api/stats/average-weight-per-bulto", response_model=AverageWeight, tags=["Estadísticas"])
def get_average_weight_per_bulto(
    start_date: date = Query(..., description="Fecha de inicio (YYYY-MM-DD)"),
    end_date: date = Query(..., description="Fecha de fin (YYYY-MM-DD)")
):
    """
    Calcula el peso promedio por bulto para los envíos en un rango de fechas.
    """
    query = f"""
        SELECT
            SUM(exp.PESOBRUTOITEM) / NULLIF(SUM(bul.CANTIDADBULTO), 0) AS average_weight_per_bulto
        FROM exportaciones AS exp
        JOIN bultos_exportaciones AS bul ON exp.NUMEROIDENT = bul.NUMEROIDENT
        WHERE CAST(exp.FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}';
    """
    con = get_db_connection()
    result = con.execute(query).fetchdf().to_dict(orient="records")
    con.close()
    return result[0] if result else {"average_weight_per_bulto": None}