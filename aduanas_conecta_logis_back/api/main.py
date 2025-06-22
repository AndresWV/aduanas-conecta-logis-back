
from datetime import date
from typing import List, Optional
import pandas as pd
import duckdb
import numpy as np
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- IMPORTACIONES CLAVE ---
# Importamos la ruta a la DB y el flujo de la ETL desde su ÚNICA fuente de verdad en 'config.py'
from aduanas_conecta_logis_back.etl.config import DB_PATH
from aduanas_conecta_logis_back.etl.main import etl_parent_flow

# --- Configuración Inicial ---
app = FastAPI(
    title="API y Orquestador de Datos de Aduanas",
    description="Provee endpoints para consultar datos y para disparar la ejecución de la ETL.",
    version="FINAL" # ¡Versión final!
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# --- Lógica de la Base de Datos ---
def get_db_connection():
    """Establece y devuelve una conexión a la base de datos DuckDB."""
    # DB_PATH ahora se importa directamente, garantizando que la API y la ETL siempre miren al mismo lugar.
    if not DB_PATH.exists():
        raise HTTPException(
            status_code=503, 
            detail=f"Base de datos no encontrada en la ruta '{DB_PATH}'. Por favor, ejecute la ETL primero."
        )
    try:
        return duckdb.connect(database=str(DB_PATH), read_only=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al conectar con la base de datos: {e}")

# --- Modelos de Datos (Pydantic) ---
class TrendData(BaseModel):
    period: str
    average_fob: float
    change_from_previous: Optional[float] = None

class ExporterRanking(BaseModel):
    week: str
    rank: int
    nro_exportador: int
    total_fob: float

class AverageWeight(BaseModel):
    average_weight_per_bulto: Optional[float]

# --- Endpoint para Disparar la ETL ---
@app.post("/api/etl/trigger", status_code=202, tags=["ETL"])
def trigger_etl(background_tasks: BackgroundTasks):
    background_tasks.add_task(etl_parent_flow)
    return {"message": "Proceso ETL iniciado en segundo plano. Revisa los logs de la consola para ver el progreso."}

# --- Endpoints de Consulta de Datos ---
@app.get("/api/trends/fob-daily", response_model=List[TrendData], tags=["Tendencias"])
def get_daily_fob_trends(start_date: date, end_date: date):
    """Consulta la vista pre-calculada de tendencias diarias."""
    query = f"SELECT * FROM V_TENDENCIAS_DIARIAS WHERE CAST(period AS DATE) BETWEEN '{start_date}' AND '{end_date}';"
    con = get_db_connection()
    df = con.execute(query).fetchdf()
    con.close()
    df_cleaned = df.replace({np.nan: None})
    return df_cleaned.to_dict(orient="records")

@app.get("/api/rankings/exporters-weekly", response_model=List[ExporterRanking], tags=["Rankings"])
def get_weekly_exporter_rankings(start_date: date, end_date: date):
    """Consulta la vista pre-calculada de rankings semanales."""
    start_week = start_date.strftime('%Y-%W')
    end_week = end_date.strftime('%Y-%W')
    query = f"SELECT * FROM V_RANKING_SEMANAL WHERE week BETWEEN '{start_week}' AND '{end_week}';"
    con = get_db_connection()
    df = con.execute(query).fetchdf()
    con.close()
    df_cleaned = df.replace({np.nan: None})
    return df_cleaned.to_dict(orient="records")

@app.get("/api/stats/average-weight-per-bulto", response_model=AverageWeight, tags=["Estadísticas"])
def get_average_weight_per_bulto(start_date: date, end_date: date):
    """Consulta la vista pre-calculada de peso promedio por bulto."""
    query = f"SELECT AVG(average_weight_per_bulto) as average_weight_per_bulto FROM V_PESO_PROMEDIO_BULTO WHERE fecha_aceptacion BETWEEN '{start_date}' AND '{end_date}';"
    con = get_db_connection()
    result = con.execute(query).fetchdf().to_dict(orient="records")
    df_cleaned = pd.DataFrame(result).replace({np.nan: None})
    return df_cleaned.to_dict(orient='records')[0]