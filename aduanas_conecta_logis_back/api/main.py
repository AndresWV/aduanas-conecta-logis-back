
import os
from pathlib import Path
from datetime import date
from typing import List, Optional

import duckdb
import numpy as np 
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Importamos la ruta a la DB y el flujo de la ETL
from aduanas_conecta_logis_back.etl.config import DB_PATH
from aduanas_conecta_logis_back.etl.main import etl_parent_flow

# --- Configuración Inicial y Modelos (sin cambios) ---
app = FastAPI(title="API y Orquestador de Datos de Aduanas", version="1.3.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

def get_db_connection():
    if not DB_PATH.exists():
        raise HTTPException(status_code=503, detail=f"Base de datos no encontrada. Ejecute la ETL primero.")
    try:
        return duckdb.connect(database=str(DB_PATH), read_only=True)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al conectar con la base de datos: {e}")

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

# --- Endpoints ---

@app.post("/api/etl/trigger", status_code=202, tags=["ETL"])
def trigger_etl(background_tasks: BackgroundTasks):
    background_tasks.add_task(etl_parent_flow)
    return {"message": "Proceso ETL iniciado en segundo plano."}

@app.get("/api/trends/fob-daily", response_model=List[TrendData], tags=["Tendencias"])
def get_daily_fob_trends(start_date: date, end_date: date):
    query = f"""
        WITH daily_avg AS (
            SELECT CAST(FECHAACEPT AS DATE) AS period_date, AVG(FOBUNITARIO) AS average_fob
            FROM exportaciones
            WHERE CAST(FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY period_date
        ), daily_trends AS (
            SELECT strftime(period_date, '%Y-%m-%d') AS period, average_fob,
                   LAG(average_fob, 1) OVER (ORDER BY period_date) AS prev_day_avg
            FROM daily_avg
        )
        SELECT period, average_fob,
               CASE WHEN prev_day_avg > 0 THEN ((average_fob - prev_day_avg) / prev_day_avg) * 100 ELSE NULL END AS change_from_previous
        FROM daily_trends ORDER BY period;
    """
    con = get_db_connection()
    df = con.execute(query).fetchdf()
    con.close()
    
    # --- PASO DE CORRECCIÓN AÑADIDO ---
    # Reemplazamos los NaN de Pandas con None de Python para que sea compatible con JSON
    df_cleaned = df.replace({np.nan: None})
    
    return df_cleaned.to_dict(orient="records")

@app.get("/api/rankings/exporters-weekly", response_model=List[ExporterRanking], tags=["Rankings"])
def get_weekly_exporter_rankings(start_date: date, end_date: date):
    query = f"""
        WITH weekly_fob AS (
            SELECT strftime(FECHAACEPT, '%Y-%W') AS week, NRO_EXPORTADOR, SUM(FOBUNITARIO) AS total_fob
            FROM exportaciones
            WHERE CAST(FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}'
            GROUP BY week, NRO_EXPORTADOR
        )
        SELECT week, RANK() OVER (PARTITION BY week ORDER BY total_fob DESC) AS rank, NRO_EXPORTADOR, total_fob
        FROM weekly_fob ORDER BY week, rank;
    """
    con = get_db_connection()
    df = con.execute(query).fetchdf()
    con.close()
    # Aplicamos la misma limpieza aquí por si acaso
    df_cleaned = df.replace({np.nan: None})
    return df_cleaned.to_dict(orient="records")

@app.get("/api/stats/average-weight-per-bulto", response_model=AverageWeight, tags=["Estadísticas"])
def get_average_weight_per_bulto(start_date: date, end_date: date):
    query = f"""
        SELECT SUM(exp.PESOBRUTOITEM) / NULLIF(SUM(bul.CANTIDADBULTO), 0) AS average_weight_per_bulto
        FROM exportaciones AS exp
        JOIN bultos_exportaciones AS bul ON exp.NUMEROIDENT = bul.NUMEROIDENT
        WHERE CAST(exp.FECHAACEPT AS DATE) BETWEEN '{start_date}' AND '{end_date}';
    """
    con = get_db_connection()
    df = con.execute(query).fetchdf()
    con.close()
    df_cleaned = df.replace({np.nan: None})
    result = df_cleaned.to_dict(orient="records")
    return result[0] if result else {"average_weight_per_bulto": None}