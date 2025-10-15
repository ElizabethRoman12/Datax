
"""
Módulo de conexión a base de datos PostgreSQL.
Crea un engine SQLAlchemy reutilizable para consultas o integraciones con pandas/ORM.
Compatible con ejecución manual o uso dentro de Airflow.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

load_dotenv(override=True, encoding="utf-8")

PG_URL = os.getenv("PG_URL")

if not PG_URL:
    raise RuntimeError("❌ Variable PG_URL no definida. Revisa tu archivo .env")

try:
    engine = create_engine(PG_URL, pool_pre_ping=True)
except SQLAlchemyError as e:
    raise RuntimeError(f"❌ Error al crear engine de PostgreSQL: {e}")

def get_engine():
    return engine
