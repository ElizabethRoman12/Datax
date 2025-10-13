# # src/db.py
# import os
# from sqlalchemy import create_engine
# from dotenv import load_dotenv

# # Fuerza lectura del .env en UTF-8
# load_dotenv(override=True, encoding="utf-8")

# PG_URL = os.getenv("PG_URL")
# if not PG_URL:
#     raise RuntimeError("PG_URL no definida. Revisa tu archivo .env")

# engine = create_engine(PG_URL, pool_pre_ping=True)


"""
Módulo de conexión a base de datos PostgreSQL.
Crea un engine SQLAlchemy reutilizable para consultas o integraciones con pandas/ORM.
Compatible con ejecución manual o uso dentro de Airflow.
"""

import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv

# ==========================================
# Cargar configuración
# ==========================================
load_dotenv(override=True, encoding="utf-8")

PG_URL = os.getenv("PG_URL")

if not PG_URL:
    raise RuntimeError("❌ Variable PG_URL no definida. Revisa tu archivo .env")

# ==========================================
# Crear engine global
# ==========================================
try:
    # `pool_pre_ping=True` evita errores de conexión inactiva
    engine = create_engine(PG_URL, pool_pre_ping=True)
    print("✅ Conexión a PostgreSQL inicializada correctamente.")
except SQLAlchemyError as e:
    raise RuntimeError(f"❌ Error al crear engine de PostgreSQL: {e}")

# ==========================================
# Helper opcional
# ==========================================
def get_engine():
    """Devuelve el engine SQLAlchemy global."""
    return engine
