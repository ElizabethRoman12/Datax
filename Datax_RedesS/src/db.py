# src/db.py
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

# Fuerza lectura del .env en UTF-8
load_dotenv(override=True, encoding="utf-8")

PG_URL = os.getenv("PG_URL")
if not PG_URL:
    raise RuntimeError("PG_URL no definida. Revisa tu archivo .env")

engine = create_engine(PG_URL, pool_pre_ping=True)
