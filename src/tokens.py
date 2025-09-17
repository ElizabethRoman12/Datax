import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv
import requests

load_dotenv()
PG_URL = os.getenv("PG_URL")

FB_GRAPH = "https://graph.facebook.com/v19.0"

def obtener_token(plataforma: str):
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()
    cur.execute("SELECT token_acceso, token_refresh, expira_en FROM tokens_redes WHERE plataforma=%s", (plataforma,))
    fila = cur.fetchone()
    conn.close()
    if not fila:
        raise RuntimeError(f"No se encontró token para {plataforma}")
    return {
        "token_acceso": fila[0],
        "token_refresh": fila[1],
        "expira_en": fila[2]
    }

def obtener_token_pagina(token_usuario: str, page_id: str) -> str:
    """
    Devuelve el token de página a partir del token de usuario largo.
    """
    url = f"{FB_GRAPH}/me/accounts"
    params = {"access_token": token_usuario}
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json().get("data", [])
    for page in data:
        if page["id"] == page_id:
            return page["access_token"]
    raise RuntimeError(f"No encontré la página {page_id} en /me/accounts")


def actualizar_token(plataforma: str, token_acceso: str, token_refresh=None, expira_en=None):
    conn = psycopg2.connect(PG_URL)
    cur = conn.cursor()
    cur.execute("""
        UPDATE tokens_redes
        SET token_acceso=%s, token_refresh=%s, expira_en=%s, actualizado_en=now()
        WHERE plataforma=%s
    """, (token_acceso, token_refresh, expira_en, plataforma))
    conn.commit()
    conn.close()
