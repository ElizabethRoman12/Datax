import os
import psycopg2
import requests
from datetime import datetime
from dotenv import load_dotenv

# Configuración
load_dotenv()
PG_URL = os.getenv("PG_URL")
FB_GRAPH = os.getenv("FB_GRAPH_URL", "https://graph.facebook.com/v19.0")

if not PG_URL:
    raise RuntimeError("❌ Falta la variable PG_URL en .env")

# Funciones principales
def obtener_token(plataforma: str) -> dict:
    """
    Obtiene el token de acceso almacenado en la tabla 'tokens_redes'.
    Retorna un dict con:
      - token_acceso
      - token_refresh
      - expira_en
    """
    try:
        with psycopg2.connect(PG_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT token_acceso, token_refresh, expira_en FROM tokens_redes WHERE plataforma=%s",
                    (plataforma,),
                )
                fila = cur.fetchone()

        if not fila:
            raise RuntimeError(f"⚠ No se encontró token para la plataforma '{plataforma}'")

        return {
            "token_acceso": fila[0],
            "token_refresh": fila[1],
            "expira_en": fila[2],
        }

    except Exception as e:
        raise RuntimeError(f"Error al obtener token de {plataforma}: {e}")

# Token de Página (Facebook / Instagram)
def obtener_token_pagina(token_usuario: str, page_id: str) -> str:
   
    url = f"{FB_GRAPH}/me/accounts"
    params = {"access_token": token_usuario}

    try:
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json().get("data", [])
    except requests.exceptions.RequestException as e:
        raise RuntimeError(f"Error al obtener /me/accounts: {e}")

    for page in data:
        if page.get("id") == page_id:
            return page["access_token"]

    raise RuntimeError(f"No se encontró la página {page_id} en /me/accounts")

# Actualización de tokens
def actualizar_token(plataforma: str, token_acceso: str, token_refresh=None, expira_en=None):
  
    try:
        with psycopg2.connect(PG_URL) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE tokens_redes
                    SET token_acceso=%s,
                        token_refresh=%s,
                        expira_en=%s,
                        actualizado_en=now()
                    WHERE plataforma=%s
                    """,
                    (token_acceso, token_refresh, expira_en, plataforma),
                )
                conn.commit()
        print(f" Token de {plataforma} actualizado correctamente ({datetime.now().isoformat(timespec='seconds')})")

    except Exception as e:
        raise RuntimeError(f"Error al actualizar token de {plataforma}: {e}")
