"""
Renueva los tokens de acceso de Facebook e Instagram usando el token actual de usuario largo.
Actualiza ambos registros en la base de datos (tabla tokens_redes).
Puede ejecutarse manualmente o como tarea en Airflow.
"""

import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tokens import obtener_token, actualizar_token

# Configuración
load_dotenv()
FB_GRAPH = os.getenv("FB_GRAPH_URL", "https://graph.facebook.com/v19.0")

APP_ID = os.getenv("FB_APP_ID")
APP_SECRET = os.getenv("FB_APP_SECRET")

if not APP_ID or not APP_SECRET:
    raise RuntimeError("❌ Faltan variables FB_APP_ID o FB_APP_SECRET en .env")

def renovar_facebook_instagram():
    """
    Renueva el token de acceso de Facebook e Instagram (válido por ~60 días).
    """
    print(f"🔄 Iniciando renovación de tokens ({datetime.now().isoformat(timespec='seconds')})")

    try:
        token = obtener_token("facebook")  # obtiene token actual desde la BD
        token_actual = token["token_acceso"]

        if not token_actual:
            raise RuntimeError("No se encontró token actual de Facebook en la base de datos")

        url = f"{FB_GRAPH}/oauth/access_token"
        params = {
            "grant_type": "fb_exchange_token",
            "client_id": APP_ID,
            "client_secret": APP_SECRET,
            "fb_exchange_token": token_actual,
        }

        print("📡 Solicitando nuevo token a Meta Graph API...")
        r = requests.get(url, params=params, timeout=30)
        r.raise_for_status()
        res = r.json()

    except requests.exceptions.RequestException as e:
        print(f"❌ Error de red al contactar con Meta: {e}")
        return
    except Exception as e:
        print(f"❌ Error al obtener token actual: {e}")
        return

    # Validar respuesta
    if "access_token" not in res:
        print(f"⚠ No se pudo renovar el token. Respuesta recibida: {res}")
        return

    # Nuevo token válido
    nuevo_token = res["access_token"]
    expira_en = datetime.now() + timedelta(days=60)

    try:
        actualizar_token("facebook", nuevo_token, expira_en=expira_en)
        actualizar_token("instagram", nuevo_token, expira_en=expira_en)
        print(f"✅ Token renovado correctamente. Nuevo vencimiento: {expira_en.date()}")
    except Exception as e:
        print(f"❌ Error al actualizar token en base de datos: {e}")

    print("✔ Proceso de renovación finalizado.")

if __name__ == "__main__":
    renovar_facebook_instagram()
