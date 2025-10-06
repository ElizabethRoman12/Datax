import requests
from datetime import datetime, timedelta
from tokens import obtener_token, actualizar_token
import os
from dotenv import load_dotenv

load_dotenv()

def renovar_facebook_instagram():
    app_id = os.getenv("FB_APP_ID")
    app_secret = os.getenv("FB_APP_SECRET")
    token = obtener_token("facebook")   #Toma el token actual desde la BD

    url = "https://graph.facebook.com/v19.0/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": token["token_acceso"],
    }
    res = requests.get(url, params=params).json()

    if "access_token" in res:
        nuevo_token = res["access_token"]
        expira_en = datetime.now() + timedelta(days=60)

        # Actualizar tanto Facebook como Instagram
        actualizar_token("facebook", nuevo_token, expira_en=expira_en)
        actualizar_token("instagram", nuevo_token, expira_en=expira_en)

        print("Token renovado para Facebook e Instagram")
    else:
        print("Error al renovar Facebook/Instagram:", res)


if __name__ == "__main__":
    renovar_facebook_instagram()

