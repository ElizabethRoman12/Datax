"""
Cliente genérico para la API Graph de Meta (Facebook / Instagram).
Incluye manejo de tokens, reintentos, control de errores y paginación segura.
"""

import os
import time
import requests
from typing import Dict, Iterable, Optional

# Configuración base
GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")

# Helpers
def _pick_token(explicit: Optional[str] = None) -> str:
    """
    Selecciona el token de acceso.
    Prioriza: argumento explícito → ACCESS_TOKEN → ACCESS_TOKEN_FB → ACCESS_TOKEN_IG.
    """
    token = explicit or os.getenv("ACCESS_TOKEN") or os.getenv("ACCESS_TOKEN_FB") or os.getenv("ACCESS_TOKEN_IG")
    if not token:
        raise RuntimeError("❌ Falta ACCESS_TOKEN/ACCESS_TOKEN_FB/ACCESS_TOKEN_IG en .env o argumento")
    return token

# Requests
def fb_get(path: str, params: Optional[Dict] = None, access_token: Optional[str] = None) -> Dict:
    """
    Hace una petición GET a la Graph API con manejo de errores y reintentos.
    """
    token = _pick_token(access_token)
    params = (params or {}).copy()
    params["access_token"] = token
    url = f"{GRAPH_URL}/{path.lstrip('/')}"

    for attempt in range(5):  # hasta 5 reintentos
        try:
            r = requests.get(url, params=params, timeout=60)
        except requests.exceptions.RequestException as e:
            # Error de red o timeout → reintentar con backoff
            print(f"[WARN] Error de conexión (intento {attempt + 1}/5): {e}")
            time.sleep(2 ** attempt)
            continue

        if r.status_code == 200:
            return r.json()

        if r.status_code in (429, 613):
            print(f"[INFO] Rate limit detectado (intento {attempt + 1}/5)... esperando...")
            time.sleep(2 ** attempt)
            continue

        if r.status_code in (400, 401, 403):
            raise RuntimeError(f"FB Auth error {r.status_code}: {r.text}")

        raise RuntimeError(f"FB {r.status_code}: {r.text}")

    raise RuntimeError("Rate limit persistente tras 5 intentos o error de conexión")

# Paginación
def paginate(path: str, params: Optional[Dict] = None, access_token: Optional[str] = None) -> Iterable[Dict]:
    
    data = fb_get(path, params, access_token=access_token)

    while True:
        yield from data.get("data", [])

        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break

        try:
            r = requests.get(next_url, timeout=60)
            if r.status_code == 200:
                data = r.json()
            else:
                print(f"[WARN] Error {r.status_code} en paginación: {r.text[:200]}")
                break
        except requests.exceptions.RequestException as e:
            print(f"[WARN] Error de red en paginación: {e}")
            break
