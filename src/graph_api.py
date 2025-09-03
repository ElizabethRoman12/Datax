
"""
Cliente genérico para la API Graph de Meta (Facebook / Instagram).
Incluye manejo de tokens, reintentos y paginación.
"""

import os
import time
import requests
from typing import Dict, Iterable, Optional

GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")

#  Helpers 
def _pick_token(explicit: Optional[str] = None) -> str:
    """
    Selecciona el token de acceso
    """
    token = explicit or os.getenv("ACCESS_TOKEN") or os.getenv("ACCESS_TOKEN_FB") or os.getenv("ACCESS_TOKEN_IG")
    if not token:
        raise RuntimeError("Falta ACCESS_TOKEN/ACCESS_TOKEN_FB/ACCESS_TOKEN_IG en .env")
    return token

# Requests
def fb_get(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Dict:
    """
    Hace una petición GET a la Graph API.
    """
    token = _pick_token(access_token)
    params = (params or {}).copy()
    params["access_token"] = token
    url = f"{GRAPH_URL}/{path.lstrip('/')}"

    for attempt in range(5):  # hasta 5 reintentos
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 613):  # rate limit → backoff exponencial
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f"FB {r.status_code}: {r.text}")

    raise RuntimeError("Rate limit persistente tras 5 intentos")

def paginate(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Iterable[Dict]:
    """
    Itera sobre todas las páginas de resultados de la Graph API.
    """
    data = fb_get(path, params, access_token=access_token)
    while True:
        yield from data.get("data", [])
        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        data = requests.get(next_url, timeout=60).json()
