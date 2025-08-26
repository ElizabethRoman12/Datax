# import os, time, requests
# from typing import Dict, Iterable

# GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")
# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

# def fb_get(path: str, params: Dict | None = None) -> Dict:
#     if not ACCESS_TOKEN:
#         raise RuntimeError("Falta ACCESS_TOKEN en .env")
#     params = params.copy() if params else {}
#     params["access_token"] = ACCESS_TOKEN
#     url = f"{GRAPH_URL}/{path.lstrip('/')}"
#     for attempt in range(5):
#         r = requests.get(url, params=params, timeout=60)
#         if r.status_code == 200:
#             return r.json()
#         if r.status_code in (429, 613):  # rate limit
#             time.sleep(2 ** attempt)
#             continue
#         raise RuntimeError(f"FB {r.status_code}: {r.text}")
#     raise RuntimeError("Rate limit persistente")

# def paginate(path: str, params: Dict | None = None) -> Iterable[Dict]:
#     data = fb_get(path, params)
#     while True:
#         for row in data.get("data", []):
#             yield row
#         next_url = (data.get("paging") or {}).get("next")
#         if not next_url:
#             break
#         data = requests.get(next_url, timeout=60).json()
# fb_api.py
import os
import time
import requests
from typing import Dict, Iterable, Optional

GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")

def _pick_token(explicit: Optional[str] = None) -> str:
    """
    Regla para obtener token:
    - si llega explícito, úsalo
    - si no, intenta ACCESS_TOKEN (compat)
    - como fallback, intenta ACCESS_TOKEN_FB y ACCESS_TOKEN_IG por si existen
    """
    token = explicit or os.getenv("ACCESS_TOKEN") or os.getenv("ACCESS_TOKEN_FB") or os.getenv("ACCESS_TOKEN_IG")
    if not token:
        raise RuntimeError("Falta ACCESS_TOKEN/ACCESS_TOKEN_FB/ACCESS_TOKEN_IG en .env")
    return token

def fb_get(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Dict:
    """
    GET a Graph API. Acepta access_token opcional (recomendado).
    Maneja rate limits 429/613 con backoff exponencial.
    """
    token = _pick_token(access_token)
    params = (params or {}).copy()
    params["access_token"] = token
    url = f"{GRAPH_URL}/{path.lstrip('/')}"
    for attempt in range(5):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 613):  # rate limit
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f"FB {r.status_code}: {r.text}")
    raise RuntimeError("Rate limit persistente")

def paginate(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Iterable[Dict]:
    """
    Iterador de paginación (sigue paging.next). Inyecta token en la 1ª llamada.
    Las URLs 'next' ya incluyen el token y los parámetros.
    """
    data = fb_get(path, params, access_token=access_token)
    while True:
        for row in data.get("data", []):
            yield row
        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        data = requests.get(next_url, timeout=60).json()
