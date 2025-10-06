# src/li_api.py
import requests
from tokens import obtener_token
import os
from dotenv import load_dotenv

# ================================
# Configuración
# ================================
load_dotenv()

# Cargar versiones desde .env (cada producto tiene la suya)
LI_VERSION_DMA = os.getenv("LI_VERSION_DMA", "202508")   # Pages Data Portability
LI_VERSION_ADS = os.getenv("LI_VERSION_ADS", "202509")   # Advertising API


# Tokens dinámicos desde la BD
token_dma = obtener_token("linkedin_pages")["token_acceso"]   # Pages Data Portability
token_ads = obtener_token("linkedin_ads")["token_acceso"]     # Advertising API

# Headers base
HEADERS_DMA = {
    "Authorization": f"Bearer {token_dma}",
    "LinkedIn-Version": LI_VERSION_DMA[:6],
    "X-Restli-Protocol-Version": "2.0.0"
}

HEADERS_ADS = {
    "Authorization": f"Bearer {token_ads}",
    "LinkedIn-Version": LI_VERSION_ADS[:6],
    "X-Restli-Protocol-Version": "2.0.0"
}





#================================
#Helpers
#================================

def li_get(url, params=None, mode="dma"):
    """
    Request simple a la API de LinkedIn.
    mode="ads" → Advertising API (campañas, métricas de página y posts)
    mode="dma" → Pages Data Portability API (posts orgánicos, seguidores segmentados)
    """
    headers = HEADERS_ADS if mode == "ads" else HEADERS_DMA
    r = requests.get(url, headers=headers, params=params or {})
    if not r.ok:
        raise RuntimeError(f"LinkedIn API error {r.status_code}: {r.text}")
    return r.json()

def li_paginate(url, params=None, mode="dma"):
    """
    Iterador sobre resultados paginados de LinkedIn.
    """
    params = params or {}
    params.setdefault("count", 50)
    start = 0
    while True:
        params["start"] = start
        js = li_get(url, params=params, mode=mode)
        elems = js.get("elements", [])
        if not elems:
            break
        for e in elems:
            yield e
        paging = js.get("paging", {})
        total = paging.get("total", 0)
        start += len(elems)
        if start >= total:
            break
