import os, time, requests
from typing import Dict, Iterable

GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")

def fb_get(path: str, params: Dict | None = None) -> Dict:
    if not ACCESS_TOKEN:
        raise RuntimeError("Falta ACCESS_TOKEN en .env")
    params = params.copy() if params else {}
    params["access_token"] = ACCESS_TOKEN
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

def paginate(path: str, params: Dict | None = None) -> Iterable[Dict]:
    data = fb_get(path, params)
    while True:
        for row in data.get("data", []):
            yield row
        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        data = requests.get(next_url, timeout=60).json()
