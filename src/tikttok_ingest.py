# tiktok_ingest.py
import os
import time
from datetime import datetime, date, timedelta, timezone
import psycopg2
import requests
from dotenv import load_dotenv

from fb_sql import (
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

load_dotenv()

PLATAFORMA = "tiktok"
PG_URL = os.getenv("PG_URL")
TTK_ACCESS_TOKEN = os.getenv("TTK_ACCESS_TOKEN")
TTK_BUSINESS_ID = os.getenv("TTK_BUSINESS_ID")

API_BASE = "https://business-api.tiktok.com/open_api"  # Content API for Business

def conn():
    if not PG_URL:
        raise RuntimeError("Falta PG_URL en .env")
    return psycopg2.connect(PG_URL)

def ttk_get(path: str, params: dict | None = None):
    if not TTK_ACCESS_TOKEN:
        raise RuntimeError("Falta TTK_ACCESS_TOKEN")
    headers = {
        "Authorization": f"Bearer {TTK_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    url = f"{API_BASE}/{path.lstrip('/')}"
    r = requests.get(url, headers=headers, params=params or {}, timeout=30)
    if r.status_code >= 400:
        raise RuntimeError(f"TTK {r.status_code}: {r.text}")
    js = r.json()
    # la API suele envolver en {code,msg,data}
    if isinstance(js, dict) and js.get("code") not in (0, "0", None) and "data" in js:
        # algunos SDKs usan code==0 como OK
        pass
    return js

def ttk_paginate(path: str, params: dict | None = None, list_key: str = "list"):
    params = dict(params or {})
    cursor = None
    while True:
        if cursor:
            params["cursor"] = cursor
        js = ttk_get(path, params)
        data = (js.get("data") or {})
        items = data.get(list_key) or data.get("items") or []
        for it in items:
            yield it
        cursor = data.get("cursor") or data.get("next_cursor")
        if not cursor or data.get("has_more") in (0, False, None):
            break

def year_start_iso():
    return datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date()

def iso_date_from_any(s: str) -> date:
    # TikTok a veces devuelve epoch seconds; soporta ambos
    try:
        if isinstance(s, (int, float)) or s.isdigit():
            return datetime.fromtimestamp(int(s), tz=timezone.utc).date()
    except Exception:
        pass
    s = str(s).replace("Z", "+00:00")
    return datetime.fromisoformat(s).date()

# ---------- MEDIA ----------
def get_videos_since_year_start():
    """
    Lista de videos orgánicos de la cuenta Business.
    Endpoint típico (Content API): /v1.3/content/creative/list/ o similar
    La forma exacta puede variar por versión, por eso dejamos el path parametrizado.
    """
    # ejemplo genérico (ajusta si tu versión usa otro path o require business_id):
    params = {
        "business_id": TTK_BUSINESS_ID,
        "page_size": 50,
    }
    for item in ttk_paginate("/v1.3/content/creative/list/", params, list_key="creatives"):
        # filtramos por fecha
        created = item.get("create_time") or item.get("publish_time")
        if not created:
            continue
        if iso_date_from_any(created) < year_start_iso():
            continue
        yield item

def fetch_video_insights(video_id: str) -> dict:
    """
    Métricas lifetime por video.
    Suele existir un endpoint de insights por creative_id/video_id.
    """
    try:
        js = ttk_get("/v1.3/content/creative/insights/", {
            "business_id": TTK_BUSINESS_ID,
            "creative_id": video_id,
        })
        data = (js.get("data") or {}).get("insights") or {}
        # normalizamos nombres
        return {
            "views": int(data.get("views") or 0),
            "likes": int(data.get("likes") or 0),
            "comments": int(data.get("comments") or 0),
            "shares": int(data.get("shares") or 0),
            "saves": int(data.get("saves") or 0),
            "reach": int(data.get("reach") or 0),  # puede no venir → 0
        }
    except Exception:
        return {}

def ingest_media():
    with conn() as con:
        for m in get_videos_since_year_start():
            vid = m.get("creative_id") or m.get("id")
            if not vid:
                continue
            created = m.get("publish_time") or m.get("create_time")
            dia = iso_date_from_any(created) if created else date.today()

            caption = m.get("caption") or m.get("title")
            permalink = m.get("share_url") or m.get("permalink")
            thumb = (m.get("cover_url") or m.get("thumbnail")) or None
            media_url = m.get("video_url") or None

            # snapshot contadores ligeros si vienen en la lista
            like_count = int(m.get("like_count") or 0)
            comment_count = int(m.get("comment_count") or 0)
            share_count = int(m.get("share_count") or 0)
            view_count = int(m.get("view_count") or 0)

            publicacion_row = {
                "id": str(vid),
                "created_time": str(created) if created else None,
                "message": caption,
                "permalink_url": permalink,
                "status_type": "VIDEO",
                "attachments": {"media_type": "VIDEO", "unshimmed_url": media_url or thumb},
                "shares": {"count": share_count},
                "comments": {"summary": {"total_count": comment_count}},
                "reactions": {"summary": {"total_count": like_count}},
            }
            upsert_publicacion(con, PLATAFORMA, TTK_BUSINESS_ID, publicacion_row)

            ins = fetch_video_insights(str(vid))
            visualizaciones = view_count or int(ins.get("views") or 0)

            metricas = {
                "visualizaciones": visualizaciones,
                "alcance": int(ins.get("reach") or 0),
                "impresiones": 0,
                "tiempo_promedio": None,
                "reacciones": like_count or int(ins.get("likes") or 0),
                "me_gusta": like_count or int(ins.get("likes") or 0),
                "me_encanta": 0,
                "me_divierte": 0,
                "me_asombra": 0,
                "me_entristece": 0,
                "me_enoja": 0,
                "comentarios": comment_count or int(ins.get("comments") or 0),
                "compartidos": share_count or int(ins.get("shares") or 0),
                "guardados": int(ins.get("saves") or 0),
                "clics_enlace": 0,
                "ctr": None,
            }
            upsert_metricas_publicacion_diaria(con, PLATAFORMA, TTK_BUSINESS_ID, str(vid), dia, metricas)

# ---------- CUENTA: semanal ----------
def _ts_day(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

def ingest_account_weekly():
    """
    Suma/último valor semanal de vistas/seguidores (si disponible).
    Muchas APIs de TikTok devuelven métricas por día con rangos since/until.
    """
    hoy = date.today()
    hasta = hoy - timedelta(days=1)
    desde = hasta - timedelta(days=28)

    per_day = {}

    # Ejemplo de endpoint de cuenta (ajusta si tu Content API tiene otro):
    try:
        js = ttk_get("/v1.3/content/account/insights/", {
            "business_id": TTK_BUSINESS_ID,
            "period": "day",
            "since": _ts_day(desde),
            "until": _ts_day(hasta + timedelta(days=1)),
            "metrics": "views,followers,profile_views",  # según disponibilidad real
        })
        for row in (js.get("data") or {}).get("values", []):
            d = iso_date_from_any(row.get("end_time") or row.get("date") or hasta.isoformat())
            val = row.get("value") or {}
            per_day[d] = {
                "views": int(val.get("views") or 0),
                "followers": int(val.get("followers") or 0),
                "profile_views": int(val.get("profile_views") or 0),
            }
    except RuntimeError as e:
        print(f"[WARN] TikTok account insights falló: {e}")

    by_iso_week = {}
    for d, vals in per_day.items():
        y, w, _ = d.isocalendar()
        key = (y, w)
        if key not in by_iso_week or d >= by_iso_week[key]["fecha"]:
            by_iso_week[key] = {"fecha": d, "data": vals}

    with conn() as con:
        for entry in by_iso_week.values():
            fecha = entry["fecha"]
            vals = entry["data"]
            fila = {
                "fecha_corte": fecha,
                "impresiones": int(vals.get("views", 0) or 0),
                "alcance": 0,  # si tu API expone reach, cámbialo
                "video_views": int(vals.get("views", 0) or 0),
                "fans_total": int(vals.get("followers", 0) or 0),
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, TTK_BUSINESS_ID, fila)

# ---------- AUDIENCIA (segmentos) ----------
def ingest_audience_segments_weekly():
    """
    Demográficos: por país/ciudad/edad/género (si tu plan lo soporta).
    """
    fecha = date.today()
    buckets = {"city": {}, "country": {}, "gender": {}, "age": {}}

    try:
        js = ttk_get("/v1.3/content/account/audience/", {"business_id": TTK_BUSINESS_ID})
        aud = (js.get("data") or {})
        # Ajusta según tu payload real:
        for row in aud.get("by_city", []) or []:
            buckets["city"][row["name"]] = int(row.get("count") or 0)
        for row in aud.get("by_country", []) or []:
            buckets["country"][row["name"]] = int(row.get("count") or 0)
        for row in aud.get("by_gender", []) or []:
            buckets["gender"][row["name"]] = int(row.get("count") or 0)
        for row in aud.get("by_age", []) or []:
            buckets["age"][row["name"]] = int(row.get("count") or 0)
    except RuntimeError as e:
        print(f"[WARN] TikTok audience falló: {e}")

    with conn() as con:
        for k, qty in buckets["city"].items():
            insert_segmento_semanal(con, PLATAFORMA, TTK_BUSINESS_ID, fecha, ciudad=k, cantidad=int(qty or 0))
        for k, qty in buckets["country"].items():
            insert_segmento_semanal(con, PLATAFORMA, TTK_BUSINESS_ID, fecha, pais=k, cantidad=int(qty or 0))
        for k, qty in buckets["gender"].items():
            insert_segmento_semanal(con, PLATAFORMA, TTK_BUSINESS_ID, fecha, genero=k, cantidad=int(qty or 0))
        for k, qty in buckets["age"].items():
            insert_segmento_semanal(con, PLATAFORMA, TTK_BUSINESS_ID, fecha, genero=f"AGE.{k}", cantidad=int(qty or 0))

def main():
    print("→ TikTok: Ingesta de publicaciones")
    ingest_media()
    print("→ TikTok: Cuenta semanal")
    ingest_account_weekly()
    print("→ TikTok: Audiencia semanal")
    ingest_audience_segments_weekly()
    print("✔ TikTok listo")

if __name__ == "__main__":
    main()
