import os
from datetime import datetime, timedelta, timezone, date
import psycopg2
from dotenv import load_dotenv

from fb_api import fb_get, paginate
from fb_sql import (
    upsert_publicacion, upsert_metricas_publicacion_diaria,
    upsert_estadistica_pagina_semanal, insert_segmento_semanal
)


# --- Reacciones por tipo (Facebook exige tipos en EN: LIKE, LOVE, HAHA, WOW, SAD, ANGRY)
def get_reactions_breakdown(post_id: str) -> dict:
    """
    Retorna conteos por tipo con claves en español alineadas a tu BD:
    {
      'me_gusta': 10, 'me_encanta': 2, 'me_divierte': 0,
      'me_asombra': 1, 'me_entristece': 0, 'me_enoja': 0
    }
    """
    mapping = {
        "LIKE":  "me_gusta",
        "LOVE":  "me_encanta",
        "HAHA":  "me_divierte",
        "WOW":   "me_asombra",
        "SAD":   "me_entristece",
        "ANGRY": "me_enoja",
    }
    out = {v: 0 for v in mapping.values()}
    for api_type, es_key in mapping.items():
        js = fb_get(f"{post_id}/reactions", {"type": api_type, "summary": "total_count", "limit": 0})
        total = (js.get("summary") or {}).get("total_count", 0)
        out[es_key] = int(total or 0)
    return out


load_dotenv()

PLATAFORMA = os.getenv("PLATAFORMA", "facebook")
PAGE_ID = os.getenv("PAGE_ID")
PG_URL = os.getenv("PG_URL")
DAYS_BACK = int(os.getenv("DAYS_BACK", "30"))

def conn():
    return psycopg2.connect(PG_URL)

# ---------- POSTS ----------
# def get_posts_since(days_back: int):
#     since = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
#     fields = ",".join([
#         "id","created_time","message","permalink_url","status_type",
#         "attachments{media_type,unshimmed_url}",
#         "shares","comments.summary(true).limit(0)","reactions.summary(true).limit(0)"
#     ])
#     params = {"fields": fields, "since": since}
#     for item in paginate(f"{PAGE_ID}/posts", params):
#         yield item
def get_posts_since():
    # primer día del año actual
    year_start = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date().isoformat()
    fields = ",".join([
        "id","created_time","message","permalink_url","status_type",
        "attachments{media_type,unshimmed_url}",
        "shares","comments.summary(true).limit(0)","reactions.summary(true).limit(0)"
    ])
    params = {"fields": fields, "since": year_start}
    for item in paginate(f"{PAGE_ID}/posts", params):
        yield item

def daily_post_insights(post_id: str):
    metrics = [
        "post_impressions","post_impressions_unique","post_clicks",
        "post_video_views"
    ]
    js = fb_get(f"{post_id}/insights", {"metric": ",".join(metrics), "period": "day"})
    out = {}
    for m in js.get("data", []):
        for v in m.get("values", []):
            d = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
            out.setdefault(d, {"impressions":0,"reach":0,"clicks":0,"video_views":0})
            val = int(v.get("value") or 0)
            if m["name"] == "post_impressions": out[d]["impressions"] = val
            elif m["name"] == "post_impressions_unique": out[d]["reach"] = val  # reach ~ unique
            elif m["name"] == "post_clicks": out[d]["clicks"] = val
            elif m["name"] == "post_video_views": out[d]["video_views"] = val
    return out  # dict[date] -> metrics

def ingest_posts():
    with conn() as con:
        for p in get_posts_since():
            pub_id = p["id"]
            upsert_publicacion(con, PLATAFORMA, PAGE_ID, p)

            # Totales "snapshot" del post (summary lifetime)
            comments  = (p.get("comments",  {}).get("summary", {}) or {}).get("total_count", 0)
            reactions = (p.get("reactions", {}).get("summary", {}) or {}).get("total_count", 0)
            shares    = (p.get("shares", {}) or {}).get("count", 0)

            # Desglose por tipo (LIKE/LOVE/...)
            try:
                rx = get_reactions_breakdown(pub_id)
            except RuntimeError as e:
                print(f"[WARN] No pude obtener reacciones por tipo para {pub_id}: {e}")
                rx = {"me_gusta":0,"me_encanta":0,"me_divierte":0,"me_asombra":0,"me_entristece":0,"me_enoja":0}

            # Serie diaria de insights
            per_day = daily_post_insights(pub_id)
            for dia, vals in sorted(per_day.items()):
                impresiones = vals.get("impressions", 0)
                clicks      = vals.get("clicks", 0)
                ctr         = (clicks / impresiones * 100.0) if impresiones > 0 else None

                m = {
                    "visualizaciones": vals.get("video_views", 0),
                    "alcance":         vals.get("reach", 0),
                    "impresiones":     impresiones,
                    "tiempo_promedio": None,  # sin /video_insights

                    # reacciones: total + tipos
                    "reacciones":      reactions,
                    "me_gusta":        rx["me_gusta"],
                    "me_encanta":      rx["me_encanta"],
                    "me_divierte":     rx["me_divierte"],
                    "me_asombra":      rx["me_asombra"],
                    "me_entristece":   rx["me_entristece"],
                    "me_enoja":        rx["me_enoja"],

                    "comentarios":     comments,
                    "compartidos":     shares,
                    "guardados":       0,  # FB no expone saved

                    "clics_enlace":    clicks,
                    "ctr":             ctr,
                }
                upsert_metricas_publicacion_diaria(con, PLATAFORMA, PAGE_ID, pub_id, dia, m)


# ---------- PÁGINA (semanal) ----------
def iso_week_end(d: date) -> date:
    # end_time de insights FB ya es el corte semanal (domingo)
    return d

def ingest_page_weekly():
    js = fb_get(f"{PAGE_ID}/insights", {
        "metric": ",".join([
            "page_impressions","page_impressions_unique","page_video_views","page_fans"
        ]),
        "period": "week"
    })
    by_week = {}  # fecha_corte -> dict
    for m in js.get("data", []):
        name = m["name"]
        for v in m.get("values", []):
            end = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
            row = by_week.setdefault(end, {"fecha_corte": end, "impresiones":0, "alcance":0, "video_views":0, "fans_total":0})
            val = int(v.get("value") or 0)
            if name == "page_impressions": row["impresiones"] = val
            elif name == "page_impressions_unique": row["alcance"] = val
            elif name == "page_video_views": row["video_views"] = val
            elif name == "page_fans": row["fans_total"] = val

    with conn() as con:
        for _, fila in by_week.items():
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, PAGE_ID, fila)

# ---------- SEGMENTACIÓN (semanal, múltiples filas) ----------
def safe_insights(metric, period):
    try:
        return fb_get(f"{PAGE_ID}/insights", {"metric": metric, "period": period})
    except RuntimeError as e:
        # Si Meta devuelve 400 invalid metric, lo registramos y seguimos
        print(f"[WARN] Métrica no disponible: {metric} ({period}). Detalle: {e}")
        return {"data": []}

def ingest_audience_segments_weekly():
    # Muchos tenants ya NO tienen estas métricas demográficas por API
    gender_js  = safe_insights("page_fans_gender_age", "lifetime")
    country_js = safe_insights("page_fans_country", "lifetime")
    city_js    = safe_insights("page_fans_city", "lifetime")

    def to_points(js):
        pts = []
        for m in js.get("data", []):
            for v in m.get("values", []):
                end = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
                pts.append((end, v.get("value") or {}))
        return pts

    def latest_by_iso_week(points):
        byweek = {}
        for end, data in points:
            key = end.isocalendar()[:2]  # (year, week)
            if key not in byweek or end >= byweek[key]["fecha"]:
                byweek[key] = {"fecha": end, "data": data}
        return byweek

    g_week   = latest_by_iso_week(to_points(gender_js))
    ctry_week= latest_by_iso_week(to_points(country_js))
    city_week= latest_by_iso_week(to_points(city_js))

    with conn() as con:
        # Género-edad (si existe)
        for entry in g_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, genero=k, cantidad=int(qty or 0))
        # País (si existe)
        for entry in ctry_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, pais=k, cantidad=int(qty or 0))
        # Ciudad (si existe)
        for entry in city_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, ciudad=k, cantidad=int(qty or 0))


    def to_points(js):
        out = []
        for m in js.get("data", []):
            for v in m.get("values", []):
                end = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
                out.append((end, v.get("value") or {}))
        return out  # list[(fecha, dict)]
    
    gender_points = to_points(gender_js)
    country_points = to_points(country_js)
    city_points = to_points(city_js)

    # Nos quedamos con el último punto de cada semana ISO (snapshot semanal)
    def latest_by_iso_week(points):
        byweek = {}
        for end, data in points:
            y, w, _ = end.isocalendar()
            key = (y, w)
            # nos quedamos con el punto más reciente de esa semana
            if key not in byweek or end >= byweek[key]["fecha"]:
                byweek[key] = {"fecha": end, "data": data}
        return byweek  # {(año,sem): {"fecha": date, "data": {...}}}

    g_week = latest_by_iso_week(gender_points)
    ctry_week = latest_by_iso_week(country_points)
    city_week = latest_by_iso_week(city_points)

    with conn() as con:
        # Género‑edad
        for (_, _), entry in g_week.items():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():  # ej. "M.18-24": 123
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, genero=k, cantidad=int(qty or 0))

        # País
        for (_, _), entry in ctry_week.items():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():  # ej. "BO": 1234
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, pais=k, cantidad=int(qty or 0))

        # Ciudad
        for (_, _), entry in city_week.items():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():  # ej. "Cochabamba": 456
                insert_segmento_semanal(con, PLATAFORMA, PAGE_ID, fecha, ciudad=k, cantidad=int(qty or 0))
                

def main():
    print("→ Ingesta de publicaciones")
    ingest_posts()
    print("→ Ingesta semanal: página")
    ingest_page_weekly()
    print("→ Ingesta semanal: segmentación de seguidores")
    ingest_audience_segments_weekly()
    print("✔ Listo")

if __name__ == "__main__":
    main()