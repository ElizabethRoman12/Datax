import os
from datetime import datetime, timezone, date
import psycopg2
from dotenv import load_dotenv

from graph_api import fb_get, paginate
from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_reaccion_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal
)

load_dotenv()

PLATAFORMA   = "facebook"
FB_PAGE_ID   = os.getenv("FB_PAGE_ID")     # 👈 ahora usamos FB_PAGE_ID
PG_URL       = os.getenv("PG_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_FB")

# ------------------ Helpers ------------------

def fb_get_fb(path, params=None):
    return fb_get(path, params or {}, access_token=ACCESS_TOKEN)

def fb_paginate(path, params=None):
    return paginate(path, params or {}, access_token=ACCESS_TOKEN)

def conn():
    if not PG_URL:
        raise RuntimeError("Falta PG_URL en .env")
    return psycopg2.connect(PG_URL)

# ------------------ Página ------------------

def ingest_page():
    """Inserta/actualiza la página antes de publicaciones"""
    js = fb_get_fb(FB_PAGE_ID, {"fields": "id,name"})
    page = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("name", "Página sin nombre")
    }
    with conn() as con:
        upsert_pagina(con, page)

# ------------------ Reacciones ------------------

def get_reactions_breakdown(post_id: str) -> dict:
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
        js = fb_get_fb(f"{post_id}/reactions", {
            "type": api_type, "summary": "total_count", "limit": 0
        })
        total = (js.get("summary") or {}).get("total_count", 0)
        out[es_key] = int(total or 0)
    return out

# ------------------ Publicaciones ------------------

def get_posts_since():
    year_start = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date().isoformat()
    fields = ",".join([
        "id","created_time","message","permalink_url","status_type",
        "attachments{media_type,unshimmed_url}",
        "shares","comments.summary(true).limit(0)","reactions.summary(true).limit(0)"
    ])
    params = {"fields": fields, "since": year_start}
    for item in fb_paginate(f"{FB_PAGE_ID}/posts", params):
        yield item

def daily_post_insights(post_id: str):
    metrics = ["post_impressions","post_impressions_unique","post_clicks","post_video_views"]
    js = fb_get_fb(f"{post_id}/insights", {"metric": ",".join(metrics), "period": "day"})
    out = {}
    for m in js.get("data", []):
        for v in m.get("values", []):
            d = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
            out.setdefault(d, {"impressions":0,"reach":0,"clicks":0,"video_views":0})
            val = int(v.get("value") or 0)
            if m["name"] == "post_impressions": out[d]["impressions"] = val
            elif m["name"] == "post_impressions_unique": out[d]["reach"] = val
            elif m["name"] == "post_clicks": out[d]["clicks"] = val
            elif m["name"] == "post_video_views": out[d]["video_views"] = val
    return out

def ingest_posts():
    with conn() as con:
        for p in get_posts_since():
            pub_id = str(p["id"])  # forzamos texto
            upsert_publicacion(con, PLATAFORMA, str(FB_PAGE_ID), p)

            comments  = (p.get("comments",  {}).get("summary", {}) or {}).get("total_count", 0)
            shares    = (p.get("shares", {}) or {}).get("count", 0)

            try:
                rx = get_reactions_breakdown(pub_id)
            except RuntimeError as e:
                print(f"[WARN] No pude obtener reacciones por tipo para {pub_id}: {e}")
                rx = {"me_gusta":0,"me_encanta":0,"me_divierte":0,"me_asombra":0,"me_entristece":0,"me_enoja":0}

            per_day = daily_post_insights(pub_id)
            for dia, vals in sorted(per_day.items()):
                impresiones = vals.get("impressions", 0)
                clicks      = vals.get("clicks", 0)
                ctr         = (clicks / impresiones * 100.0) if impresiones > 0 else None

                m = {
                    "visualizaciones": vals.get("video_views", 0),
                    "alcance":         vals.get("reach", 0),
                    "impresiones":     impresiones,
                    "tiempo_promedio": None,
                    "comentarios":     comments,
                    "compartidos":     shares,
                    "guardados":       0,
                    "clics_enlace":    clicks,
                    "ctr":             ctr,
                }
                upsert_metricas_publicacion_diaria(con, PLATAFORMA, str(FB_PAGE_ID), pub_id, dia, m)

                for nombre_reaccion, cantidad in rx.items():
                    with con.cursor() as cur:
                        cur.execute("""
                            SELECT id FROM tipo_reaccion
                            WHERE plataforma=%s AND nombre=%s
                        """, (PLATAFORMA, nombre_reaccion))
                        row = cur.fetchone()
                        if row:
                            tipo_id = row[0]
                        else:
                            cur.execute("""
                                INSERT INTO tipo_reaccion (plataforma, nombre)
                                VALUES (%s,%s) RETURNING id
                            """, (PLATAFORMA, nombre_reaccion))
                            tipo_id = cur.fetchone()[0] # type: ignore

                    upsert_reaccion_publicacion_diaria(
                        con, PLATAFORMA, str(FB_PAGE_ID), pub_id, dia, tipo_id, cantidad
                    )

# ------------------ Página semanal ------------------

def ingest_page_weekly():
    js = fb_get_fb(f"{FB_PAGE_ID}/insights", {
        "metric": ",".join([
            "page_impressions","page_impressions_unique","page_video_views","page_fans"
        ]),
        "period": "week"
    })
    by_week = {}
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
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, str(FB_PAGE_ID), fila)

# ------------------ Segmentación semanal ------------------

def safe_insights(metric, period):
    try:
        return fb_get_fb(f"{FB_PAGE_ID}/insights", {"metric": metric, "period": period})
    except RuntimeError as e:
        print(f"[WARN] Métrica no disponible: {metric} ({period}). Detalle: {e}")
        return {"data": []}

def ingest_audience_segments_weekly():
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
            key = end.isocalendar()[:2]
            if key not in byweek or end >= byweek[key]["fecha"]:
                byweek[key] = {"fecha": end, "data": data}
        return byweek

    g_week   = latest_by_iso_week(to_points(gender_js))
    ctry_week= latest_by_iso_week(to_points(country_js))
    city_week= latest_by_iso_week(to_points(city_js))

    with conn() as con:
        for entry in g_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, str(FB_PAGE_ID), fecha, genero=k, cantidad=int(qty or 0))

        for entry in ctry_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, str(FB_PAGE_ID), fecha, pais=k, cantidad=int(qty or 0))

        for entry in city_week.values():
            fecha = entry["fecha"]
            for k, qty in (entry["data"] or {}).items():
                insert_segmento_semanal(con, PLATAFORMA, str(FB_PAGE_ID), fecha, ciudad=k, cantidad=int(qty or 0))

# ------------------ Main ------------------

def main():
    print("→ Ingesta de página")
    ingest_page()

    print("→ Ingesta de publicaciones")
    ingest_posts()

    print("→ Ingesta semanal: página")
    ingest_page_weekly()

    print("→ Ingesta semanal: segmentación de seguidores")
    ingest_audience_segments_weekly()

    print("✔ Listo")

if __name__ == "__main__":
    main()
