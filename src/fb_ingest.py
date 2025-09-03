
import os
from datetime import datetime, timezone
import psycopg2
from dotenv import load_dotenv
from calc_variaciones import calcular_variaciones

from graph_api import fb_get, paginate
from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_reaccion_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal
)

#  Configuración 
load_dotenv()

PLATAFORMA   = "facebook"
FB_PAGE_ID   = os.getenv("FB_PAGE_ID")
PG_URL       = os.getenv("PG_URL")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_FB")

if not PG_URL:
    raise RuntimeError("Falta PG_URL en .env")


# Helpers

def conn():
    """Retorna una conexión PostgreSQL."""
    return psycopg2.connect(PG_URL)

def fb_get_fb(path, params=None):
    """Wrapper para fb_get con token ya configurado."""
    return fb_get(path, params or {}, access_token=ACCESS_TOKEN)

def fb_paginate(path, params=None):
    """Wrapper para paginate con token ya configurado."""
    return paginate(path, params or {}, access_token=ACCESS_TOKEN)

def parse_insights(js, metrics_map):
    """
    Convierte datos de /insights en un dict agrupado por fecha.
    """
    out = {}
    for m in js.get("data", []):
        name = m["name"]
        field = metrics_map.get(name)
        if not field:
            continue
        for v in m.get("values", []):
            fecha = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
            out.setdefault(fecha, {f: 0 for f in metrics_map.values()})
            out[fecha][field] = int(v.get("value") or 0)
    return out


# Ingesta Página

def ingest_page():
    """Inserta/actualiza la página base antes de publicaciones."""
    js = fb_get_fb(FB_PAGE_ID, {"fields": "id,name"})
    page = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("name", "Página sin nombre")
    }
    with conn() as con:
        upsert_pagina(con, page)


#  Ingesta Publicaciones 

def get_reactions_breakdown(post_id: str) -> dict:
    """Obtiene desglose de reacciones por tipo para un post."""
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
        out[es_key] = int(js.get("summary", {}).get("total_count", 0))
    return out

def get_posts_since():
    """Descarga publicaciones desde inicio de año."""
    year_start = datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date().isoformat()
    fields = ",".join([
        "id","created_time","message","permalink_url","status_type",
        "attachments{media_type,unshimmed_url}",
        "shares","comments.summary(true).limit(0)","reactions.summary(true).limit(0)"
    ])
    return fb_paginate(f"{FB_PAGE_ID}/posts", {"fields": fields, "since": year_start})

def daily_post_insights(post_id: str):
    """Obtiene métricas diarias de un post."""
    metrics_map = {
        "post_impressions": "impressions",
        "post_impressions_unique": "reach",
        "post_clicks": "clicks",
        "post_video_views": "video_views"
    }
    js = fb_get_fb(f"{post_id}/insights", {"metric": ",".join(metrics_map), "period": "day"})
    return parse_insights(js, metrics_map)

def ingest_posts():
    """Inserta publicaciones y sus métricas diarias."""
    with conn() as con:
        for p in get_posts_since():
            pub_id = str(p["id"])
            upsert_publicacion(con, PLATAFORMA, FB_PAGE_ID, p) # type: ignore

            comments = p.get("comments", {}).get("summary", {}).get("total_count", 0)
            shares   = p.get("shares", {}).get("count", 0)

            # Reacciones detalladas
            try:
                rx = get_reactions_breakdown(pub_id)
            except RuntimeError as e:
                print(f"[WARN] Reacciones no disponibles {pub_id}: {e}")
                rx = {k: 0 for k in ["me_gusta","me_encanta","me_divierte","me_asombra","me_entristece","me_enoja"]}

            # Métricas por día
            for dia, vals in sorted(daily_post_insights(pub_id).items()):
                impresiones = vals["impressions"]
                clicks      = vals["clicks"]
                ctr         = (clicks / impresiones * 100) if impresiones else None

                m = {
                    "visualizaciones": vals["video_views"],
                    "alcance": vals["reach"],
                    "impresiones": impresiones,
                    "tiempo_promedio": None,
                    "comentarios": comments,
                    "compartidos": shares,
                    "guardados": 0,
                    "clics_enlace": clicks,
                    "ctr": ctr,
                }
                upsert_metricas_publicacion_diaria(con, PLATAFORMA, FB_PAGE_ID, pub_id, dia, m)

                # Guardar reacciones por tipo
                for nombre_reaccion, cantidad in rx.items():
                    with con.cursor() as cur:
                        cur.execute("""
                            INSERT INTO tipo_reaccion (plataforma, nombre)
                            VALUES (%s,%s)
                            ON CONFLICT (plataforma, nombre) DO UPDATE SET nombre=EXCLUDED.nombre
                            RETURNING id
                        """, (PLATAFORMA, nombre_reaccion))
                        tipo_id = cur.fetchone()[0] # type: ignore

                    upsert_reaccion_publicacion_diaria(con, PLATAFORMA, FB_PAGE_ID, pub_id, dia, tipo_id, cantidad)


# Ingesta Página Semanal

def ingest_page_weekly():
    """Inserta métricas semanales de la página."""
    metrics_map = {
        "page_impressions": "impresiones",
        "page_impressions_unique": "alcance",
        "page_video_views": "video_views",
        "page_fans": "fans_total"
    }
    js = fb_get_fb(f"{FB_PAGE_ID}/insights", {"metric": ",".join(metrics_map), "period": "week"})
    by_week = parse_insights(js, metrics_map)

    with conn() as con:
        for fecha, fila in by_week.items():
            fila["fecha_corte"] = fecha
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, FB_PAGE_ID, fila)


# Ingesta Segmentos Semanales

def safe_insights(metric, period):
    """Consulta insights y maneja métricas no disponibles."""
    try:
        return fb_get_fb(f"{FB_PAGE_ID}/insights", {"metric": metric, "period": period})
    except RuntimeError as e:
        print(f"[WARN] Métrica no disponible: {metric} ({period}). {e}")
        return {"data": []}

def ingest_audience_segments_weekly():
    """Inserta segmentación semanal de seguidores (género, país, ciudad)."""
    metrics = {
        "genero": "page_fans_gender_age",
        "pais": "page_fans_country",
        "ciudad": "page_fans_city"
    }

    def latest_by_iso_week(js):
        points = [(datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date(), v.get("value", {}))
                  for m in js.get("data", []) for v in m.get("values", [])]
        byweek = {}
        for fecha, data in points:
            key = fecha.isocalendar()[:2]
            if key not in byweek or fecha >= byweek[key]["fecha"]:
                byweek[key] = {"fecha": fecha, "data": data}
        return byweek.values()

    with conn() as con:
        for campo, metric in metrics.items():
            for entry in latest_by_iso_week(safe_insights(metric, "lifetime")):
                fecha = entry["fecha"]
                for k, qty in (entry["data"] or {}).items():
                    insert_segmento_semanal(con, PLATAFORMA, FB_PAGE_ID, fecha, **{campo: k}, cantidad=int(qty or 0))


def main():
    print("→ Ingesta de página"); ingest_page()
    print("→ Ingesta de publicaciones"); ingest_posts()
    print("→ Ingesta semanal: página"); ingest_page_weekly()
    print("→ Ingesta semanal: segmentación de seguidores"); ingest_audience_segments_weekly()
    calcular_variaciones()
    print("✔ Listo")

if __name__ == "__main__":
    main()
