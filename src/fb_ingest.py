import os
import sys
from datetime import datetime, date, timedelta
import psycopg2
from dotenv import load_dotenv
from calc_variaciones import calcular_variaciones

from tokens import obtener_token, obtener_token_pagina
from graph_api import fb_get, paginate
from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_reaccion_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal
)

# Configuración
load_dotenv()

PLATAFORMA = "facebook"
FB_PAGE_ID = os.getenv("FB_PAGE_ID")
PG_URL = os.getenv("PG_URL")

if not PG_URL:
    raise RuntimeError("Falta PG_URL en .env")

# Token dinámico desde BD
token_usuario = obtener_token(PLATAFORMA)["token_acceso"]
token_facebook = obtener_token_pagina(token_usuario, FB_PAGE_ID)

# Helpers
def conn():
    """Retorna conexión PostgreSQL."""
    return psycopg2.connect(PG_URL)

def fb_get_fb(path, params=None):
    """Wrapper para fb_get con token dinámico."""
    return fb_get(path, params or {}, access_token=token_facebook)

def fb_paginate(path, params=None):
    """Wrapper para paginate con token dinámico."""
    return paginate(path, params or {}, access_token=token_facebook)

def parse_insights(js, metrics_map):
    """
    Convierte datos de /insights en un dict agrupado por fecha o lifetime.
    Si la métrica no trae 'end_time' (caso lifetime), usa la fecha actual.
    """
    out = {}
    for m in js.get("data", []):
        name = m.get("name")
        field = metrics_map.get(name)
        if not field:
            continue
        for v in m.get("values", []):
            end_time = v.get("end_time")
            if end_time:
                fecha = datetime.fromisoformat(end_time.replace("Z", "+00:00")).date()
            else:
                fecha = date.today()
            out.setdefault(fecha, {f: 0 for f in metrics_map.values()})
            out[fecha][field] = int(v.get("value") or 0)
    return out

# Página
def ingest_page():
    """Inserta/actualiza la página base."""
    js = fb_get_fb(FB_PAGE_ID, {"fields": "id,name"})
    page = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("name", "Página sin nombre")
    }
    with conn() as con:
        upsert_pagina(con, page)

# Publicaciones y Métricas
def get_posts_por_rango(inicio: date, fin: date):
    """
    Descarga publicaciones dentro del rango [inicio, fin] (inclusivo).
    Corrige el desfase UTC → hora local (UTC-4).
    """
    since = inicio - timedelta(days=1)
    fields = ",".join([
        "id","created_time","message","permalink_url","status_type",
        "attachments{media_type,unshimmed_url}",
        "shares","comments.summary(true).limit(0)","reactions.summary(true).limit(0)"
    ])

    publicaciones = []
    try:
        for post in fb_paginate(
            f"{FB_PAGE_ID}/posts",
            {"fields": fields, "since": since.isoformat(), "until": fin.isoformat()}
        ):
            ts = post.get("created_time")
            if not ts:
                continue
            dt_utc = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            dt_local = dt_utc - timedelta(hours=4)
            fecha_pub_local = dt_local.date()
            if inicio <= fecha_pub_local <= fin:
                publicaciones.append(post)
    except RuntimeError as e:
        print(f"[WARN] Error al obtener publicaciones del rango {inicio}–{fin}: {e}")
        return []

    if not publicaciones:
        print(f"⚠ No hay publicaciones entre {inicio} y {fin}")
    return publicaciones

def get_reactions_breakdown(post_id: str) -> dict:
    """Obtiene desglose de reacciones por tipo."""
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
        try:
            js = fb_get_fb(f"{post_id}/reactions", {"type": api_type, "summary": "total_count", "limit": 0})
            out[es_key] = int(js.get("summary", {}).get("total_count", 0))
        except RuntimeError:
            out[es_key] = 0
    return out

def daily_post_insights(post_id: str):
    """Obtiene métricas lifetime de una publicación."""
    metrics_map = {
        "post_impressions": "impressions",
        "post_impressions_unique": "reach",
        "post_clicks": "clicks",
        "post_video_views": "video_views"
    }
    js = fb_get_fb(f"{post_id}/insights", {
        "metric": ",".join(metrics_map),
        "period": "lifetime"
    })
    return parse_insights(js, metrics_map)



def ingest_posts(inicio: date, fin: date):
    """Inserta publicaciones y métricas dentro del rango (snapshot diario)."""
    posts = get_posts_por_rango(inicio, fin)
    if not posts:
        return

    print(f"* {len(posts)} publicaciones encontradas entre {inicio} y {fin}")

    #Fecha del snapshot del día (para métricas y reacciones)
    fecha_descarga = date.today()

    with conn() as con:
        for p in posts:
            pub_id = str(p["id"])
            upsert_publicacion(con, PLATAFORMA, FB_PAGE_ID, p)

            comments = p.get("comments", {}).get("summary", {}).get("total_count", 0)
            shares = p.get("shares", {}).get("count", 0)
            rx = get_reactions_breakdown(pub_id)

            insights = daily_post_insights(pub_id)
            for _, vals in sorted(insights.items()):
                impresiones = vals.get("impressions", 0)
                clicks = vals.get("clicks", 0)
                ctr = (clicks / impresiones * 100) if impresiones else None

                m = {
                    "visualizaciones": vals.get("video_views", 0),
                    "alcance": vals.get("reach", 0),
                    "impresiones": impresiones,
                    "tiempo_promedio": None,
                    "comentarios": comments,
                    "compartidos": shares,
                    "guardados": 0,
                    "clics_enlace": clicks,
                    "ctr": ctr,
                }
                upsert_metricas_publicacion_diaria(
                    con, PLATAFORMA, FB_PAGE_ID, pub_id, fecha_descarga, m
                )
                for nombre_reaccion, cantidad in rx.items():
                    with con.cursor() as cur:
                        cur.execute("""
                            INSERT INTO tipo_reaccion (plataforma, nombre)
                            VALUES (%s, %s)
                            ON CONFLICT (plataforma, nombre)
                            DO UPDATE SET nombre = EXCLUDED.nombre
                            RETURNING id
                        """, (PLATAFORMA, nombre_reaccion))
                        tipo_id = cur.fetchone()[0]

                    upsert_reaccion_publicacion_diaria(
                        con, PLATAFORMA, FB_PAGE_ID, pub_id, fecha_descarga, tipo_id, cantidad
                    )

# Métricas de Página y Audiencia
def ingest_page_metrics_range(inicio: date, fin: date):
    """Inserta métricas de la página dentro del rango."""
    metrics_map = {
        "page_impressions": "impresiones",
        "page_impressions_unique": "alcance",
        "page_video_views": "video_views",
        "page_fans": "fans_total"
    }
    js = fb_get_fb(f"{FB_PAGE_ID}/insights", {
        "metric": ",".join(metrics_map),
        "period": "day",
        "since": inicio.isoformat(),
        "until": fin.isoformat()
    })
    by_day = parse_insights(js, metrics_map)
    with conn() as con:
        for fecha, fila in by_day.items():
            if inicio <= fecha <= fin:
                fila["fecha_corte"] = fecha
                upsert_estadistica_pagina_semanal(con, PLATAFORMA, FB_PAGE_ID, fila)

def ingest_audience_segments_range(fecha: date):
    """Inserta segmentación de audiencia (género, país, ciudad)."""
    metrics = {
        "genero": "page_fans_gender_age",
        "pais": "page_fans_country",
        "ciudad": "page_fans_city"
    }

    def safe_insights(metric, period):
        try:
            return fb_get_fb(f"{FB_PAGE_ID}/insights", {"metric": metric, "period": period})
        except RuntimeError as e:
            print(f"[WARN] Métrica no disponible: {metric}. {e}")
            return {"data": []}

    with conn() as con:
        for campo, metric in metrics.items():
            js = safe_insights(metric, "lifetime")
            for m in js.get("data", []):
                for v in m.get("values", []):
                    data = v.get("value", {})
                    for k, qty in (data or {}).items():
                        insert_segmento_semanal(con, PLATAFORMA, FB_PAGE_ID, fecha, **{campo: k}, cantidad=int(qty or 0))
def main():
    if len(sys.argv) == 3:
        inicio = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        fin = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    else:
        inicio = fin = date.today()

    print(f"\n→ FB: Ingestando datos desde {inicio} hasta {fin}\n")
    ingest_page()
    ingest_posts(inicio, fin)
    ingest_page_metrics_range(inicio, fin)
    ingest_audience_segments_range(fin)
    calcular_variaciones()
    print("\n✔ FB listo (rango procesado correctamente)")

if __name__ == "__main__":
    main()
