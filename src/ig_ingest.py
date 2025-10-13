import os
import sys
from datetime import datetime, date, timedelta, timezone
import psycopg2
from dotenv import load_dotenv
from calc_variaciones import calcular_variaciones

from tokens import obtener_token  
from graph_api import fb_get, paginate
from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_reaccion_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

# Configuración
load_dotenv()

PLATAFORMA = "instagram"
PG_URL = os.getenv("PG_URL")
IG_USER_ID = os.getenv("IG_USER_ID")

if not PG_URL:
    raise RuntimeError("Falta PG_URL en .env")

token_instagram = obtener_token(PLATAFORMA)["token_acceso"]

# Helpers
def conn():
    return psycopg2.connect(PG_URL)

def ig_get(path, params=None):
    return fb_get(path, params or {}, access_token=token_instagram)

def ig_paginate(path, params=None):
    return paginate(path, params or {}, access_token=token_instagram)

def ig_id() -> str:
    if not IG_USER_ID:
        raise RuntimeError("Falta IG_USER_ID en .env")
    return str(IG_USER_ID)

def iso_date(s: str) -> date:
    return datetime.fromisoformat(s.replace("Z", "+00:00").replace("+0000", "+00:00")).date()

# Ingesta de Cuenta
def ingest_account():
    js = ig_get(ig_id(), {"fields": "id,username"})
    cuenta = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("username", "Cuenta IG"),
    }
    with conn() as con:
        upsert_pagina(con, cuenta)

# Publicaciones y Métricas
def get_media_por_rango(inicio: date, fin: date):
    """Obtiene publicaciones IG dentro del rango dado."""
    fields = ",".join([
        "id","caption","media_type","media_url","permalink","timestamp",
        "thumbnail_url","like_count","comments_count",
        "children{media_type,media_url,permalink,timestamp,id}",
    ])
    publicaciones = []
    for item in ig_paginate(f"{ig_id()}/media", {"fields": fields, "limit": 100}):
        ts = item.get("timestamp")
        if ts:
            fecha_pub = iso_date(ts)
            if inicio <= fecha_pub <= fin:
                publicaciones.append(item)
    return publicaciones

def media_insights_lifetime(media_id: str) -> dict:
    out = {"reach": 0, "saved": 0, "video_views": 0}
    for metric in ("reach,saved", "video_views"):
        try:
            js = ig_get(f"{media_id}/insights", {"metric": metric})
            for m in js.get("data", []):
                vals = m.get("values", [])
                if vals:
                    out[m["name"]] = int(vals[-1].get("value") or 0)
        except RuntimeError as e:
            if "does not support the video_views metric" not in str(e):
                print(f"[WARN] insights {metric} falló para media {media_id}: {e}")
    return out

from datetime import date

def ingest_media(inicio: date, fin: date):
    """Inserta publicaciones IG y sus métricas dentro del rango (snapshot diario)."""
    publicaciones = get_media_por_rango(inicio, fin)
    if not publicaciones:
        print(f"⚠ No hay publicaciones entre {inicio} y {fin}")
        return

    print(f"* {len(publicaciones)} publicaciones encontradas entre {inicio} y {fin}")

    fecha_descarga = date.today()

    with conn() as con:
        for m in publicaciones:
            media_id = str(m["id"])
            fecha_pub = iso_date(m["timestamp"])

            publicacion = {
                "id": media_id,
                "created_time": m.get("timestamp"),
                "message": m.get("caption"),
                "permalink_url": m.get("permalink"),
                "status_type": m.get("media_type", "").upper(),
                "attachments": {"media_type": m.get("media_type"), "unshimmed_url": m.get("media_url")},
                "shares": {"count": 0},
                "comments": {"summary": {"total_count": int(m.get("comments_count") or 0)}},
                "reactions": {"summary": {"total_count": int(m.get("like_count") or 0)}},
            }
            upsert_publicacion(con, PLATAFORMA, ig_id(), publicacion)

         
            ins = media_insights_lifetime(media_id)
            metricas = {
                "visualizaciones": int(ins.get("video_views", 0)),
                "alcance": int(ins.get("reach", 0)),
                "impresiones": 0,
                "tiempo_promedio": None,
                "comentarios": int(m.get("comments_count") or 0),
                "compartidos": 0,
                "guardados": int(ins.get("saved", 0)),
                "clics_enlace": 0,
                "ctr": None,
            }

            upsert_metricas_publicacion_diaria(
                con, PLATAFORMA, ig_id(), media_id, fecha_descarga, metricas
            )

            with con.cursor() as cur:
                cur.execute("""
                    INSERT INTO tipo_reaccion (plataforma, nombre)
                    VALUES (%s,%s)
                    ON CONFLICT (plataforma, nombre) DO UPDATE SET nombre=EXCLUDED.nombre
                    RETURNING id
                """, (PLATAFORMA, "me_gusta"))
                tipo_id = cur.fetchone()[0]

            upsert_reaccion_publicacion_diaria(
                con, PLATAFORMA, ig_id(), media_id, fecha_descarga, tipo_id, int(m.get("like_count") or 0)
            )

# Página y Audiencia
def ingest_account_range(inicio: date, fin: date):
    """Inserta métricas diarias de cuenta IG (seguidores y alcance) para el rango."""
    per_day = {}

    def fetch_metric(metric, start, end):
        try:
            js = ig_get(f"{ig_id()}/insights", {
                "period": "day",
                "since": int(datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp()),
                "until": int(datetime.combine(end, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp()),
                "metric": metric
            })
            for m in js.get("data", []):
                for v in m.get("values", []):
                    d = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
                    per_day.setdefault(d, {"reach": 0, "follower_count": 0})
                    per_day[d][metric] = int(v.get("value") or 0)
        except RuntimeError as e:
            print(f"[WARN] {metric} falló: {e}")

    fetch_metric("reach", inicio, fin)
    fetch_metric("follower_count", inicio, fin)

    with conn() as con:
        for d, vals in per_day.items():
            fila = {
                "fecha_corte": d,
                "impresiones": 0,
                "alcance": int(vals.get("reach", 0)),
                "video_views": 0,
                "fans_total": int(vals.get("follower_count", 0)),
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ig_id(), fila)

def ingest_audience_segments_range(fecha: date):
    dims = {"city": "ciudad", "country": "pais", "gender": "genero", "age": "genero"}
    def fetch_breakdown(dim):
        return ig_get(f"{ig_id()}/insights", {
            "metric": "follower_demographics",
            "period": "lifetime",
            "metric_type": "total_value",
            "breakdown": dim,
        })
    buckets = {k:{} for k in dims}
    for dim in dims:
        try:
            js = fetch_breakdown(dim)
            for b in (js.get("data",[{}])[0].get("breakdowns") or []):
                if (b.get("dimension") or "").lower() != dim:
                    continue
                for v in b.get("values", []):
                    name, val = str(v.get("name") or v.get("value")), int(v.get("value") or 0)
                    buckets[dim][name] = buckets[dim].get(name, 0) + val
        except RuntimeError as e:
            print(f"[WARN] demographics {dim} falló: {e}")

    with conn() as con:
        for dim, campo in dims.items():
            for k, qty in buckets[dim].items():
                insert_segmento_semanal(con, PLATAFORMA, ig_id(), fecha, **{campo: f"AGE.{k}" if dim=="age" else k}, cantidad=qty)

def main():
    # Fechas de entrada
    if len(sys.argv) == 3:
        inicio = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        fin = datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
    else:
        inicio = fin = date.today()

    print(f"\n→ IG: Ingestando datos desde {inicio} hasta {fin}\n")

    ingest_account()
    ingest_media(inicio, fin)
    ingest_account_range(inicio, fin)
    ingest_audience_segments_range(fin)

    calcular_variaciones()
    print("\n✔ IG listo (rango procesado correctamente)")

if __name__ == "__main__":
    main()
