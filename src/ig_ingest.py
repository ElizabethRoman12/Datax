import os
from datetime import datetime, date, timedelta, timezone
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
    insert_segmento_semanal,
)

# Configuración 
load_dotenv()

PLATAFORMA = "instagram"
PG_URL = os.getenv("PG_URL")
IG_USER_ID = os.getenv("IG_USER_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_IG")

if not PG_URL:
    raise RuntimeError("Falta PG_URL en .env")

# Helpers
def conn():
    """Devuelve conexión a PostgreSQL."""
    return psycopg2.connect(PG_URL)

def ig_get(path, params=None):
    """Wrapper de fb_get para IG."""
    return fb_get(path, params or {}, access_token=ACCESS_TOKEN)

def ig_paginate(path, params=None):
    """Wrapper de paginate para IG."""
    return paginate(path, params or {}, access_token=ACCESS_TOKEN)

def iso_date(s: str) -> date:
    """Convierte un timestamp ISO de IG a objeto date."""
    return datetime.fromisoformat(s.replace("Z", "+00:00").replace("+0000", "+00:00")).date()

def year_start() -> date:
    """Devuelve el inicio del año en UTC."""
    return datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date()

def ig_id() -> str:
    """Valida que IG_USER_ID exista y lo retorna como str."""
    if not IG_USER_ID:
        raise RuntimeError("Falta IG_USER_ID en .env")
    return str(IG_USER_ID)

# Cuenta/pagina
def ingest_account():
    """Inserta/actualiza la cuenta IG en tabla paginas."""
    js = ig_get(ig_id(), {"fields": "id,username"})
    cuenta = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("username", "Cuenta IG"),
    }
    with conn() as con:
        upsert_pagina(con, cuenta)

# Publicaciones
def get_media_since_year_start():
    """Obtiene publicaciones IG desde inicio de año."""
    fields = ",".join([
        "id","caption","media_type","media_url","permalink","timestamp",
        "thumbnail_url","like_count","comments_count",
        "children{media_type,media_url,permalink,timestamp,id}",
    ])
    for item in ig_paginate(f"{ig_id()}/media", {"fields": fields, "limit": 100}):
        ts = item.get("timestamp")
        if ts and iso_date(ts) >= year_start():
            yield item

def media_insights_lifetime(media_id: str) -> dict:
    """Obtiene métricas lifetime (alcance, guardados, vistas)."""
    out = {"reach": 0, "saved": 0, "video_views": 0}
    for metric in ("reach,saved", "video_views"):
        try:
            js = ig_get(f"{media_id}/insights", {"metric": metric})
            for m in js.get("data", []):
                vals = m.get("values", [])
                if vals:
                    out[m["name"]] = int(vals[-1].get("value") or 0)
        except RuntimeError as e:
            print(f"[WARN] insights {metric} falló para media {media_id}: {e}")
    return out

def ingest_media():
    """Inserta publicaciones IG y sus métricas."""
    with conn() as con:
        for m in get_media_since_year_start():
            media_id = str(m["id"])
            dia = iso_date(m.get("timestamp", datetime.now(timezone.utc).isoformat()))

            # Publicación base
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

            # Métricas
            ins = media_insights_lifetime(media_id)
            visualizaciones = int(
                m.get("video_view_count") or m.get("video_views") or ins.get("video_views", 0) or 0
            )
            metricas = {
                "visualizaciones": visualizaciones,
                "alcance": int(ins.get("reach", 0)),
                "impresiones": 0,
                "tiempo_promedio": None,
                "comentarios": int(m.get("comments_count") or 0),
                "compartidos": 0,
                "guardados": int(ins.get("saved", 0)),
                "clics_enlace": 0,
                "ctr": None,
            }
            upsert_metricas_publicacion_diaria(con, PLATAFORMA, ig_id(), media_id, dia, metricas)

            # Reacciones (solo "me_gusta" en IG)
            with con.cursor() as cur:
                cur.execute("""
                    INSERT INTO tipo_reaccion (plataforma, nombre)
                    VALUES (%s,%s)
                    ON CONFLICT (plataforma, nombre) DO UPDATE SET nombre=EXCLUDED.nombre
                    RETURNING id
                """, (PLATAFORMA, "me_gusta"))
                tipo_id = cur.fetchone()[0] # type: ignore

            upsert_reaccion_publicacion_diaria(con, PLATAFORMA, ig_id(), media_id, dia, tipo_id, int(m.get("like_count") or 0))

#  Cuenta Semanal 
def _ts_day(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

def ingest_account_weekly():
    """Inserta métricas semanales de cuenta IG (alcance y seguidores)."""
    hoy, hasta, desde = date.today(), date.today() - timedelta(days=1), date.today() - timedelta(days=29)
    per_day = {}

    def fetch_metric(metric, start, end):
        try:
            js = ig_get(f"{ig_id()}/insights", {
                "period": "day", "since": _ts_day(start), "until": _ts_day(end+timedelta(days=1)), "metric": metric
            })
            for m in js.get("data", []):
                for v in m.get("values", []):
                    d = datetime.fromisoformat(v["end_time"].replace("Z","+00:00")).date()
                    per_day.setdefault(d, {"reach": 0, "follower_count": 0})
                    per_day[d][metric] = int(v.get("value") or 0)
        except RuntimeError as e:
            print(f"[WARN] {metric} falló: {e}")

    fetch_metric("reach", desde, hasta)
    fetch_metric("follower_count", max(desde, hasta - timedelta(days=28)), hasta)

    # Agrupar por semana ISO
    by_week = {}
    for d, vals in per_day.items():
        y, w, _ = d.isocalendar()
        if (y,w) not in by_week or d >= by_week[(y,w)]["fecha"]:
            by_week[(y,w)] = {"fecha": d, "data": vals}

    with conn() as con:
        for entry in by_week.values():
            fila = {
                "fecha_corte": entry["fecha"],
                "impresiones": 0,
                "alcance": int(entry["data"].get("reach", 0)),
                "video_views": 0,
                "fans_total": int(entry["data"].get("follower_count", 0)),
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ig_id(), fila)

# Audiencia 
def ingest_audience_segments_weekly():
    """Inserta segmentación semanal de audiencia IG (ciudad, país, género, edad)."""
    fecha = date.today()
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
                if (b.get("dimension") or "").lower() != dim: continue
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
    print("→ IG: Ingesta de cuenta"); ingest_account()
    print("→ IG: Ingesta de publicaciones"); ingest_media()
    print("→ IG: Cuenta semanal"); ingest_account_weekly()
    print("→ IG: Audiencia semanal"); ingest_audience_segments_weekly()
    calcular_variaciones()
    print("✔ IG listo")

if __name__ == "__main__":
    main()
