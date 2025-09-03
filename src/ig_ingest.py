import os
from datetime import datetime, date, timedelta, timezone
import psycopg2
from dotenv import load_dotenv

from graph_api import fb_get, paginate  # cliente Graph
from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_reaccion_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

load_dotenv()

PLATAFORMA = "instagram"
PG_URL = os.getenv("PG_URL")
IG_USER_ID = os.getenv("IG_USER_ID")  # 👈 ahora solo usamos IG_USER_ID
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_IG")

# ---------- helpers ----------
def ig_get(path: str, params: dict | None = None) -> dict:
    return fb_get(path, params or {}, access_token=ACCESS_TOKEN)

def ig_paginate(path, params=None):
    return paginate(path, params or {}, access_token=ACCESS_TOKEN)

def conn():
    if not PG_URL:
        raise RuntimeError("Falta PG_URL en .env")
    return psycopg2.connect(PG_URL)

def iso_date_from_any(s: str) -> date:
    s = s.replace("Z", "+00:00").replace("+0000", "+00:00")
    return datetime.fromisoformat(s).date()

def year_start_iso():
    return datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date()

def ensure_ig_user_id() -> str:
    """Devuelve el IG_USER_ID como string"""
    if not IG_USER_ID:
        raise RuntimeError("Falta IG_USER_ID en .env")
    return str(IG_USER_ID)

# ---------- Cuenta ----------
def ingest_account():
    """Inserta la cuenta de IG en la tabla paginas"""
    ig_id = ensure_ig_user_id()
    js = ig_get(ig_id, {"fields": "id,username"})
    cuenta = {
        "pagina_id": str(js["id"]),
        "plataforma": PLATAFORMA,
        "nombre": js.get("username", "Cuenta IG"),
    }
    with conn() as con:
        upsert_pagina(con, cuenta)

# ---------- Media (publicaciones) ----------
def get_media_since_year_start():
    ig_id = ensure_ig_user_id()
    fields = ",".join([
        "id","caption","media_type","media_url","permalink","timestamp",
        "thumbnail_url","like_count","comments_count",
        "children{media_type,media_url,permalink,timestamp,id}",
    ])
    for item in ig_paginate(f"{ig_id}/media", {"fields": fields, "limit": 100}):
        ts = item.get("timestamp")
        if not ts:
            continue
        if iso_date_from_any(ts) < year_start_iso():
            continue
        yield item

def media_insights_lifetime(media_id: str) -> dict:
    out = {"reach": 0, "saved": 0, "video_views": 0}
    try:
        js = ig_get(f"{media_id}/insights", {"metric": "reach,saved"})
        for m in js.get("data", []):
            vals = m.get("values", [])
            if vals:
                out[m["name"]] = int(vals[-1].get("value") or 0)
    except RuntimeError as e:
        print(f"[WARN] insights base (reach/saved) falló para media {media_id}: {e}")

    try:
        vjs = ig_get(f"{media_id}/insights", {"metric": "video_views"})
        for m in vjs.get("data", []):
            vals = m.get("values", [])
            if vals:
                out["video_views"] = int(vals[-1].get("value") or 0)
    except RuntimeError:
        pass

    return out

def ingest_media():
    ig_id = ensure_ig_user_id()
    with conn() as con:
        for m in get_media_since_year_start():
            media_id = str(m["id"])
            created_at = m.get("timestamp")
            dia = iso_date_from_any(created_at) if created_at else datetime.now(timezone.utc).date()

            like_count = int(m.get("like_count") or 0)
            comments_count = int(m.get("comments_count") or 0)
            media_type = (m.get("media_type") or "").upper()
            permalink = m.get("permalink")
            media_url = m.get("media_url")

            publicacion_row = {
                "id": media_id,
                "created_time": created_at,
                "message": m.get("caption"),
                "permalink_url": permalink,
                "status_type": media_type,
                "attachments": {"media_type": media_type, "unshimmed_url": media_url},
                "shares": {"count": 0},
                "comments": {"summary": {"total_count": comments_count}},
                "reactions": {"summary": {"total_count": like_count}},
            }
            upsert_publicacion(con, PLATAFORMA, ig_id, publicacion_row)

            ins = media_insights_lifetime(media_id)
            visualizaciones = int(
                m.get("video_view_count") or m.get("video_views") or ins.get("video_views", 0) or 0
            )

            metricas = {
                "visualizaciones": visualizaciones,
                "alcance": int(ins.get("reach", 0) or 0),
                "impresiones": 0,
                "tiempo_promedio": None,
                "comentarios": comments_count,
                "compartidos": 0,
                "guardados": int(ins.get("saved", 0) or 0),
                "clics_enlace": 0,
                "ctr": None,
            }
            upsert_metricas_publicacion_diaria(con, PLATAFORMA, ig_id, media_id, dia, metricas)

            with con.cursor() as cur:
                cur.execute("""
                    SELECT id FROM tipo_reaccion
                    WHERE plataforma=%s AND nombre=%s
                """, (PLATAFORMA, "me_gusta"))
                row = cur.fetchone()
                if row:
                    tipo_id = row[0]
                else:
                    cur.execute("""
                        INSERT INTO tipo_reaccion (plataforma, nombre)
                        VALUES (%s,%s) RETURNING id
                    """, (PLATAFORMA, "me_gusta"))
                    tipo_id = cur.fetchone()[0] # type: ignore

            upsert_reaccion_publicacion_diaria(
                con, PLATAFORMA, ig_id, media_id, dia, tipo_id, like_count
            )

# ---------- Account (estadísticas semanales) ----------
def _ts_day(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

def ingest_account_weekly():
    ig_id = ensure_ig_user_id()
    hoy = date.today()
    hasta = hoy - timedelta(days=1)
    desde = hasta - timedelta(days=28)

    def chunks_30d(start: date, end: date):
        cur = start
        while cur <= end:
            nxt = min(cur + timedelta(days=29), end)
            yield cur, nxt
            cur = nxt + timedelta(days=1)

    per_day = {}

    for c_desde, c_hasta in chunks_30d(desde, hasta):
        base_range = {
            "period": "day",
            "since": _ts_day(c_desde),
            "until": _ts_day(c_hasta + timedelta(days=1)),
        }

        try:
            js_reach = ig_get(f"{ig_id}/insights", {**base_range, "metric": "reach"})
            for m in js_reach.get("data", []):
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "follower_count": 0})
                    per_day[end]["reach"] = int(v.get("value") or 0)
        except RuntimeError as e:
            print(f"[WARN] insights diarios (reach) fallaron: {e}")

        try:
            fc_desde = max(c_desde, (c_hasta - timedelta(days=28)))
            js_fc = ig_get(
                f"{ig_id}/insights",
                {
                    "period": "day",
                    "since": _ts_day(fc_desde),
                    "until": _ts_day(c_hasta + timedelta(days=1)),
                    "metric": "follower_count",
                },
            )
            for m in js_fc.get("data", []):
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "follower_count": 0})
                    per_day[end]["follower_count"] = int(v.get("value") or 0)
        except RuntimeError as e:
            print(f"[WARN] insights diarios (follower_count) fallaron: {e}")

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
                "impresiones": 0,
                "alcance": int(vals.get("reach", 0) or 0),
                "video_views": 0,
                "fans_total": int(vals.get("follower_count", 0) or 0),
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ig_id, fila)

# ---------- Audiencia (segmentación semanal) ----------
def ingest_audience_segments_weekly():
    ig_id = ensure_ig_user_id()
    fecha = date.today()

    def fetch_breakdown(dim: str):
        return ig_get(
            f"{ig_id}/insights",
            {
                "metric": "follower_demographics",
                "period": "lifetime",
                "metric_type": "total_value",
                "breakdown": dim,
            },
        )

    buckets = {"city": {}, "country": {}, "gender": {}, "age": {}}

    for dim in ("city", "country", "gender", "age"):
        try:
            js = fetch_breakdown(dim)
        except RuntimeError as e:
            print(f"[WARN] demographics {dim} falló: {e}")
            continue

        data = js.get("data") or []
        if not data:
            continue

        m0 = data[0]
        bks = m0.get("breakdowns") or []
        if bks:
            for b in bks:
                if (b.get("dimension") or "").lower() != dim:
                    continue
                for v in b.get("values", []):
                    name = v.get("name") or v.get("value")
                    val = v.get("value")
                    try:
                        val = int(val)
                    except Exception:
                        val = 0
                    if name is not None:
                        buckets[dim][str(name)] = buckets[dim].get(str(name), 0) + val

    with conn() as con:
        for k, qty in buckets["city"].items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, ciudad=k, cantidad=int(qty or 0))
        for k, qty in buckets["country"].items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, pais=k, cantidad=int(qty or 0))
        for k, qty in buckets["gender"].items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, genero=k, cantidad=int(qty or 0))
        for k, qty in buckets["age"].items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, genero=f"AGE.{k}", cantidad=int(qty or 0))

# ---------- main ----------
def main():
    print("→ IG: Ingesta de cuenta")
    ingest_account()

    print("→ IG: Ingesta de publicaciones")
    ingest_media()

    print("→ IG: Cuenta semanal")
    ingest_account_weekly()

    print("→ IG: Audiencia semanal")
    ingest_audience_segments_weekly()

    print("✔ IG listo")

if __name__ == "__main__":
    main()
