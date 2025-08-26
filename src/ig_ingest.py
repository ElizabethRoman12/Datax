# ig_ingest.py
import os
import time  # para backoff en reintentos
from datetime import datetime, date, timedelta, timezone
import psycopg2
from dotenv import load_dotenv

from fb_api import fb_get, paginate  # cliente Graph
from fb_sql import (
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

load_dotenv()

PLATAFORMA = "instagram"
PG_URL = os.getenv("PG_URL")
IG_USER_ID = os.getenv("IG_USER_ID")  # si no está, se resuelve desde PAGE_ID
PAGE_ID = os.getenv("PAGE_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_IG")  # token específico para IG


# ---------- helpers de token ----------
def ig_get(path: str, params: dict | None = None) -> dict:
    # proxy al cliente de Graph pero forzando el token IG
    return fb_get(path, params or {}, access_token=ACCESS_TOKEN)


def ig_paginate(path, params=None):
    return paginate(path, params or {}, access_token=ACCESS_TOKEN)


# ---------- utilidades ----------
def conn():
    if not PG_URL:
        raise RuntimeError("Falta PG_URL en .env")
    return psycopg2.connect(PG_URL)


def iso_date_from_any(s: str) -> date:
    # IG devuelve timestamp como ...Z o +0000
    s = s.replace("Z", "+00:00").replace("+0000", "+00:00")
    return datetime.fromisoformat(s).date()


def year_start_iso():
    return datetime(datetime.now().year, 1, 1, tzinfo=timezone.utc).date()


def ensure_ig_user_id() -> str:
    """
    Si no tenemos IG_USER_ID, lo intentamos resolver desde la Page:
    GET /{PAGE_ID}?fields=instagram_business_account
    """
    global IG_USER_ID
    if IG_USER_ID:
        return IG_USER_ID
    if not PAGE_ID:
        raise RuntimeError("Falta IG_USER_ID o PAGE_ID en .env para resolver IG_USER_ID")
    js = ig_get(f"{PAGE_ID}", {"fields": "instagram_business_account"})
    iba = (js.get("instagram_business_account") or {}).get("id")
    if not iba:
        raise RuntimeError("No se encontró instagram_business_account en la Page. Vincula IG Business/Creator.")
    IG_USER_ID = iba
    return IG_USER_ID


# -------------------------------------------
WARN_COUNT = 0
MAX_WARN = 5


def warn_once(msg):
    global WARN_COUNT
    if WARN_COUNT < MAX_WARN:
        print(msg)
    WARN_COUNT += 1


# ---------- MEDIA (publicaciones) ----------
def get_media_since_year_start():
    """
    Lista media (posts, reels, carruseles) desde 1/enero del año actual.
    IG no siempre respeta 'since' en /media, por eso filtramos por timestamp del lado cliente.
    """
    ig_id = ensure_ig_user_id()
    fields = ",".join(
        [
            "id",
            "caption",
            "media_type",
            "media_url",
            "permalink",
            "timestamp",
            "thumbnail_url",
            "like_count",
            "comments_count",
            "children{media_type,media_url,permalink,timestamp,id}",
        ]
    )
    for item in ig_paginate(f"{ig_id}/media", {"fields": fields, "limit": 100}):
        ts = item.get("timestamp")
        if not ts:
            continue
        if iso_date_from_any(ts) < year_start_iso():
            continue  # fuera de rango (año actual)
        yield item


def media_insights_lifetime(media_id: str) -> dict:
    """
    Métricas lifetime por media compatibles con IG v22+.
    impressions/plays ya no están soportadas.
    """
    out = {"reach": 0, "saved": 0, "video_views": 0}

    # reach + saved (soportadas por v22+)
    try:
        js = ig_get(f"{media_id}/insights", {"metric": "reach,saved"})
        for m in js.get("data", []):
            vals = m.get("values", [])
            if vals:
                out[m["name"]] = int(vals[-1].get("value") or 0)
    except RuntimeError as e:
        print(f"[WARN] insights base (reach/saved) falló para media {media_id}: {e}")

    # video_views ya no es estable vía insights → usar solo si aplica
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
    """
    Inserta:
      - publicacion (normalizada)
      - métricas por día (snapshot en fecha de publicación)
    """
    ig_id = ensure_ig_user_id()
    with conn() as con:
        for m in get_media_since_year_start():
            media_id = m["id"]
            created_at = m.get("timestamp")
            dia = iso_date_from_any(created_at) if created_at else datetime.now(timezone.utc).date()

            like_count = int(m.get("like_count") or 0)
            comments_count = int(m.get("comments_count") or 0)
            media_type = (m.get("media_type") or "").upper()
            permalink = m.get("permalink")
            media_url = m.get("media_url")

            # 1) Normaliza la publicación (compat con tu modelo FB)
            publicacion_row = {
                "id": media_id,
                "created_time": created_at,
                "message": m.get("caption"),
                "permalink_url": permalink,
                "status_type": media_type,  # lo usamos como “formato”
                "attachments": {"media_type": media_type, "unshimmed_url": media_url},
                "shares": {"count": 0},  # IG no expone shares por media
                "comments": {"summary": {"total_count": comments_count}},
                "reactions": {"summary": {"total_count": like_count}},
            }
            upsert_publicacion(con, PLATAFORMA, ig_id, publicacion_row)

            # 2) Insights “seguros” por media (v22+):
            try:
                ins = media_insights_lifetime(media_id)  # reach, saved, video_views (si aplica)
            except Exception as e:
                print(f"[WARN] insights fallaron para media {media_id}: {e}")
                ins = {}

            # 3) Visualizaciones (preferir campo del media si existe)
            visualizaciones = int(
                m.get("video_view_count") or m.get("video_views") or ins.get("video_views", 0) or 0
            )

            metricas = {
                "visualizaciones": visualizaciones,  # videos/reels si el campo existe
                "alcance": int(ins.get("reach", 0) or 0),
                "impresiones": 0,  # v22+ ya no disponible por media
                "tiempo_promedio": None,
                "reacciones": like_count,
                "me_gusta": like_count,
                "me_encanta": 0,
                "me_divierte": 0,
                "me_asombra": 0,
                "me_entristece": 0,
                "me_enoja": 0,
                "comentarios": comments_count,
                "compartidos": 0,  # IG no expone shares por media
                "guardados": int(ins.get("saved", 0) or 0),
                "clics_enlace": 0,
                "ctr": None,
            }

            upsert_metricas_publicacion_diaria(con, PLATAFORMA, ig_id, media_id, dia, metricas)


# -----------------------------------------
def _ts_day(d: date) -> int:
    # IG Graph usa epoch seconds (UTC) para since/until
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())


def ingest_account_weekly():
    ig_id = ensure_ig_user_id()

    # IG: follower_count (y reach day) -> máx últimos 30 días EXCLUYENDO el día actual
    hoy = date.today()
    hasta = hoy - timedelta(days=1)  # ayer
    desde = hasta - timedelta(days=28)  # 29 días exactos

    def chunks_30d(start: date, end: date):
        cur = start
        while cur <= end:
            nxt = min(cur + timedelta(days=29), end)
            yield cur, nxt
            cur = nxt + timedelta(days=1)

    per_day = {}

    for c_desde, c_hasta in chunks_30d(desde, hasta):
        # ---- dentro del for c_desde, c_hasta ----
        base_range = {
            "period": "day",
            "since": _ts_day(c_desde),
            "until": _ts_day(c_hasta + timedelta(days=1)),  # until exclusivo
        }

        # A) reach (solo)
        try:
            js_reach = ig_get(f"{ig_id}/insights", {**base_range, "metric": "reach"})
            for m in js_reach.get("data", []):
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "profile_views": 0, "follower_count": 0})
                    per_day[end]["reach"] = int(v.get("value") or 0)
        except RuntimeError as e:
            warn_once(f"[WARN] insights diarios (reach) fallaron: {e}")

        # B) follower_count (solo) → fuerza sub-rango de 29 días
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
                    per_day.setdefault(end, {"reach": 0, "profile_views": 0, "follower_count": 0})
                    per_day[end]["follower_count"] = int(v.get("value") or 0)
        except RuntimeError as e:
            warn_once(f"[WARN] insights diarios (follower_count) fallaron: {e}")

        # C) profile_views (total_value)
        try:
            js_tv = ig_get(
                f"{ig_id}/insights",
                {**base_range, "metric": "profile_views", "metric_type": "total_value"},
            )
            for m in js_tv.get("data", []):
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "profile_views": 0, "follower_count": 0})
                    raw = v.get("value")
                    val = int(raw.get("value")) if isinstance(raw, dict) else int(raw or 0) # type: ignore
                    per_day[end]["profile_views"] = val
        except RuntimeError as e:
            warn_once(f"[WARN] insights total_value (profile_views) fallaron: {e}")

    # 3) último punto por semana ISO
    by_iso_week = {}
    for d, vals in per_day.items():
        y, w, _ = d.isocalendar()
        key = (y, w)
        if key not in by_iso_week or d >= by_iso_week[key]["fecha"]:
            by_iso_week[key] = {"fecha": d, "data": vals}

    # 4) persistir
    with conn() as con:
        for entry in by_iso_week.values():
            fecha = entry["fecha"]
            vals = entry["data"]
            fila = {
                "fecha_corte": fecha,
                "impresiones": 0,  # no disponible cuenta v22+
                "alcance": int(vals.get("reach", 0) or 0),
                "video_views": 0,  # no disponible cuenta
                "fans_total": int(vals.get("follower_count", 0) or 0),
                # "profile_views": int(vals.get("profile_views", 0) or 0),  # opcional
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ig_id, fila)

# ------------------------
def ig_get_retry(path: str, params: dict | None = None, retries=3, backoff=1.2):
    last_err = None
    for i in range(retries):
        try:
            return ig_get(path, params or {})
        except RuntimeError as e:
            last_err = e
            # si es 5xx o "unknown" -> retry
            msg = str(e)
            if '"code":1' in msg or "unknown error" in msg.lower() or "An unknown error" in msg:
                time.sleep(backoff ** i)
                continue
            break
    raise last_err # type: ignore

# ---------- AUDIENCIA: lifetime -> snapshot semanal ----------
def ingest_audience_segments_weekly():
    ig_id = ensure_ig_user_id()
    fecha = date.today()

    def fetch_breakdown(dim: str):
        # cada llamada un breakdown
        return ig_get_retry(
            f"{ig_id}/insights",
            {
                "metric": "follower_demographics",
                "period": "lifetime",
                "metric_type": "total_value",
                "breakdown": dim,
            },
            retries=4,
            backoff=1.5,
        )

    # acumular resultados
    buckets = {"city": {}, "country": {}, "gender": {}, "age": {}}

    for dim in ("city", "country", "gender", "age"):
        try:
            js = fetch_breakdown(dim)
        except RuntimeError as e:
            warn_once(f"[WARN] demographics {dim} falló: {e}")
            continue

        data = js.get("data") or []
        if not data:
            continue

        m0 = data[0]
        # Forma nueva con breakdowns
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
                        try:
                            val = int((val or {}).get("value", 0))
                        except Exception:
                            val = 0
                    if name is not None:
                        buckets[dim][str(name)] = buckets[dim].get(str(name), 0) + val
        else:
            # Forma antigua: values[-1].value como dict {dim: {k: v}}
            vals = m0.get("values") or []
            if vals:
                latest = vals[-1].get("value") or {}
                for k, v in (latest.get(dim) or {}).items():
                    buckets[dim][str(k)] = buckets[dim].get(str(k), 0) + int(v or 0)

    # persistir
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
    print("→ IG: Ingesta de publicaciones")
    ingest_media()
    print("→ IG: Cuenta semanal")
    ingest_account_weekly()
    print("→ IG: Audiencia semanal")
    ingest_audience_segments_weekly()
    print("✔ IG listo")


if __name__ == "__main__":
    main()
