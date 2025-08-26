# ig_ingest.py
import os
from datetime import datetime, timezone, date
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

PLATAFORMA  = "instagram"
PG_URL      = os.getenv("PG_URL")
IG_USER_ID  = os.getenv("IG_USER_ID")   # si no está, se resuelve desde PAGE_ID
PAGE_ID     = os.getenv("PAGE_ID")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_IG")  # token específico para IG

# ---------- helpers de token ----------
# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN_IG")

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
#-------------------------------------------
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
    fields = ",".join([
        "id","caption","media_type","media_url","permalink","timestamp",
        "thumbnail_url","like_count","comments_count",
        "children{media_type,media_url,permalink,timestamp,id}"
    ])
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
    # muchos media dan 400 si se pide este metric
    try:
        vjs = ig_get(f"{media_id}/insights", {"metric": "video_views"})
        for m in vjs.get("data", []):
            vals = m.get("values", [])
            if vals:
                out["video_views"] = int(vals[-1].get("value") or 0)
    except RuntimeError:
        # si no aplica (imagen/carrusel), no es error
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
            media_id   = m["id"]
            created_at = m.get("timestamp")
            dia        = iso_date_from_any(created_at) if created_at else datetime.now(timezone.utc).date()

            like_count     = int(m.get("like_count") or 0)
            comments_count = int(m.get("comments_count") or 0)
            media_type     = (m.get("media_type") or "").upper()
            permalink      = m.get("permalink")
            media_url      = m.get("media_url")

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
            #    reach y saved OK; NO pedir impressions/plays vía /insights
            try:
                ins = media_insights_lifetime(media_id)  # devuelve reach, saved, video_views (si aplica)
            except Exception as e:
                print(f"[WARN] insights fallaron para media {media_id}: {e}")
                ins = {}

            # 3) Visualizaciones:
            #    Preferir el campo del propio media; si no está, usar lo que traiga ins (si hubo).
            visualizaciones = int(m.get("video_view_count") or ins.get("video_views", 0) or 0)

            metricas = {
                "visualizaciones": visualizaciones,     # videos/reels si el campo existe
                "alcance":         int(ins.get("reach", 0) or 0),
                "impresiones":     0,                   # v22+ ya no disponible por media
                "tiempo_promedio": None,

                "reacciones":      like_count,
                "me_gusta":        like_count,
                "me_encanta":      0,
                "me_divierte":     0,
                "me_asombra":      0,
                "me_entristece":   0,
                "me_enoja":        0,

                "comentarios":     comments_count,
                "compartidos":     0,                   # IG no expone shares por media
                "guardados":       int(ins.get("saved", 0) or 0),
                "clics_enlace":    0,
                "ctr":             None,
            }

            upsert_metricas_publicacion_diaria(con, PLATAFORMA, ig_id, media_id, dia, metricas)
#-----------------------------------------
from datetime import datetime, date, time, timedelta, timezone

def _ts_day(d: date) -> int:
    # IG Graph usa epoch seconds (UTC) para since/until
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

def _get_total_value(js: dict, name: str, default=0):
    # Para metric_type=total_value
    try:
        for m in js.get("data", []):
            if m.get("name") == name:
                vals = m.get("values") or []
                if vals:
                    v = vals[-1].get("value")
                    # a veces viene como dict {"value": N} o directamente N
                    return int(v.get("value")) if isinstance(v, dict) else int(v or 0)
    except Exception:
        pass
    return default

# ---------- CUENTA: serie diaria -> snapshot semanal ----------
from datetime import datetime, date, timedelta, timezone

def _ts_day(d: date) -> int:
    return int(datetime(d.year, d.month, d.day, tzinfo=timezone.utc).timestamp())

def ingest_account_weekly():
    ig_id = ensure_ig_user_id()

    # Tomamos ~60 días hacia atrás pero consultando en chunks <=30 días
    hoy   = date.today()
    desde = hoy - timedelta(days=60)

    def chunks_30d(start: date, end: date):
        cur = start
        while cur < end:
            nxt = min(cur + timedelta(days=29), end)  # 29 días (<=30 límite estricto entre since/until)
            yield cur, nxt
            cur = nxt + timedelta(days=1)

    per_day = {}

    for c_desde, c_hasta in chunks_30d(desde, hoy):
        base_range = {
            "period": "day",
            "since": _ts_day(c_desde),
            "until": _ts_day(c_hasta + timedelta(days=1)),  # hasta mañana 00:00 UTC del chunk
        }

        # 1) reach + follower_count (SIN metric_type)
        try:
            js_daily = ig_get(f"{ig_id}/insights", {
                **base_range,
                "metric": "reach,follower_count",
            })
            for m in js_daily.get("data", []):
                name = m.get("name")
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "profile_views": 0, "follower_count": 0})
                    try:
                        per_day[end][name] = int(v.get("value") or 0)
                    except Exception:
                        per_day[end][name] = 0
        except RuntimeError as e:
            warn_once(f"[WARN] insights diarios (reach,follower_count) fallaron: {e}")

        # 2) profile_views (REQUiere metric_type=total_value)
        try:
            js_tv = ig_get(f"{ig_id}/insights", {
                **base_range,
                "metric": "profile_views",
                "metric_type": "total_value",
            })
            for m in js_tv.get("data", []):
                for v in m.get("values", []):
                    end = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
                    per_day.setdefault(end, {"reach": 0, "profile_views": 0, "follower_count": 0})
                    raw = v.get("value")
                    val = 0
                    try:
                        val = int(raw.get("value")) if isinstance(raw, dict) else int(raw or 0)
                    except Exception:
                        val = 0
                    per_day[end]["profile_views"] = val
        except RuntimeError as e:
            warn_once(f"[WARN] insights total_value (profile_views) fallaron: {e}")

    # 3) último punto de cada semana ISO
    by_iso_week = {}
    for d, vals in per_day.items():
        y, w, _ = d.isocalendar()
        key = (y, w)
        if key not in by_iso_week or d >= by_iso_week[key]["fecha"]:
            by_iso_week[key] = {"fecha": d, "data": vals}

    # 4) Persistir
    with conn() as con:
        for entry in by_iso_week.values():
            fecha = entry["fecha"]
            vals  = entry["data"]
            fila = {
                "fecha_corte": fecha,
                "impresiones": 0,                                # no disponible a nivel cuenta v22+
                "alcance":     int(vals.get("reach", 0) or 0),
                "video_views": 0,                                # no disponible a nivel cuenta
                "fans_total":  int(vals.get("follower_count", 0) or 0),
                # Si quieres guardar profile_views semanales en tu tabla, añade el/los campos
                # "profile_views": int(vals.get("profile_views", 0) or 0),
            }
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ig_id, fila)

# ---------- AUDIENCIA: lifetime -> snapshot semanal ----------
def safe_ig_user_insights(metrics, period, metric_type=None):
    params = {"metric": ",".join(metrics), "period": period}
    if metric_type:
        params["metric_type"] = metric_type
    try:
        return ig_get(f"{ensure_ig_user_id()}/insights", params)
    except RuntimeError as e:
        warn_once(f"[WARN] IG insights error: {metrics} ({period}). {e}")
        return {"data": []}

def ingest_audience_segments_weekly():
    """
    Los antiguos audience_city / audience_country / audience_gender_age
    fueron reemplazados por métricas *demographics*:
    - follower_demographics (lo más cercano a tus 'audience_*' previos)
    - reached_audience_demographics (alcance)
    - engaged_audience_demographics (engagement)
    Todas son 'lifetime' y requieren metric_type=total_value.
    """
    # Usa follower_demographics para reemplazar audiencia base
    js_demo = safe_ig_user_insights(
        ["follower_demographics"], "lifetime", metric_type="total_value"
    )

    # Extrae el último punto y sus breakdowns (si existen)
    latest = {}
    for m in js_demo.get("data", []):
        vals = m.get("values") or []
        if vals:
            latest = vals[-1].get("value") or {}
            # "value" debería ser un dict con keys como "city", "country", "gender", "age" o "gender_age"
            break

    # Normalizamos posibles nombres
    cities   = latest.get("city")   or {}
    countries= latest.get("country")or {}
    # Algunas cuentas devuelven 'gender_age', otras 'age_gender'
    ga_raw   = latest.get("gender_age") or latest.get("age_gender") or {}

    # Persistimos como “snapshot semanal” usando la semana de hoy
    ig_id = ensure_ig_user_id()
    fecha = date.today()  # si prefieres alinear a la semana ISO como arriba, puedes calcular el último día de la semana

    with conn() as con:
        # Ciudad
        for k, qty in cities.items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, ciudad=k, cantidad=int(qty or 0))
        # País
        for k, qty in countries.items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, pais=k, cantidad=int(qty or 0))
        # Género-edad (ej. "M.25-34")
        for k, qty in ga_raw.items():
            insert_segmento_semanal(con, PLATAFORMA, ig_id, fecha, genero=k, cantidad=int(qty or 0))

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
