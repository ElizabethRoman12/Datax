import os
from datetime import datetime, date, timedelta, timezone
import psycopg2
from urllib.parse import quote

from dotenv import load_dotenv
from linkedin_api import li_get, paginate_elements, LIError
from fb_sql import (
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

load_dotenv()
PLATAFORMA = "linkedin"
PG_URL = os.getenv("PG_URL")
ORG_ID = os.getenv("LI_ORG_ID")
ORG_URN = f"urn:li:organization:{ORG_ID}" if ORG_ID else None

def conn():
    if not PG_URL:
        raise RuntimeError("Falta PG_URL en .env")
    return psycopg2.connect(PG_URL)

def today():
    return datetime.now(timezone.utc).date()

def _list_param(urn: str) -> str:
    return f"List({quote(urn, safe='')})"

# ---------- PUBLICACIONES (Community) ----------
def iter_posts_since(year_start: date):
    """
    Devuelve posts normalizados desde 1/enero.
    Intenta /rest/posts (partner) → /rest/ugcPosts → /rest/shares usando kind='cm'.
    """
    # 1) posts (partner; puede 403)
    try:
        params = {"q": "author", "author": ORG_URN, "sort": "LAST_MODIFIED"}
        for p in paginate_elements("posts", params, count=50, kind="cm"):
            created_ms = (p.get("createdAt") or {}).get("time") \
                         or (p.get("lastModifiedAt") or {}).get("time")
            if not created_ms:
                continue
            d = datetime.fromtimestamp(int(created_ms)/1000, tz=timezone.utc).date()
            if d < year_start:
                continue
            yield {
                "id": p.get("urn") or p.get("id"),
                "created_ms": int(created_ms),
                "text": (p.get("commentary") or {}).get("text"),
                "permalink": (p.get("permalinks") or [None])[0]
                              if isinstance(p.get("permalinks"), list) else p.get("permalink"),
                "media_type": ((p.get("content") or {}).get("media") or {}).get("type") or "POST",
                "activity_urn": p.get("associatedUrn") or p.get("activity") or (p.get("urn") or ""),
            }
        return
    except Exception:
        pass

    # 2) ugcPosts
    try:
        params = {"q": "authors", "authors": _list_param(ORG_URN), "sort": "LAST_MODIFIED"} # type: ignore
        for p in paginate_elements("ugcPosts", params, count=50, kind="cm"):
            created_ms = (p.get("created") or {}).get("time") \
                         or (p.get("lastModifiedAt") or {}).get("time")
            if not created_ms:
                continue
            d = datetime.fromtimestamp(int(created_ms)/1000, tz=timezone.utc).date()
            if d < year_start:
                continue
            sc = (p.get("specificContent") or {}).get("com.linkedin.ugc.ShareContent") or {}
            text = (sc.get("shareCommentary") or {}).get("text")
            media_type = "POST"
            media = (sc.get("media") or [])
            if media:
                media_type = (media[0].get("status") or "POST").upper()
            act = p.get("lifecycleActivity") or p.get("activity") or p.get("urn") or ""
            yield {
                "id": p.get("id") or p.get("urn"),
                "created_ms": int(created_ms),
                "text": text,
                "permalink": None,
                "media_type": media_type,
                "activity_urn": act,
            }
        return
    except Exception:
        pass

    # 3) shares
    params = {"q": "owners", "owners": _list_param(ORG_URN), "sort": "LAST_MODIFIED"} # type: ignore
    for p in paginate_elements("shares", params, count=50, kind="cm"):
        created_ms = (p.get("created") or {}).get("time") \
                     or (p.get("lastModified") or {}).get("time")
        if not created_ms:
            continue
        d = datetime.fromtimestamp(int(created_ms)/1000, tz=timezone.utc).date()
        if d < year_start:
            continue
        text = (p.get("text") or {}).get("text")
        permalink = p.get("permalink")
        media_type = "POST"
        act = p.get("activity") or ""
        if not act:
            sid = p.get("id") or ""
            if sid and str(sid).isdigit():
                act = f"urn:li:activity:{sid}"
        yield {
            "id": p.get("urn") or p.get("id"),
            "created_ms": int(created_ms),
            "text": text,
            "permalink": permalink,
            "media_type": media_type,
            "activity_urn": act,
        }

def social_counts(activity_urn: str) -> dict:
    encoded = activity_urn.replace(":", "%3A")
    js = li_get(f"socialActions/{encoded}", kind="cm")
    likes = (js.get("likesSummary") or {}).get("totalLikes") or 0
    comments = (js.get("commentsSummary") or {}).get("totalFirstLevelComments") or 0
    shares = js.get("totalShareStatistics") or js.get("shares") or 0
    return {"likes": int(likes), "comments": int(comments), "shares": int(shares)}

def ingest_posts_and_metrics():
    year_start = date(datetime.now().year, 1, 1)
    with conn() as con:
        for p in iter_posts_since(year_start):
            created_iso = datetime.fromtimestamp(p["created_ms"]/1000, tz=timezone.utc).isoformat()
            dia_pub = datetime.fromtimestamp(p["created_ms"]/1000, tz=timezone.utc).date()

            publicacion_row = {
                "id": p["id"],
                "created_time": created_iso,
                "message": p.get("text"),
                "permalink_url": p.get("permalink"),
                "status_type": (p.get("media_type") or "POST").upper(),
                "attachments": {"media_type": p.get("media_type"), "unshimmed_url": None},
                "shares": {"count": 0},
                "comments": {"summary": {"total_count": 0}},
                "reactions": {"summary": {"total_count": 0}},
            }
            upsert_publicacion(con, PLATAFORMA, ORG_URN, publicacion_row)

            # socialActions si se pudo resolver activity_urn
            likes = comments = shares = 0
            act_urn = p.get("activity_urn") or ""
            if act_urn:
                try:
                    c = social_counts(act_urn)
                    likes, comments, shares = c["likes"], c["comments"], c["shares"]
                except Exception:
                    pass

            metricas = {
                "visualizaciones": 0, "alcance": 0, "impresiones": 0, "tiempo_promedio": None,
                "reacciones": likes, "me_gusta": likes, "me_encanta": 0, "me_divierte": 0,
                "me_asombra": 0, "me_entristece": 0, "me_enoja": 0,
                "comentarios": comments, "compartidos": shares,
                "guardados": 0, "clics_enlace": 0, "ctr": None,
            }
            upsert_metricas_publicacion_diaria(con, PLATAFORMA, ORG_URN, p["id"], dia_pub, metricas)

# ---------- ESTADÍSTICA DE CUENTA (preferir Pages; fallback Community) ----------
def _best_kind_for_followers():
    # si tienes token de Pages úsalo; si no, usa Community
    return "pages" if os.getenv("LI_PAGES_ACCESS_TOKEN") else "cm"

def ingest_account_weekly():
    end = today()
    start = end - timedelta(days=60)
    kind = _best_kind_for_followers()
    params = {
        "q": "organizationalEntity",
        "organizationalEntity": ORG_URN,
        "timeIntervals.timeGranularityType": "DAY",
        "timeIntervals.timeRange.start": int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()*1000),
        "timeIntervals.timeRange.end": int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp()*1000),
    }
    js = li_get("organizationFollowerStatistics", params, kind=kind)

    by_day = {}
    for e in js.get("elements") or []:
        tr = e.get("timeRange") or {}
        end_ms = tr.get("end")
        if not end_ms:
            continue
        d = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()
        val = (e.get("followerCounts") or {}).get("organicFollowerCount")
        if val is None:
            val = (e.get("followerGains") or {}).get("organicFollowerGain")
        by_day[d] = int(val or 0)

    by_week = {}
    for d, val in by_day.items():
        y, w, _ = d.isocalendar()
        if (y, w) not in by_week or d >= by_week[(y, w)]["fecha"]:
            by_week[(y, w)] = {"fecha": d, "valor": val}

    with conn() as con:
        for _, v in by_week.items():
            fila = {"fecha_corte": v["fecha"], "impresiones": 0, "alcance": 0, "video_views": 0, "fans_total": v["valor"]}
            upsert_estadistica_pagina_semanal(con, PLATAFORMA, ORG_URN, fila)

# ---------- AUDIENCIA / SEGMENTOS (preferir Pages) ----------
def ingest_audience_segments_weekly():
    end = today()
    start = end - timedelta(days=60)
    kind = "pages" if os.getenv("LI_PAGES_ACCESS_TOKEN") else "cm"
    params = {
        "q": "organizationalEntity",
        "organizationalEntity": ORG_URN,
        "timeIntervals.timeGranularityType": "DAY",
        "timeIntervals.timeRange.start": int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()*1000),
        "timeIntervals.timeRange.end": int(datetime(end.year, end.month, end.day, tzinfo=timezone.utc).timestamp()*1000),
        "aggregation": "COUNTRY",
    }
    js = li_get("organizationFollowerStatistics", params, kind=kind)

    latest = {}
    for e in js.get("elements") or []:
        country = (e.get("followerCountsByCountry") or {}).get("country") or e.get("country")
        if not country:
            country = (e.get("aggregations") or {}).get("country")
        tr = e.get("timeRange") or {}
        end_ms = tr.get("end")
        if not country or not end_ms:
            continue
        d = datetime.fromtimestamp(end_ms/1000, tz=timezone.utc).date()
        val = (e.get("followerCountsByCountry") or {}).get("followerCount") \
              or (e.get("followerCounts") or {}).get("organicFollowerCount") or 0
        cur = latest.get(country)
        if not cur or d >= cur["fecha"]:
            latest[country] = {"fecha": d, "valor": int(val)}

    with conn() as con:
        fref = today()
        for country, obj in latest.items():
            insert_segmento_semanal(con, PLATAFORMA, ORG_URN, fref,
                                    pais=str(country).upper(), cantidad=obj["valor"])

# ---------- MAIN ----------
def main():
    if not ORG_URN:
        raise RuntimeError("Falta LI_ORG_ID en .env")

    print("→ LI: Ingesta de publicaciones (requiere token Community)")
    try:
        ingest_posts_and_metrics()
    except LIError as e:
        if "ACCESS_DENIED" in str(e) or "permissions" in str(e):
            print("[WARN] Publicaciones: sin permisos/token Community. Continuo con seguidores y audiencia.")
        else:
            raise

    print("→ LI: Cuenta semanal")
    try:
        ingest_account_weekly()
    except LIError as e:
        print(f"[WARN] Seguidores: {e}")

    print("→ LI: Audiencia semanal")
    try:
        ingest_audience_segments_weekly()
    except LIError as e:
        print(f"[WARN] Audiencia: {e}")

    print("✔ LinkedIn listo")

if __name__ == "__main__":
    main()
