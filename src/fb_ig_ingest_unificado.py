
import os
import sys
import time
import psycopg2
import requests
import logging
from datetime import datetime, date, timedelta, timezone
from typing import Dict, Iterable, Optional
from pathlib import Path
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# CONFIGURACIÓN Y LOGGING
load_dotenv(override=True, encoding="utf-8")

PG_URL = os.getenv("PG_URL")
GRAPH_URL = os.getenv("GRAPH_URL", "https://graph.facebook.com/v19.0")
FB_GRAPH = "https://graph.facebook.com/v19.0"

if not PG_URL:
    raise RuntimeError("Falta PG_URL en .env")

# Configura logging a archivo y consola
os.makedirs("logs", exist_ok=True)
LOG_FILE = "logs/ingesta.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# CONEXIÓN SQLALCHEMY
engine = create_engine(PG_URL, pool_pre_ping=True)

def conn():
    return psycopg2.connect(PG_URL)

# TOKENS Y REFRESH
def obtener_token(plataforma: str):
    with conn() as c:
        cur = c.cursor()
        cur.execute(
            "SELECT token_acceso, token_refresh, expira_en FROM tokens_redes WHERE plataforma=%s",
            (plataforma,),
        )
        fila = cur.fetchone()
    if not fila:
        raise RuntimeError(f"No se encontró token para {plataforma}")
    return {
        "token_acceso": fila[0],
        "token_refresh": fila[1],
        "expira_en": fila[2],
    }

def obtener_token_pagina(token_usuario: str, page_id: str) -> str:
    url = f"{FB_GRAPH}/me/accounts"
    params = {"access_token": token_usuario}
    r = requests.get(url, params=params)
    r.raise_for_status()
    data = r.json().get("data", [])
    for page in data:
        if page["id"] == page_id:
            return page["access_token"]
    raise RuntimeError(f"No encontré la página {page_id} en /me/accounts")

def actualizar_token(plataforma: str, token_acceso: str, token_refresh=None, expira_en=None):
    with conn() as c:
        cur = c.cursor()
        cur.execute(
            """
            UPDATE tokens_redes
            SET token_acceso=%s, token_refresh=%s, expira_en=%s, actualizado_en=now()
            WHERE plataforma=%s
            """,
            (token_acceso, token_refresh, expira_en, plataforma),
        )
        c.commit()

def renovar_facebook_instagram():
    app_id = os.getenv("FB_APP_ID")
    app_secret = os.getenv("FB_APP_SECRET")
    token = obtener_token("facebook")

    url = "https://graph.facebook.com/v19.0/oauth/access_token"
    params = {
        "grant_type": "fb_exchange_token",
        "client_id": app_id,
        "client_secret": app_secret,
        "fb_exchange_token": token["token_acceso"],
    }
    res = requests.get(url, params=params).json()
    if "access_token" in res:
        nuevo_token = res["access_token"]
        expira_en = datetime.now() + timedelta(days=60)
        actualizar_token("facebook", nuevo_token, expira_en=expira_en)
        actualizar_token("instagram", nuevo_token, expira_en=expira_en)
        logger.info("Token renovado para Facebook e Instagram")
    else:
        logger.warning(f"Error al renovar token: {res}")

# GRAPH API
def _pick_token(explicit: Optional[str] = None) -> str:
    token = (
        explicit
        or os.getenv("ACCESS_TOKEN")
        or os.getenv("ACCESS_TOKEN_FB")
        or os.getenv("ACCESS_TOKEN_IG")
    )
    if not token:
        raise RuntimeError("Falta ACCESS_TOKEN en entorno")
    return token

def fb_get(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Dict:
    token = _pick_token(access_token)
    params = (params or {}).copy()
    params["access_token"] = token
    url = f"{GRAPH_URL}/{path.lstrip('/')}"
    for attempt in range(5):
        r = requests.get(url, params=params, timeout=60)
        if r.status_code == 200:
            return r.json()
        if r.status_code in (429, 613):
            time.sleep(2 ** attempt)
            continue
        raise RuntimeError(f"FB {r.status_code}: {r.text}")
    raise RuntimeError("Rate limit persistente tras 5 intentos")

def paginate(path: str, params: Dict | None = None, access_token: Optional[str] = None) -> Iterable[Dict]:
    data = fb_get(path, params, access_token=access_token)
    while True:
        yield from data.get("data", [])
        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        data = requests.get(next_url, timeout=60).json()

# FUNCIONES SQL
def _exec(conn, sql: str, params: tuple | dict):
    with conn.cursor() as cur:
        cur.execute(sql, params)

def upsert_pagina(conn, pagina: dict):
    sql = """
    INSERT INTO paginas (pagina_id, plataforma, nombre)
    VALUES (%s, %s, %s)
    ON CONFLICT (pagina_id) DO UPDATE
    SET plataforma = EXCLUDED.plataforma,
        nombre     = EXCLUDED.nombre;
    """
    _exec(conn, sql, (pagina["pagina_id"], pagina["plataforma"], pagina["nombre"]))
    conn.commit()
def upsert_publicacion(conn, plataforma: str, pagina_id: str, pub: dict):
    """Inserta o actualiza una publicación en la tabla publicaciones."""
    sql = """
    INSERT INTO publicaciones
      (plataforma, pagina_id, publicacion_id, url_publicacion,
       fecha_hora_publicacion, texto_publicacion, formato)
    VALUES
      (%(plataforma)s, %(pagina_id)s, %(publicacion_id)s, %(url)s,
       %(fecha_hora)s, %(texto)s, %(formato)s)
    ON CONFLICT (publicacion_id) DO UPDATE SET
      url_publicacion = EXCLUDED.url_publicacion,
      texto_publicacion = EXCLUDED.texto_publicacion,
      formato = EXCLUDED.formato,
      fecha_hora_publicacion = EXCLUDED.fecha_hora_publicacion;
    """
    params = {
        "plataforma": plataforma,
        "pagina_id": pagina_id,
        "publicacion_id": pub["id"],
        "url": pub.get("permalink_url"),
        "fecha_hora": pub.get("created_time", "").replace("Z", "+00:00"),
        "texto": pub.get("message"),
        "formato": infer_formato(pub),
    }
    _exec(conn, sql, params)
    conn.commit()


def infer_formato(post: dict) -> str:
    att = (post.get("attachments") or {}).get("data") or [{}]
    media = (att[0] or {}).get("media_type", "") or ""
    st = (post.get("status_type") or "") or ""
    m, s = media.lower(), st.lower()
    if "video" in (m or s):
        return "video"
    if "photo" in m or "image" in m:
        return "imagen"
    if "album" in m:
        return "carrusel"
    if "link" in m or "shared_story" in s:
        return "link"
    return s or "desconocido"

# MÉTRICAS, REACCIONES Y VARIACIONES
def _ultimo_registro_prev(conn, plataforma, pagina_id, publicacion_id, fecha_descarga):
    q = """
      SELECT visualizaciones, alcance, comentarios, compartidos, guardados
      FROM metricas_publicaciones_diarias
      WHERE plataforma=%s AND pagina_id=%s AND publicacion_id=%s AND fecha_descarga < %s
      ORDER BY fecha_descarga DESC
      LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q, (plataforma, pagina_id, publicacion_id, fecha_descarga))
        return cur.fetchone()

def upsert_metricas_publicacion_diaria(conn, plataforma, pagina_id, publicacion_id, fecha_descarga: date, m: dict):
    prev = _ultimo_registro_prev(conn, plataforma, pagina_id, publicacion_id, fecha_descarga)
    sql = """
    INSERT INTO metricas_publicaciones_diarias
      (plataforma, pagina_id, publicacion_id, fecha_descarga,
       visualizaciones, alcance, impresiones, tiempo_promedio_seg,
       comentarios, compartidos, guardados,
       clics_enlace, ctr,
       delta_visualizaciones, delta_alcance, delta_comentarios, delta_compartidos, delta_guardados)
    VALUES
      (%(plataforma)s, %(pagina_id)s, %(publicacion_id)s, %(fecha_descarga)s,
       %(visualizaciones)s, %(alcance)s, %(impresiones)s, %(tiempo_promedio)s,
       %(comentarios)s, %(compartidos)s, %(guardados)s,
       %(clics)s, %(ctr)s,
       %(d_vis)s, %(d_alc)s, %(d_com)s, %(d_comp)s, %(d_guard)s)
    ON CONFLICT (plataforma, pagina_id, publicacion_id, fecha_descarga) DO UPDATE SET
       visualizaciones = EXCLUDED.visualizaciones,
       alcance         = EXCLUDED.alcance,
       impresiones     = EXCLUDED.impresiones,
       tiempo_promedio_seg = EXCLUDED.tiempo_promedio_seg,
       comentarios     = EXCLUDED.comentarios,
       compartidos     = EXCLUDED.compartidos,
       guardados       = EXCLUDED.guardados,
       clics_enlace    = EXCLUDED.clics_enlace,
       ctr             = EXCLUDED.ctr,
       delta_visualizaciones = EXCLUDED.delta_visualizaciones,
       delta_alcance         = EXCLUDED.delta_alcance,
       delta_comentarios     = EXCLUDED.delta_comentarios,
       delta_compartidos     = EXCLUDED.delta_compartidos,
       delta_guardados       = EXCLUDED.delta_guardados;
    """
    d = lambda k, i: (m.get(k, 0) - (prev[i] if prev else 0))
    row = {
        "plataforma": plataforma,
        "pagina_id": pagina_id,
        "publicacion_id": publicacion_id,
        "fecha_descarga": fecha_descarga,
        "visualizaciones": m.get("visualizaciones", 0),
        "alcance": m.get("alcance", 0),
        "impresiones": m.get("impresiones", 0),
        "tiempo_promedio": m.get("tiempo_promedio"),
        "comentarios": m.get("comentarios", 0),
        "compartidos": m.get("compartidos", 0),
        "guardados": m.get("guardados", 0),
        "clics": m.get("clics_enlace", 0),
        "ctr": m.get("ctr"),
        "d_vis": d("visualizaciones", 0),
        "d_alc": d("alcance", 1),
        "d_com": d("comentarios", 2),
        "d_comp": d("compartidos", 3),
        "d_guard": d("guardados", 4),
    }
    _exec(conn, sql, row)
    conn.commit()

def upsert_reaccion_publicacion_diaria(conn, plataforma, pagina_id, publicacion_id, fecha_descarga: date, tipo_reaccion_id: int, cantidad: int):
    sql = """
    INSERT INTO reacciones_publicacion_diaria
      (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id, cantidad)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id) DO UPDATE SET
      cantidad = EXCLUDED.cantidad;
    """
    _exec(conn, sql, (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id, cantidad))

def upsert_estadistica_pagina_semanal(conn, plataforma, pagina_id, fila: dict):
    sql = """
    INSERT INTO estadisticas_pagina_semanal
      (plataforma, pagina_id, fecha_corte_semana, total_seguidores, alcance_pagina, visualizaciones_pagina)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (plataforma, pagina_id, fecha_corte_semana) DO UPDATE SET
      total_seguidores = EXCLUDED.total_seguidores,
      alcance_pagina = EXCLUDED.alcance_pagina,
      visualizaciones_pagina = EXCLUDED.visualizaciones_pagina;
    """
    _exec(conn, sql, (
        plataforma, pagina_id, fila["fecha_corte"],
        fila.get("fans_total", 0), fila.get("alcance", 0), fila.get("impresiones", 0)
    ))

def insert_segmento_semanal(conn, plataforma, pagina_id, fecha_corte, genero=None, pais=None, ciudad=None, nivel_edu=None, cantidad=0):
    sql = """
    INSERT INTO segmentacion_seguidores_semanal
      (plataforma, pagina_id, fecha_corte_semana, genero, pais, ciudad, nivel_educacion, cantidad_seguidores)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    """
    _exec(conn, sql, (plataforma, pagina_id, fecha_corte, genero, pais, ciudad, nivel_edu, cantidad))

# CALCULAR VARIACIONES
def calcular_variaciones():
    sql = """
    WITH m AS (
      SELECT
        plataforma,
        pagina_id,
        publicacion_id,
        fecha_descarga,
        visualizaciones,
        alcance,
        comentarios,
        compartidos,
        guardados,
        LAG(visualizaciones) OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_visualizaciones,
        LAG(alcance)         OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_alcance,
        LAG(comentarios)     OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_comentarios,
        LAG(compartidos)     OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_compartidos,
        LAG(guardados)       OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_guardados
      FROM metricas_publicaciones_diarias
    )
    UPDATE metricas_publicaciones_diarias d
    SET
      delta_visualizaciones = COALESCE(m.visualizaciones - m.prev_visualizaciones, 0),
      delta_alcance         = COALESCE(m.alcance - m.prev_alcance, 0),
      delta_comentarios     = COALESCE(m.comentarios - m.prev_comentarios, 0),
      delta_compartidos     = COALESCE(m.compartidos - m.prev_compartidos, 0),
      delta_guardados       = COALESCE(m.guardados - m.prev_guardados, 0)
    FROM m
    WHERE d.plataforma = m.plataforma
      AND d.pagina_id = m.pagina_id
      AND d.publicacion_id = m.publicacion_id
      AND d.fecha_descarga = m.fecha_descarga;
    """
    try:
        with engine.begin() as conn:
            conn.execute(text(sql))
        logger.info("Variaciones actualizadas correctamente en metricas_publicaciones_diarias")
    except Exception as e:
        logger.error(f"Error al calcular variaciones: {e}")

# FUNCIONES DE FACEBOOK
def ejecutar_ingesta_facebook(inicio: date, fin: date):
    """Ingesta completa de página, publicaciones, métricas y audiencia de Facebook."""
    PLATAFORMA = "facebook"
    FB_PAGE_ID = os.getenv("FB_PAGE_ID")
    token_usuario = obtener_token(PLATAFORMA)["token_acceso"]
    token_facebook = obtener_token_pagina(token_usuario, FB_PAGE_ID)

    def fb_get_fb(path, params=None):
        return fb_get(path, params or {}, access_token=token_facebook)

    def fb_paginate(path, params=None):
        return paginate(path, params or {}, access_token=token_facebook)

    def parse_insights(js, metrics_map):
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

    # Página base
    js = fb_get_fb(FB_PAGE_ID, {"fields": "id,name"})
    with conn() as con:
        upsert_pagina(con, {
            "pagina_id": str(js["id"]),
            "plataforma": PLATAFORMA,
            "nombre": js.get("name", "Página sin nombre")
        })

    # Publicaciones
    def get_posts_por_rango(inicio: date, fin: date):
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
        mapping = {
            "LIKE": "me_gusta",
            "LOVE": "me_encanta",
            "HAHA": "me_divierte",
            "WOW": "me_asombra",
            "SAD": "me_entristece",
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
        metrics_map = {
            "post_impressions": "impressions",
            "post_impressions_unique": "reach",
            "post_clicks": "clicks",
            "post_video_views": "video_views"
        }
        js = fb_get_fb(f"{post_id}/insights", {"metric": ",".join(metrics_map), "period": "lifetime"})
        return parse_insights(js, metrics_map)

    posts = get_posts_por_rango(inicio, fin)
    if not posts:
        return
    print(f"* {len(posts)} publicaciones encontradas entre {inicio} y {fin}")
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

    print("✔ Facebook listo (publicaciones y métricas procesadas correctamente)")
    ingest_page_metrics_range(inicio, fin)

# MÉTRICAS DE PÁGINA (FACEBOOK)
def ingest_page_metrics_range(inicio: date, fin: date):
    """Inserta métricas de la página dentro del rango (alcance, impresiones, visualizaciones, fans)."""
    PLATAFORMA = "facebook"
    FB_PAGE_ID = os.getenv("FB_PAGE_ID")

    token_usuario = obtener_token(PLATAFORMA)["token_acceso"]
    token_facebook = obtener_token_pagina(token_usuario, FB_PAGE_ID)

    def fb_get_fb(path, params=None):
        return fb_get(path, params or {}, access_token=token_facebook)

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

    by_day = {}
    for m in js.get("data", []):
        name = m.get("name")
        if name not in metrics_map:
            continue
        for v in m.get("values", []):
            fecha = datetime.fromisoformat(v["end_time"].replace("Z", "+00:00")).date()
            by_day.setdefault(fecha, {f: 0 for f in metrics_map.values()})
            by_day[fecha][metrics_map[name]] = int(v.get("value") or 0)

    with conn() as con:
        for fecha, fila in by_day.items():
            if inicio <= fecha <= fin:
                fila["fecha_corte"] = fecha
                upsert_estadistica_pagina_semanal(con, PLATAFORMA, FB_PAGE_ID, fila)

# FUNCIONES DE INSTAGRAM
def ejecutar_ingesta_instagram(inicio: date, fin: date):
    """Ingesta completa de cuenta, publicaciones, métricas y audiencia de Instagram."""
    PLATAFORMA = "instagram"
    IG_USER_ID = os.getenv("IG_USER_ID")
    token_instagram = obtener_token(PLATAFORMA)["token_acceso"]

    def ig_get(path, params=None):
        return fb_get(path, params or {}, access_token=token_instagram)

    def ig_paginate(path, params=None):
        return paginate(path, params or {}, access_token=token_instagram)

    logger.info(f"Iniciando ingesta Instagram del {inicio} al {fin}")

    # CUENTA BASE
    js = ig_get(IG_USER_ID, {"fields": "id,username"})
    with conn() as con:
        upsert_pagina(con, {
            "pagina_id": str(js["id"]),
            "plataforma": PLATAFORMA,
            "nombre": js.get("username", "Cuenta IG")
        })

    # PUBLICACIONES
    def media_insights_lifetime(media_id: str) -> dict:
        """Obtiene métricas lifetime (alcance, guardados, video_views) de una publicación IG."""
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

    # Publicaciones por rango
    fields = ",".join([
        "id","caption","media_type","media_url","permalink","timestamp",
        "thumbnail_url","like_count","comments_count",
        "children{media_type,media_url,permalink,timestamp,id}"
    ])
    publicaciones = []
    for item in ig_paginate(f"{IG_USER_ID}/media", {"fields": fields, "limit": 100}):
        ts = item.get("timestamp")
        if ts:
            fecha_pub = datetime.fromisoformat(ts.replace("Z", "+00:00")).date()
            if inicio <= fecha_pub <= fin:
                publicaciones.append(item)

    if not publicaciones:
        logger.info("⚠ No hay publicaciones IG en el rango.")
        return

    print(f"* {len(publicaciones)} publicaciones encontradas entre {inicio} y {fin}")
    fecha_descarga = date.today()

    with conn() as con:
        for m in publicaciones:
            media_id = str(m["id"])
            fecha_pub = datetime.fromisoformat(m["timestamp"].replace("Z", "+00:00")).date()

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
            upsert_publicacion(con, PLATAFORMA, IG_USER_ID, publicacion)

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
            upsert_metricas_publicacion_diaria(con, PLATAFORMA, IG_USER_ID, media_id, fecha_descarga, metricas)

            with con.cursor() as cur:
                cur.execute("""
                    INSERT INTO tipo_reaccion (plataforma, nombre)
                    VALUES (%s,%s)
                    ON CONFLICT (plataforma, nombre) DO UPDATE SET nombre=EXCLUDED.nombre
                    RETURNING id
                """, (PLATAFORMA, "me_gusta"))
                tipo_id = cur.fetchone()[0]

            upsert_reaccion_publicacion_diaria(
                con, PLATAFORMA, IG_USER_ID, media_id, fecha_descarga, tipo_id, int(m.get("like_count") or 0)
            )

    # MÉTRICAS DE CUENTA Y AUDIENCIA
    def ingest_account_range(inicio: date, fin: date):
        """Inserta métricas diarias de cuenta IG (seguidores y alcance)."""
        per_day = {}
        def fetch_metric(metric, start, end):
            try:
                js = ig_get(f"{IG_USER_ID}/insights", {
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
                upsert_estadistica_pagina_semanal(con, PLATAFORMA, IG_USER_ID, fila)

    def ingest_audience_segments_range(fecha: date):
        """Inserta segmentación de audiencia (género, país, ciudad, edad)."""
        dims = {"city": "ciudad", "country": "pais", "gender": "genero", "age": "genero"}
        def fetch_breakdown(dim):
            return ig_get(f"{IG_USER_ID}/insights", {
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
                    insert_segmento_semanal(con, PLATAFORMA, IG_USER_ID, fecha, **{campo: f"AGE.{k}" if dim=="age" else k}, cantidad=qty)

    ingest_account_range(inicio, fin)
    ingest_audience_segments_range(fin)
    print("✔ Instagram listo (publicaciones, métricas y audiencia procesadas correctamente)")


def run_ingesta(plataforma="ambos", inicio=None, fin=None):
    """Función lista para usar desde Airflow o CLI."""
    inicio = inicio or date.today()
    fin = fin or date.today()

    if plataforma == "facebook":
        ejecutar_ingesta_facebook(inicio, fin)
    elif plataforma == "instagram":
        ejecutar_ingesta_instagram(inicio, fin)
    elif plataforma == "ambos":
        ejecutar_ingesta_facebook(inicio, fin)
        ejecutar_ingesta_instagram(inicio, fin)
    else:
        raise ValueError("Plataforma no reconocida")

    calcular_variaciones()
    logger.info("✔ Ingesta completada correctamente")


if __name__ == "__main__":
    import sys
    args = sys.argv
    modo = args[1].lower() if len(args) >= 2 else "ambos"
    inicio = datetime.strptime(args[2], "%Y-%m-%d").date() if len(args) >= 3 else date.today()
    fin = datetime.strptime(args[3], "%Y-%m-%d").date() if len(args) >= 4 else date.today()
    run_ingesta(plataforma=modo, inicio=inicio, fin=fin)
