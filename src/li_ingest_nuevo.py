import os
from datetime import datetime, date, timezone, timedelta
import psycopg2
from dotenv import load_dotenv
from calc_variaciones import calcular_variaciones

from graph_sql import (
    upsert_pagina,
    upsert_publicacion,
    upsert_metricas_publicacion_diaria,
    upsert_estadistica_pagina_semanal,
    insert_segmento_semanal,
)

from li_api import li_get, li_paginate
from li_sql import upsert_campania, upsert_metricas_campania_diaria

# ================================
# Configuración
# ================================
load_dotenv()

PLATAFORMA = "linkedin"
PG_URL = os.getenv("PG_URL")
LI_ORG_ID = os.getenv("LI_ORG_ID")
LI_AD_ACCOUNT_ID = os.getenv("LI_AD_ACCOUNT_ID")

def conn():
    return psycopg2.connect(PG_URL)

# ================================
# Classic (usando App Ads)
# ================================
def ingest_page_classic():
    try:
        url = "https://api.linkedin.com/rest/organizationPageStatistics"
        params = {"q": "organization", "organization": f"urn:li:organization:{LI_ORG_ID}"}
        data = li_get(url, params=params, mode="ads")

        with conn() as con:
            for e in data.get("elements", []):
                fila = {
                    "fecha_corte": date.today(),
                    "impresiones": e.get("pageStatistics", {}).get("impressionsCount", 0),
                    "alcance": e.get("pageStatistics", {}).get("uniqueImpressionsCount", 0),
                    "video_views": e.get("pageStatistics", {}).get("videoViewCount", 0),
                    "fans_total": e.get("followerCount", 0),
                }
                upsert_estadistica_pagina_semanal(con, PLATAFORMA, LI_ORG_ID, fila)
        print("✔ Página cargada (Classic)")
    except Exception as e:
        print(f"⚠ Error en ingest_page_classic: {e}")

def ingest_followers_dma():
    """
    Inserta total y segmentación de seguidores desde DMA.
    Si no hay dataset (404), cae a Classic y guarda solo el total de seguidores.
    """
    url = "https://api.linkedin.com/rest/dmaOrganizationalPageFollows"
    params = {
        "q": "organizationalPage",
        "organizationalPage": f"urn:li:organization:{LI_ORG_ID}"
    }
    fecha = date.today()

    try:
        data = li_get(url, params=params, mode="dma")
        with conn() as con:
            for e in data.get("elements", []):
                total_followers = e.get("followerCount", 0)

                fila = {
                    "fecha_corte": fecha,
                    "impresiones": 0,
                    "alcance": 0,
                    "video_views": 0,
                    "fans_total": total_followers
                }
                upsert_estadistica_pagina_semanal(con, PLATAFORMA, LI_ORG_ID, fila)

                # Segmentaciones (si existen en DMA)
                for country, qty in (e.get("followerCountsByCountry", {}) or {}).items():
                    insert_segmento_semanal(con, PLATAFORMA, LI_ORG_ID, fecha, pais=country, cantidad=qty)
                for city, qty in (e.get("followerCountsByGeo", {}) or {}).items():
                    insert_segmento_semanal(con, PLATAFORMA, LI_ORG_ID, fecha, ciudad=city, cantidad=qty)
                for seniority, qty in (e.get("followerCountsBySeniority", {}) or {}).items():
                    insert_segmento_semanal(con, PLATAFORMA, LI_ORG_ID, fecha, nivel=seniority, cantidad=qty)

        print("✔ Seguidores cargados desde DMA")

    except RuntimeError as e:
        if "404" in str(e):
            print("⚠ DMA no devuelve dataset de seguidores → usando Classic solo para total.")
            try:
                url_classic = "https://api.linkedin.com/rest/organizationPageStatistics"
                params_classic = {
                    "q": "organization",
                    "organization": f"urn:li:organization:{LI_ORG_ID}"
                }
                data_classic = li_get(url_classic, params=params_classic, mode="ads")

                with conn() as con:
                    for e in data_classic.get("elements", []):
                        total_followers = e.get("followerCounts", {}).get("organicFollowerCount", 0) + \
                                          e.get("followerCounts", {}).get("paidFollowerCount", 0)

                        fila = {
                            "fecha_corte": fecha,
                            "impresiones": 0,
                            "alcance": 0,
                            "video_views": 0,
                            "fans_total": total_followers
                        }
                        upsert_estadistica_pagina_semanal(con, PLATAFORMA, LI_ORG_ID, fila)

                print("✔ Seguidores cargados desde Classic (solo total)")
            except Exception as e2:
                print(f"⚠ Error al cargar seguidores desde Classic: {e2}")
        else:
            print(f"⚠ Error en ingest_followers_dma: {e}")

# def ingest_posts_classic():
#     """
#     Descarga métricas de publicaciones desde la Classic API (/rest/organizationalEntityShareStatistics).
#     """
#     try:
#         url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics"
#         params = {
#             "q": "organizationalEntity",
#             "organizationalEntity": f"urn:li:organization:{LI_ORG_ID}"
#         }

#         data = li_get(url, params=params, mode="ads")
#         count_ok, count_skip = 0, 0

#         with conn() as con:
#             for e in data.get("elements", []):
#                 share_urn = e.get("share")
#                 if not share_urn:
#                     print("⚠ Publicación descartada por falta de ID:", e)
#                     count_skip += 1
#                     continue

#                 stats = e.get("totalShareStatistics", {})

#                 created_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

#                 publicacion = {
#                     "id": share_urn,
#                     "created_time": created_time,
#                     "message": None,
#                     "permalink_url": None,
#                     "status_type": None,
#                     "attachments": {},
#                     "shares": {"count": stats.get("shareCount", 0)},
#                     "comments": {"summary": {"total_count": stats.get("commentCount", 0)}},
#                     "reactions": {"summary": {"total_count": stats.get("likeCount", 0)}},
#                 }

#                 upsert_publicacion(con, PLATAFORMA, LI_ORG_ID, publicacion)

#                 dia = date.today()
#                 metricas = {
#                     "visualizaciones": stats.get("impressionCount", 0),
#                     "alcance": stats.get("uniqueImpressionsCount", 0),
#                     "impresiones": stats.get("impressionCount", 0),
#                     "tiempo_promedio": None,
#                     "comentarios": stats.get("commentCount", 0),
#                     "compartidos": stats.get("shareCount", 0),
#                     "guardados": 0,
#                     "clics_enlace": stats.get("clickCount", 0),
#                     "ctr": None,
#                 }
#                 upsert_metricas_publicacion_diaria(con, PLATAFORMA, LI_ORG_ID, share_urn, dia, metricas)
#                 count_ok += 1

#         print(f"✔ Publicaciones cargadas (Métricas Classic) → {count_ok} insertadas, {count_skip} descartadas")
#     except Exception as e:
#         print(f"⚠ Error en ingest_posts_classic: {e}")


# def ingest_posts_classic():
#     """
#     Descarga métricas de publicaciones desde la Classic API (/rest/organizationalEntityShareStatistics).
#     """
#     try:
#         url = "https://api.linkedin.com/rest/organizationalEntityShareStatistics"
#         params = {
#             "q": "organizationalEntity",
#             "organizationalEntity": f"urn:li:organization:{LI_ORG_ID}"
#         }

#         data = li_get(url, params=params, mode="ads")
#         count_ok, count_skip = 0, 0

#         with conn() as con:
#             for e in data.get("elements", []):
#                 share_urn = e.get("share")

#                 # Si no existe un ID real, generamos uno sintético
#                 if not share_urn:
#                     ts = e.get("lastModified", {}).get("time")
#                     if ts:
#                         share_urn = f"urn:li:syntheticPost:{ts}"
#                     else:
#                         share_urn = f"urn:li:syntheticPost:{int(datetime.now().timestamp())}"

#                 stats = e.get("totalShareStatistics", {})

#                 created_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

#                 publicacion = {
#                     "id": share_urn,
#                     "created_time": created_time,
#                     "message": None,
#                     "permalink_url": None,
#                     "status_type": None,
#                     "attachments": {},
#                     "shares": {"count": stats.get("shareCount", 0)},
#                     "comments": {"summary": {"total_count": stats.get("commentCount", 0)}},
#                     "reactions": {"summary": {"total_count": stats.get("likeCount", 0)}},
#                 }

#                 upsert_publicacion(con, PLATAFORMA, LI_ORG_ID, publicacion)

#                 dia = date.today()
#                 metricas = {
#                     "visualizaciones": stats.get("impressionCount", 0),
#                     "alcance": stats.get("uniqueImpressionsCount", 0),
#                     "impresiones": stats.get("impressionCount", 0),
#                     "tiempo_promedio": None,
#                     "comentarios": stats.get("commentCount", 0),
#                     "compartidos": stats.get("shareCount", 0),
#                     "guardados": 0,
#                     "clics_enlace": stats.get("clickCount", 0),
#                     "ctr": None,
#                 }
#                 upsert_metricas_publicacion_diaria(con, PLATAFORMA, LI_ORG_ID, share_urn, dia, metricas)
#                 count_ok += 1

#         print(f"✔ Publicaciones cargadas (Métricas Classic) → {count_ok} insertadas, {count_skip} descartadas")
#     except Exception as e:
#         print(f"⚠ Error en ingest_posts_classic: {e}")




def ingest_posts_ads():
    """
    Descarga publicaciones (contenido) desde la API UGC (/v2/ugcPosts).
    Inserta cada publicación en la tabla publicaciones.
    """
    try:
        url = "https://api.linkedin.com/v2/ugcPosts"
        params = {
            "q": "authors",
            "authors": f"List(urn:li:organization:{LI_ORG_ID})",
            "count": 50
        }

        data = li_get(url, params=params, mode="ads")
        count_ok, count_skip = 0, 0

        with conn() as con:
            for e in data.get("elements", []):
                post_id = e.get("id")
                if not post_id:
                    print("⚠ Publicación descartada sin ID:", e)
                    count_skip += 1
                    continue

                # Fecha de creación
                created_time = None
                if e.get("created", {}).get("time"):
                    try:
                        created_time = datetime.fromtimestamp(
                            int(e["created"]["time"]) / 1000, tz=timezone.utc
                        ).isoformat().replace("+00:00", "Z")
                    except Exception:
                        created_time = None
                if not created_time:
                    created_time = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

                # Texto del post
                text = e.get("specificContent", {}) \
                        .get("com.linkedin.ugc.ShareContent", {}) \
                        .get("shareCommentary", {}) \
                        .get("text")

                # URL del post (se puede construir con el URN)
                permalink = None
                if "urn:li:share:" in post_id:
                    permalink = f"https://www.linkedin.com/feed/update/{post_id}"

                # Formato (texto, imagen, video, etc.)
                media = e.get("specificContent", {}) \
                         .get("com.linkedin.ugc.ShareContent", {}) \
                         .get("shareMediaCategory", "desconocido")

                publicacion = {
                    "id": post_id,
                    "created_time": created_time,
                    "message": text,
                    "permalink_url": permalink,
                    "status_type": None,
                    "attachments": e.get("specificContent", {}),
                    "shares": {"count": 0},   # No disponible aquí
                    "comments": {"summary": {"total_count": 0}},
                    "reactions": {"summary": {"total_count": 0}},
                }

                upsert_publicacion(con, PLATAFORMA, LI_ORG_ID, publicacion)
                count_ok += 1

        print(f"✔ Publicaciones cargadas (UGC Ads) → {count_ok} insertadas, {count_skip} descartadas")

    except Exception as e:
        print(f"⚠ Error en ingest_posts_ads: {e}")




# ================================
# DMA (solo complemento)
# ================================
def ingest_posts_dma():
    try:
        url = "https://api.linkedin.com/rest/dmaOrganizationalPageContentAnalytics"
        params = {
            "q": "organizationalPage",
            "organizationalPage": f"urn:li:organization:{LI_ORG_ID}",
            "finder": "postGestures"
        }
        data = li_get(url, params=params, mode="dma")
        if not data.get("elements"):
            print("⚠ DMA no devolvió publicaciones (puede ser limitación de permisos o dataset).")
        else:
            print(f"✔ Publicaciones DMA: {len(data['elements'])} registros")
    except RuntimeError as e:
        print(f"⚠ DMA aún no disponible para publicaciones: {e}")

# ================================
# Advertising
# ================================
def ingest_campaigns_ads():
    try:
        url = "https://api.linkedin.com/rest/adCampaignsV2"
        params = {
            "q": "search",
            "search.account": f"urn:li:sponsoredAccount:{LI_AD_ACCOUNT_ID}"
        }
        count = 0
        for c in li_paginate(url, params=params, mode="ads"):
            camp_id = str(c.get("id"))
            campania = {
                "id": camp_id,
                "name": c.get("name"),
                "status": c.get("status"),
                "budget": c.get("dailyBudget", {}).get("amount")
            }
            with conn() as con:
                upsert_campania(con, PLATAFORMA, LI_AD_ACCOUNT_ID, campania)
            count += 1
        if count == 0:
            print(f"⚠ No se encontraron campañas en la cuenta {LI_AD_ACCOUNT_ID}")
        else:
            print(f"✔ Campañas cargadas: {count}")
    except RuntimeError as e:
        print(f"⚠ Error al cargar campañas: {e}")

# ================================
# Main
# ================================
def main():
    resumen = {
        "pagina": False,
        "seguidores": False,
        "posts_ads": {"ok": 0, "skip": 0},
        "campanias": 0,
        "posts_dma": 0
    }

    print("→ LI: Ingesta de página (Ads)")
    try:
        ingest_page_classic()
        resumen["pagina"] = True
    except Exception as e:
        print(f"⚠ Error en ingest_page_classic: {e}")

    print("→ LI: Ingesta de seguidores (DMA)")
    try:
        ingest_followers_dma()
        resumen["seguidores"] = True
    except Exception as e:
        print(f"⚠ Error en ingest_followers_dma: {e}")

    print("→ LI: Ingesta de publicaciones (UGC Ads)")
    ingest_posts_ads()

    print("→ LI: Ingesta de campañas (Ads)")
    try:
        ingest_campaigns_ads()
        # si quieres, en la función puedes devolver count y asignarlo aquí
    except Exception as e:
        print(f"⚠ Error en ingest_campaigns_ads: {e}")

    print("→ LI: Ingesta complementaria (DMA)")
    try:
        ingest_posts_dma()
        # idem, si devuelve número se asigna aquí
    except Exception as e:
        print(f"⚠ Error en ingest_posts_dma: {e}")

    calcular_variaciones()
    print("✔ Variaciones calculadas")

    # ================================
    # Resumen final
    # ================================
    print("\nResumen de ingesta LinkedIn")
    print(f" - Página: {'✔' if resumen['pagina'] else '⚠'}")
    print(f" - Seguidores: {'✔' if resumen['seguidores'] else '⚠'}")
    print(f" - Publicaciones (Ads): {resumen['posts_ads']['ok']} insertadas, {resumen['posts_ads']['skip']} descartadas")
    print(f" - Campañas: {resumen['campanias']}")
    print(f" - Publicaciones (DMA): {resumen['posts_dma']}")
    print("✔ LinkedIn listo")


if __name__ == "__main__":
    main()
