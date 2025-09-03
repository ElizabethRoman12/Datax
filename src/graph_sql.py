import psycopg2
from datetime import date



#----------PAGINA
def upsert_pagina(con, pagina: dict):
    """
    Inserta o actualiza la página en la tabla paginas.
    Espera un dict con:
      - pagina_id (str)
      - plataforma (str)
      - nombre (str)
    """
    sql = """
    INSERT INTO paginas (pagina_id, plataforma, nombre)
    VALUES (%s, %s, %s)
    ON CONFLICT (pagina_id) DO UPDATE
    SET plataforma = EXCLUDED.plataforma,
        nombre     = EXCLUDED.nombre;
    """
    params = (pagina["pagina_id"], pagina["plataforma"], pagina["nombre"])
    with con.cursor() as cur:
        cur.execute(sql, params)
    con.commit()


# ---------- PUBLICACIONES ----------

def infer_formato(post):
    att = (post.get("attachments") or {}).get("data") or [{}]
    media = (att[0] or {}).get("media_type", "") or ""
    st = (post.get("status_type") or "") or ""
    m, s = media.lower(), st.lower()
    if "video" in (m or s): return "video"
    if "photo" in m or "image" in m: return "imagen"
    if "album" in m: return "carrusel"
    if "link" in m or "shared_story" in s: return "link"
    return s or "desconocido"

def upsert_publicacion(conn, plataforma, pagina_id, pub):
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
        "fecha_hora": pub["created_time"].replace("Z","+00:00"),
        "texto": pub.get("message"),
        "formato": infer_formato(pub),
    }
    with conn.cursor() as cur:
        cur.execute(sql, params)

# ---------- MÉTRICAS PUBLICACIÓN DIARIA (SOLO GENERALES) ----------

def _ultimo_registro_prev(conn, plataforma, pagina_id, publicacion_id, fecha_descarga):
    q = """
      SELECT visualizaciones, alcance, impresiones, comentarios, compartidos, guardados
      FROM metricas_publicaciones_diarias
      WHERE plataforma=%s AND pagina_id=%s AND publicacion_id=%s AND fecha_descarga < %s
      ORDER BY fecha_descarga DESC
      LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(q, (plataforma, pagina_id, publicacion_id, fecha_descarga))
        return cur.fetchone()  # tuple or None

def upsert_metricas_publicacion_diaria(conn, plataforma, pagina_id, publicacion_id, fecha_descarga: date, m):
    prev = _ultimo_registro_prev(conn, plataforma, pagina_id, publicacion_id, fecha_descarga)
    # Índices del SELECT previo: (visualizaciones, alcance, impresiones, comentarios, compartidos, guardados)
    d = lambda key, idx: (m.get(key, 0) - (prev[idx] if prev else 0))

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

    row = {
        "plataforma": plataforma,
        "pagina_id": pagina_id,
        "publicacion_id": publicacion_id,
        "fecha_descarga": fecha_descarga,

        "visualizaciones": m.get("visualizaciones", 0),
        "alcance":         m.get("alcance", 0),
        "impresiones":     m.get("impresiones", 0),
        "tiempo_promedio": m.get("tiempo_promedio", None),

        "comentarios": m.get("comentarios", 0),
        "compartidos": m.get("compartidos", 0),
        "guardados":   m.get("guardados", 0),

        "clics": m.get("clics_enlace", 0),
        "ctr":   m.get("ctr", None),

        # deltas vs. día previo
        "d_vis":  d("visualizaciones", 0),
        "d_alc":  d("alcance", 1),
        "d_com":  d("comentarios", 3),
        "d_comp": d("compartidos", 4),
        "d_guard":d("guardados", 5),
    }

    with conn.cursor() as cur:
        cur.execute(sql, row)

# ---------- REACCIONES PUBLICACIÓN DIARIA ----------

def upsert_reaccion_publicacion_diaria(conn, plataforma, pagina_id, publicacion_id, fecha_descarga: date, tipo_reaccion_id: int, cantidad: int):
    sql = """
    INSERT INTO reacciones_publicacion_diaria
      (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id, cantidad)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id) DO UPDATE SET
      cantidad = EXCLUDED.cantidad;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plataforma, pagina_id, publicacion_id, fecha_descarga, tipo_reaccion_id, cantidad))

# ---------- ESTADÍSTICAS DE PÁGINA (SEMANAL) ----------

def upsert_estadistica_pagina_semanal(conn, plataforma, pagina_id, fila):
    sql = """
    INSERT INTO estadisticas_pagina_semanal
      (plataforma, pagina_id, fecha_corte_semana, total_seguidores, alcance_pagina, visualizaciones_pagina)
    VALUES (%s,%s,%s,%s,%s,%s)
    ON CONFLICT (plataforma, pagina_id, fecha_corte_semana) DO UPDATE SET
      total_seguidores = EXCLUDED.total_seguidores,
      alcance_pagina = EXCLUDED.alcance_pagina,
      visualizaciones_pagina = EXCLUDED.visualizaciones_pagina;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (
            plataforma, pagina_id, fila["fecha_corte"],
            fila.get("fans_total", 0), fila.get("alcance", 0), fila.get("impresiones", 0)
        ))

# ---------- SEGMENTACIÓN (SEMANAL) ----------

def insert_segmento_semanal(conn, plataforma, pagina_id, fecha_corte, genero=None, pais=None, ciudad=None, nivel_edu=None, cantidad=0):
    sql = """
    INSERT INTO segmentacion_seguidores_semanal
      (plataforma, pagina_id, fecha_corte_semana, genero, pais, ciudad, nivel_educacion, cantidad_seguidores)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (plataforma, pagina_id, fecha_corte, genero, pais, ciudad, nivel_edu, cantidad))
