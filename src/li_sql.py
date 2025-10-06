

def upsert_campania(con, plataforma, cuenta_id, campania):
    """
    Inserta o actualiza una campaña de LinkedIn en la tabla campanias.
    """
    with con.cursor() as cur:
        cur.execute("""
            INSERT INTO campanias (plataforma, cuenta_id, campania_id, nombre, estado, presupuesto)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (plataforma, cuenta_id, campania_id)
            DO UPDATE SET nombre      = EXCLUDED.nombre,
                          estado      = EXCLUDED.estado,
                          presupuesto = EXCLUDED.presupuesto
        """, (
            plataforma,
            cuenta_id,
            campania["id"],
            campania.get("name"),
            campania.get("status"),
            campania.get("budget")
        ))

def upsert_metricas_campania_diaria(con, plataforma, cuenta_id, campania_id, fecha, metricas):
    """
    Inserta o actualiza métricas diarias de campañas en la tabla metricas_campanias_diarias.
    """
    with con.cursor() as cur:
        cur.execute("""
            INSERT INTO metricas_campanias_diarias
                (plataforma, cuenta_id, campania_id, fecha_descarga,
                 alcance, impresiones, resultados,
                 cpc, cpm, cpa, presupuesto_invertido)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (plataforma, cuenta_id, campania_id, fecha_descarga)
            DO UPDATE SET alcance             = EXCLUDED.alcance,
                          impresiones         = EXCLUDED.impresiones,
                          resultados          = EXCLUDED.resultados,
                          cpc                 = EXCLUDED.cpc,
                          cpm                 = EXCLUDED.cpm,
                          cpa                 = EXCLUDED.cpa,
                          presupuesto_invertido = EXCLUDED.presupuesto_invertido
        """, (
            plataforma,
            cuenta_id,
            campania_id,
            fecha,
            metricas.get("alcance"),
            metricas.get("impresiones"),
            metricas.get("resultados"),
            metricas.get("cpc"),
            metricas.get("cpm"),
            metricas.get("cpa"),
            metricas.get("gasto"),   # en tu modelo está como presupuesto_invertido
        ))
