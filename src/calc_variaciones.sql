WITH m AS (
  SELECT
    plataforma, pagina_id, publicacion_id, fecha_descarga,
    visualizaciones, alcance, reacciones, comentarios, compartidos, guardados,
    LAG(visualizaciones) OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_visualizaciones,
    LAG(alcance)         OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_alcance,
    LAG(reacciones)      OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_reacciones,
    LAG(comentarios)     OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_comentarios,
    LAG(compartidos)     OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_compartidos,
    LAG(guardados)       OVER (PARTITION BY plataforma, pagina_id, publicacion_id ORDER BY fecha_descarga) AS prev_guardados
  FROM metricas_publicaciones_diarias
)
UPDATE metricas_publicaciones_diarias d
SET
  delta_visualizaciones = m.visualizaciones - m.prev_visualizaciones,
  delta_alcance         = m.alcance - m.prev_alcance,
  delta_reacciones      = m.reacciones - m.prev_reacciones,
  delta_comentarios     = m.comentarios - m.prev_comentarios,
  delta_compartidos     = m.compartidos - m.prev_compartidos,
  delta_guardados       = m.guardados - m.prev_guardados
FROM m
WHERE d.plataforma=m.plataforma
  AND d.pagina_id=m.pagina_id
  AND d.publicacion_id=m.publicacion_id
  AND d.fecha_descarga=m.fecha_descarga;
