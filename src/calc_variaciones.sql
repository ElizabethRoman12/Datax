WITH m AS (
  SELECT
    plataforma, pagina_id, publicacion_id, fecha_descarga,
    visualizaciones, alcance, comentarios, compartidos, guardados,
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
WHERE d.plataforma=m.plataforma
  AND d.pagina_id=m.pagina_id
  AND d.publicacion_id=m.publicacion_id
  AND d.fecha_descarga=m.fecha_descarga;
