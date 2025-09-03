# Dataxcat > README.md << 'EOF'
# 📊 DATAX – Ingesta de Redes Sociales (v2-nueva-base)

Este proyecto corresponde a la **nueva versión (v2)** del sistema de ingesta de datos de redes sociales para **DATAX**.  
Incluye la migración a un modelo de base de datos optimizado y la corrección de flujos de ingesta.

---

## 🚀 Cambios principales respecto a la versión anterior (v1)
- **Nuevo modelo de base de datos**:
  - Normalización de reacciones en tabla `tipo_reaccion` + `reacciones_publicacion_diaria`.
  - Eliminación del campo redundante `reacciones` (ahora se calcula con un trigger y un campo `total_reacciones` en `metricas_publicaciones_diarias`).
  - Tablas organizadas para páginas, publicaciones, métricas, campañas y segmentación.

- **Triggers y vistas**:
  - Trigger automático que mantiene actualizado el `total_reacciones`.
  - Vista `vista_metricas_publicaciones` que consolida métricas diarias + reacciones pivotadas en un solo resultado.

- **Scripts de ingesta corregidos**:
  - `fb_ingest.py`: ahora inserta primero la página (`ingest_page`), luego publicaciones y métricas.
  - `ig_ingest.py`: ahora inserta primero la cuenta de IG (`ingest_account`), luego medios y métricas.
  - Variables de entorno separadas en `.env`:
    - `FB_PAGE_ID` para Facebook.
    - `IG_USER_ID` para Instagram.

---

## 📂 Estructura del proyecto

src/
├── fb_ingest.py # Ingesta de Facebook
├── ig_ingest.py # Ingesta de Instagram
├── graph_api.py # Cliente para Graph API
├── graph_sql.py # Funciones SQL (upserts)
├── ...
.env # Variables de entorno
requisitos.txt # Dependencias
README.md # Documentación


---

## ⚙️ Variables de entorno (`.env`)

```env
# Base de datos
PG_URL=postgresql://postgres:postgres@localhost:5432/Datax
GRAPH_URL=https://graph.facebook.com/v19.0

# Facebook
ACCESS_TOKEN_FB=xxxxxxxxxxxx
FB_PAGE_ID=137292499683680

# Instagram
ACCESS_TOKEN_IG=xxxxxxxxxxxx
IG_USER_ID=17841461386841732
