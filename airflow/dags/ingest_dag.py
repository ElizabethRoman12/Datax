"""
DAG de Airflow para ingestar publicaciones y métricas de Facebook e Instagram.
- Si se ejecuta automáticamente (schedule o "Trigger DAG now") → usa el día actual.
- Si se ejecuta manualmente con fechas (Airflow backfill o params) → usa el rango dado.
"""

import sys, os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Acceso al código fuente (src/)
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src"))

from refresh_graph import renovar_facebook_instagram
from fb_ingest import run_fb_ingest
from ig_ingest import run_ig_ingest

# Configuración base del DAG
default_args = {
    "owner": "DataX",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Funciones auxiliares
def ejecutar_facebook(**context):
    """Ejecuta la ingesta de Facebook con rango o fecha actual."""
    params = context["params"]
    start = params.get("start_date")
    end = params.get("end_date")

    # Si no hay rango → usa la fecha de ejecución del DAG (o hoy)
    if not start or not end:
        fecha = context.get("ds") or datetime.today().strftime("%Y-%m-%d")
        start = end = fecha
        print(f"📅 Ejecutando FB para el día: {fecha}")
    else:
        print(f"📅 Ejecutando FB para rango: {start} → {end}")

    run_fb_ingest(start, end)


def ejecutar_instagram(**context):
    """Ejecuta la ingesta de Instagram con rango o fecha actual."""
    params = context["params"]
    start = params.get("start_date")
    end = params.get("end_date")

    if not start or not end:
        fecha = context.get("ds") or datetime.today().strftime("%Y-%m-%d")
        start = end = fecha
        print(f"📅 Ejecutando IG para el día: {fecha}")
    else:
        print(f"📅 Ejecutando IG para rango: {start} → {end}")

    run_ig_ingest(start, end)

# Definición del DAG
with DAG(
    dag_id="datax_ingesta_redes_flex",
    description="Pipeline flexible de ingesta de Facebook e Instagram (día actual o rango manual).",
    default_args=default_args,
    schedule_interval="@daily",   # corre automáticamente cada día
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["datax", "facebook", "instagram", "meta"],
) as dag:

    # Renovar token (una vez antes de las ingestas)
    renovar_token = PythonOperator(
        task_id="renovar_token_facebook_instagram",
        python_callable=renovar_facebook_instagram,
    )

    #Ingesta de Facebook
    ingest_facebook = PythonOperator(
        task_id="ingest_facebook",
        python_callable=ejecutar_facebook,
        provide_context=True,
        params={
            # puedes pasar fechas al ejecutar manualmente desde la UI
            "start_date": None,
            "end_date": None,
        },
    )

    #Ingesta de Instagram
    ingest_instagram = PythonOperator(
        task_id="ingest_instagram",
        python_callable=ejecutar_instagram,
        provide_context=True,
        params={
            "start_date": None,
            "end_date": None,
        },
    )

    # Dependencias
    renovar_token >> ingest_facebook >> ingest_instagram
