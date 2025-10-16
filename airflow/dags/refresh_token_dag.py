from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fb_ig_ingest_unificado import renovar_facebook_instagram

default_args = {
    'owner': 'datax',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='refresh_graph_dag',
    default_args=default_args,
    description='Renueva el token de acceso de Facebook/Instagram (reemplazo de refresh_graph.bat)',
    schedule_interval='@monthly',  #Ejecutar una vez al mes
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=['datax', 'tokens', 'facebook', 'instagram']
) as dag:

    renovar_tokens = PythonOperator(
        task_id='renovar_tokens_fb_ig',
        python_callable=renovar_facebook_instagram,
    )

    renovar_tokens
