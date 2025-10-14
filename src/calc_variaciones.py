from pathlib import Path
from sqlalchemy import text
from db import get_engine  # más seguro que importar directamente `engine`

def calcular_variaciones():
    """Ejecuta el SQL que recalcula las variaciones de métricas diarias."""
    sql_path = Path(__file__).resolve().parent / "calc_variaciones.sql"

    print(f" Ejecutando script de variaciones: {sql_path.name}")

    try:
        sql = sql_path.read_text(encoding="utf-8")

        engine = get_engine()
        with engine.begin() as conn:
            conn.execute(text(sql))

        print("Variaciones actualizadas correctamente en metricas_publicaciones_diarias.")

    except FileNotFoundError:
        print(f" No se encontró el archivo SQL: {sql_path}")
    except Exception as e:
        print(f" Error al ejecutar {sql_path.name}: {e}")

# Ejecución manual
if __name__ == "__main__":
    calcular_variaciones()
