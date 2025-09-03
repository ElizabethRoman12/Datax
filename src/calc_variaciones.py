
from pathlib import Path
from sqlalchemy import text
from db import engine

def calcular_variaciones():
    """Ejecuta el SQL que recalcula las variaciones en metricas_publicaciones_diarias."""
    sql_path = Path(__file__).resolve().parent / "calc_variaciones.sql"

    try:
        sql = sql_path.read_text(encoding="utf-8")

        with engine.begin() as conn:
            conn.execute(text(sql))

        print("Variaciones de métricas generales actualizadas en metricas_publicaciones_diarias")

    except Exception as e:
        print(f"❌ Error al ejecutar calc_variaciones.sql: {e}")


if __name__ == "__main__":
    calcular_variaciones()


