from pathlib import Path
from sqlalchemy import text
from db import engine

def main():
    # Ruta del archivo SQL
    sql_path = Path(__file__).resolve().parent / "calc_variaciones.sql"
    
    # Leer el archivo SQL
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    
    # Ejecutar el SQL en la base
    with engine.begin() as conn:
        conn.execute(text(sql))
    
    print("✅ Variaciones de métricas generales actualizadas en metricas_publicaciones_diarias")

if __name__ == "__main__":
    main()
