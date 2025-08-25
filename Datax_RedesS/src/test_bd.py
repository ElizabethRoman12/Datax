# src/test_db.py
from sqlalchemy import text, inspect
from db import engine, PG_URL

def main():
    print("Probando conexión...")

    with engine.connect() as conn:
        one = conn.execute(text("SELECT 1")).scalar_one()
        print("SELECT 1 =>", one)

        enc_server = conn.execute(text("SHOW SERVER_ENCODING")).scalar_one()
        enc_client = conn.execute(text("SHOW CLIENT_ENCODING")).scalar_one()
        version    = conn.execute(text("SELECT version()")).scalar_one()

        print("SERVER_ENCODING:", enc_server, "| CLIENT_ENCODING:", enc_client)
        print("Version:", version)

        if enc_client.upper() != "UTF8":
            conn.execute(text("SET CLIENT_ENCODING TO 'UTF8'"))
            enc_client2 = conn.execute(text("SHOW CLIENT_ENCODING")).scalar_one()
            print("CLIENT_ENCODING ajustado a:", enc_client2)

    insp = inspect(engine)
    print("Tablas en 'public':", insp.get_table_names(schema="public"))

    with engine.begin() as conn:
        conn.execute(text("CREATE TEMP TABLE ping (x int)"))
        conn.execute(text("INSERT INTO ping (x) VALUES (1),(2)"))
        n = conn.execute(text("SELECT COUNT(*) FROM ping")).scalar_one()
        print("Escritura temporal OK, filas =", n)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("\n❌ Error al conectar o consultar:")
        print(type(e).__name__, "-", e)
        print("\nSolución rápida:")
        print("1) Revisa que .env esté en UTF-8 (sin acentos sin codificar).")
        print("2) Si la contraseña tiene caracteres especiales, URL-codifícala en PG_URL.")
        print("3) Verifica que PostgreSQL esté corriendo y acepta conexiones en 5432.")
