# crear_indices_concurrently.py
import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("PGHOST"),
    port=os.getenv("PGPORT", "5432"),
    user=os.getenv("PGUSER"),
    password=os.getenv("PGPASSWORD"),
    dbname=os.getenv("PGDATABASE"),
    sslmode="require"
)

# Necesitamos autocommit para CREATE INDEX CONCURRENTLY (no puede estar en transacción)
conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
cur = conn.cursor()

try:
    # opcional: ver timeout actual
    cur.execute("SHOW statement_timeout;")
    print("statement_timeout antes:", cur.fetchone()[0])

    # opcional: eliminar timeout de sesión (0 = sin límite). Si el servidor no lo permite, da error.
    try:
        cur.execute("SET statement_timeout = 0;")
        print("statement_timeout deshabilitado para la sesión.")
    except Exception as e:
        print("No se pudo cambiar statement_timeout:", e)

    indices = [
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_empresa_ruc ON dim_empresa (ruc);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_vehiculo_placa ON dim_vehiculo (placa);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_ubicacion_ubigeo ON dim_ubicacion (ubigeo);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_combustible_comb ON dim_combustible (combustible);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_carac_comp ON dim_caracteristicas_tecnicas (n_asientos, n_llantas, n_ejes, carga_util, p_seco, p_bruto, largo, ancho, alto);",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS ux_dim_tiempo_fechas ON dim_tiempo (fecha_resolucion, vigencia_hasta, fecha_corte);"
    ]

    for sql in indices:
        print("Ejecutando:", sql.split("ON")[0].strip() + " ...")
        try:
            cur.execute(sql)
            print(" OK")
        except Exception as e:
            print(" ERROR al crear índice:", e)
            # continua con los siguientes índices

    # opcional: restaurar timeout (ejemplo 60000 ms = 60s)
    try:
        cur.execute("SET statement_timeout = DEFAULT;")
    except:
        pass

finally:
    cur.close()
    conn.close()
    print("Conexión cerrada.")
