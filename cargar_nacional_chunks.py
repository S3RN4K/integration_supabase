# cargar_nacional_chunks_xlsx_corregido.py
import os, sys, io, json
from dotenv import load_dotenv
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ---------- CARGAR CONFIG ----------
load_dotenv()
PGHOST = os.getenv("PGHOST")
PGPORT = os.getenv("PGPORT", "5432")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGDATABASE = os.getenv("PGDATABASE")

DATA_PATH = "Transporte Terrestre Carga Nacional_2022-2024.xlsx"
CHUNK_SIZE = 1000  # Forzar chunks de 1000 filas
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", "checkpoint_nacional.json")

# CLI args
# No CLI flags: siempre ejecutar subida real
DRY_RUN = False

if not (PGHOST and PGUSER and PGPASSWORD and PGDATABASE):
    print("Faltan variables de conexión en .env (PGHOST/PGUSER/PGPASSWORD/PGDATABASE).")
    sys.exit(1)

# ---------- CONEXIÓN ----------
conn = psycopg2.connect(
    host=PGHOST, port=PGPORT, user=PGUSER, password=PGPASSWORD, dbname=PGDATABASE, sslmode="require"
)
cur = conn.cursor()

# ---------- Sincronizar secuencias ----------
seq_updates = [
    ("dim_empresa_id_empresa_seq", "dim_empresa", "id_empresa"),
    ("dim_vehiculo_id_vehiculo_seq", "dim_vehiculo", "id_vehiculo"),
    ("dim_ubicacion_id_ubicacion_seq", "dim_ubicacion", "id_ubicacion"),
    ("dim_combustible_id_combustible_seq", "dim_combustible", "id_combustible"),
    ("dim_caracteristicas_tecnicas_id_caracteristicas_tecnicas_seq", "dim_caracteristicas_tecnicas", "id_caracteristicas_tecnicas"),
    ("dim_tiempo_id_tiempo_seq", "dim_tiempo", "id_tiempo")
]
for seq_name, table_name, pk_col in seq_updates:
    cur.execute(f"SELECT setval('{seq_name}', COALESCE((SELECT MAX({pk_col}) FROM {table_name}), 0) + 1, false);")
    print(f"Secuencia {seq_name} alineada con {table_name}.{pk_col}")
conn.commit()

# ---------- HELPERS ----------
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_chunk": -1, "rows_processed": 0}

def save_checkpoint(state):
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)

def safe_int(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        s = str(x).strip().replace(",", "")
        if s == "":
            return None
        # evita "2.0" retornando int
        return int(float(s))
    except:
        return None

def safe_float(x):
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        s = str(x).strip().replace(",", ".")
        if s == "":
            return None
        return float(s)
    except:
        return None

def parse_date_ymd(val):
    try:
        if val is None: return None
        s = str(val).strip()
        if s == "": return None
        if s.isdigit() and len(s) == 8:
            return pd.to_datetime(s, format="%Y%m%d", errors="coerce").date()
        dt = pd.to_datetime(s, errors="coerce")
        if pd.isna(dt): return None
        return dt.date()
    except:
        return None

def safe_date(v):
    """Return a Python date or None. Handles pandas NaT/Timestamps."""
    try:
        if v is None:
            return None
        if isinstance(v, str):
            if v.strip() == "":
                return None
        # pandas NA / NaT
        if isinstance(v, float) and pd.isna(v):
            return None
        try:
            if pd.isna(v):
                return None
        except Exception:
            pass
        if hasattr(v, 'date'):
            try:
                return v.date()
            except Exception:
                return v
        return v
    except Exception:
        return None

def normalize_chasis(val):
    """Normalize N_CHASIS values to plain string without scientific notation.

    Rules:
    - None/empty -> None
    - If original looks like scientific notation (contains 'e' or 'E'), use Decimal to
      convert to a plain string without exponent.
    - If string is all digits and starts with '0', preserve it (likely textual with leading zeros).
    - If numeric float-like and integer-valued, convert to int -> str.
    - Otherwise return the original string stripped.
    """
    try:
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        s = str(val).strip()
        if s == "":
            return None
        # preserve textual values with leading zeros
        if s.isdigit() and s.startswith("0"):
            return s
        # scientific notation string e.g. '1.21117E+11'
        if "e" in s.lower():
            from decimal import Decimal, InvalidOperation
            try:
                d = Decimal(s)
                # format without exponent
                plain = format(d, 'f')
                # if it's integer-valued, remove trailing .0
                if plain.endswith('.0'):
                    plain = plain[:-2]
                return plain
            except InvalidOperation:
                return s
        # numeric-looking strings or floats
        if s.replace('.', '', 1).isdigit():
            try:
                f = float(s)
                if f.is_integer():
                    return str(int(f))
                # remove unnecessary trailing zeros
                return str(f).rstrip('0').rstrip('.')
            except:
                return s
        return s
    except Exception:
        return None

def df_to_buffer_tab(df):
    buf = io.StringIO()
    # NaNs ya están como None -> to_csv convertirá a empty; usamos na_rep='\\N'
    df.to_csv(buf, index=False, header=False, sep="\t", na_rep='\\N')
    buf.seek(0)
    return buf

# ---------- TEMP STAGING ----------
def create_temp_staging():
    # stg_empresa como text (luego casteamos a bigint al insertar en dim_empresa)
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS stg_empresa (
        ruc text,
        razon_social text,
        estado_tributario text
    );
    """)
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS stg_vehiculo (
        placa text,
        anio_fab int,
        n_chasis text,
        n_motor text,
        marca text,
        servicio text,
        clase text
    );
    """)
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS stg_ubicacion (
        ubigeo text,
        departamento text,
        provincia text,
        distrito text
    );
    """)
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS stg_combustible (combustible text);")
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS stg_carac (
        n_asientos int,
        n_llantas int,
        n_ejes int,
        carga_util numeric,
        p_seco numeric,
        p_bruto numeric,
        largo numeric,
        ancho numeric,
        alto numeric
    );
    """)
    cur.execute("""
    CREATE TEMP TABLE IF NOT EXISTS stg_tiempo (
        fecha_resolucion date,
        vigencia_hasta date,
        fecha_corte date,
        anio int,
        mes int
    );
    """)
    conn.commit()

def drop_temp_staging():
    cur.execute("""
        DROP TABLE IF EXISTS stg_empresa;
        DROP TABLE IF EXISTS stg_vehiculo;
        DROP TABLE IF EXISTS stg_ubicacion;
        DROP TABLE IF EXISTS stg_combustible;
        DROP TABLE IF EXISTS stg_carac;
        DROP TABLE IF EXISTS stg_tiempo;
    """)
    conn.commit()
    print("✅ Tablas temporales eliminadas correctamente.")

# ---------- PROCESAMIENTO XLSX POR CHUNKS ----------
if not os.path.exists(DATA_PATH):
    print(f"Archivo no encontrado: {DATA_PATH}")
    sys.exit(1)

# Soporte para usar un CSV preprocesado si existe (por ejemplo "nacional_fixed.csv").
CSV_FALLBACK = "nacional_fixed.csv"
USE_CSV = os.path.exists(CSV_FALLBACK)
if USE_CSV:
    DATA_PATH = CSV_FALLBACK

state = load_checkpoint()
start_chunk = state.get("last_chunk", -1) + 1
processed = state.get("rows_processed", 0)
chunk_idx = start_chunk

create_temp_staging()

def process_chunk(chunk, chunk_idx):
    """Process one chunk inside a DB transaction. Commits on success, rolls back and
    re-raises on failure so the caller can decide whether to save checkpoint.

    Returns number of rows processed (len(chunk)).
    """
    try:
        cur.execute("BEGIN;")

        # normalizamos/limpiamos columnas ya realizados por el caller
        for c in ["FECHA_RESOLUCION", "VIGENCIA_HASTA", "FECHA_CORTE"]:
            if c in chunk.columns:
                chunk[c] = chunk[c].apply(parse_date_ymd)

        # ---------- Construir staging ----------
        df_emp = chunk[["RUC", "RAZON_SOCIAL"]].copy() if {"RUC", "RAZON_SOCIAL"}.issubset(chunk.columns) else pd.DataFrame(columns=["RUC", "RAZON_SOCIAL"])
        if not df_emp.empty:
            df_emp["ESTADO_TRIBUTARIO"] = "ACTIVO"
            df_emp = df_emp.drop_duplicates(subset=["RUC"])

        veh_cols = [c for c in ["PLACA", "ANIO_FAB", "N_CHASIS", "N_MOTOR", "MARCA", "SERVICIO", "CLASE"] if c in chunk.columns]
        df_veh = chunk[veh_cols].copy() if veh_cols else pd.DataFrame(columns=veh_cols)
        if "ANIO_FAB" in df_veh.columns:
            df_veh["ANIO_FAB"] = df_veh["ANIO_FAB"].apply(safe_int)
        if "PLACA" in df_veh.columns:
            df_veh = df_veh.drop_duplicates(subset=["PLACA"])

        ubi_cols = [c for c in ["UBIGEO", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"] if c in chunk.columns]
        df_ubi = chunk[ubi_cols].copy() if ubi_cols else pd.DataFrame(columns=ubi_cols)
        if "UBIGEO" in df_ubi.columns:
            df_ubi = df_ubi.drop_duplicates(subset=["UBIGEO"])

        df_comb = chunk[["COMBUSTIBLE"]].drop_duplicates() if "COMBUSTIBLE" in chunk.columns else pd.DataFrame(columns=["COMBUSTIBLE"])

        car_cols = [c for c in ["N_ASIENTOS", "N_LLANTAS", "N_EJES", "CARGA_UTIL", "P_SECO", "P_BRUTO", "LARGO", "ANCHO", "ALTO"] if c in chunk.columns]
        df_car = chunk[car_cols].copy() if car_cols else pd.DataFrame(columns=car_cols)
        for c in ["N_ASIENTOS", "N_LLANTAS", "N_EJES"]:
            if c in df_car.columns:
                df_car[c] = df_car[c].apply(safe_int)
        for c in ["CARGA_UTIL", "P_SECO", "P_BRUTO", "LARGO", "ANCHO", "ALTO"]:
            if c in df_car.columns:
                df_car[c] = df_car[c].apply(safe_float)
        if not df_car.empty:
            df_car = df_car.drop_duplicates()

        time_cols = [c for c in ["FECHA_RESOLUCION", "VIGENCIA_HASTA", "FECHA_CORTE"] if c in chunk.columns]
        df_time = chunk[time_cols].copy() if time_cols else pd.DataFrame(columns=time_cols)
        if "FECHA_CORTE" in df_time.columns and not df_time.empty:
            df_time["ANIO"] = pd.to_datetime(df_time["FECHA_CORTE"], errors='coerce').dt.year
            df_time["MES"] = pd.to_datetime(df_time["FECHA_CORTE"], errors='coerce').dt.month
        else:
            if df_time.empty:
                df_time = pd.DataFrame(columns=["FECHA_RESOLUCION", "VIGENCIA_HASTA", "FECHA_CORTE", "ANIO", "MES"])
            else:
                df_time["ANIO"] = None
                df_time["MES"] = None
        if not df_time.empty:
            df_time = df_time.drop_duplicates()

        # ---------- COPY a staging ----------
        staging_map = {
            "df_emp": ("stg_empresa", ["RUC", "RAZON_SOCIAL", "ESTADO_TRIBUTARIO"]),
            "df_veh": ("stg_vehiculo", veh_cols),
            "df_ubi": ("stg_ubicacion", ubi_cols),
            "df_comb": ("stg_combustible", ["COMBUSTIBLE"]),
            "df_car": ("stg_carac", car_cols),
            "df_time": ("stg_tiempo", ["FECHA_RESOLUCION", "VIGENCIA_HASTA", "FECHA_CORTE", "ANIO", "MES"])
        }

        for df_name, (stg_table, cols) in staging_map.items():
            df = locals()[df_name]
            if df.empty:
                continue
            if DRY_RUN:
                print(f"[dry-run] would COPY {len(df)} rows into {stg_table} ({','.join(cols)})")
            else:
                df_copy = df[cols].copy()
                int_cols = {"ANIO_FAB", "N_ASIENTOS", "N_LLANTAS", "N_EJES"}
                for ic in int_cols:
                    if ic in df_copy.columns:
                        df_copy[ic] = df_copy[ic].apply(lambda v: str(int(v)) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None)
                buf = df_to_buffer_tab(df_copy)
                cur.copy_expert(f"COPY {stg_table} ({','.join([c.lower() for c in cols])}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')", buf)

        # ---------- INSERT dimensiones (no commits until the end) ----------
        if not df_emp.empty:
            cur.execute("""
                INSERT INTO dim_empresa (ruc, razon_social, estado_tributario)
                SELECT DISTINCT NULLIF(s.ruc,'')::bigint, s.razon_social, s.estado_tributario
                FROM stg_empresa s
                LEFT JOIN dim_empresa d ON d.ruc = NULLIF(s.ruc,'')::bigint
                WHERE NULLIF(s.ruc,'') IS NOT NULL
                AND d.ruc IS NULL;
            """)

        if not df_veh.empty:
            cur.execute("""
                INSERT INTO dim_vehiculo (placa, anio_fab, n_chasis, n_motor, marca, servicio, clase)
                SELECT DISTINCT s.placa, s.anio_fab, s.n_chasis, s.n_motor, s.marca, s.servicio, s.clase
                FROM stg_vehiculo s
                LEFT JOIN dim_vehiculo d ON d.placa = s.placa
                WHERE s.placa IS NOT NULL AND d.placa IS NULL;
            """)

        if not df_ubi.empty:
            cur.execute("""
                INSERT INTO dim_ubicacion (ubigeo, departamento, provincia, distrito)
                SELECT DISTINCT s.ubigeo, s.departamento, s.provincia, s.distrito
                FROM stg_ubicacion s
                LEFT JOIN dim_ubicacion d ON d.ubigeo = s.ubigeo
                WHERE s.ubigeo IS NOT NULL AND d.ubigeo IS NULL;
            """)

        if not df_comb.empty:
            cur.execute("""
                INSERT INTO dim_combustible (combustible)
                SELECT DISTINCT s.combustible
                FROM stg_combustible s
                LEFT JOIN dim_combustible d ON d.combustible = s.combustible
                WHERE s.combustible IS NOT NULL AND d.combustible IS NULL;
            """)

        if not df_car.empty:
            cur.execute("""
                INSERT INTO dim_caracteristicas_tecnicas 
                    (n_asientos, n_llantas, n_ejes, carga_util, p_seco, p_bruto, largo, ancho, alto)
                SELECT DISTINCT
                    s.n_asientos,
                    s.n_llantas,
                    s.n_ejes,
                    s.carga_util,
                    s.p_seco,
                    s.p_bruto,
                    s.largo,
                    s.ancho,
                    s.alto
                FROM stg_carac s
                LEFT JOIN dim_caracteristicas_tecnicas d
                    ON d.n_asientos IS NOT DISTINCT FROM s.n_asientos
                    AND d.n_llantas IS NOT DISTINCT FROM s.n_llantas
                    AND d.n_ejes IS NOT DISTINCT FROM s.n_ejes
                    AND d.carga_util IS NOT DISTINCT FROM s.carga_util
                    AND d.p_seco IS NOT DISTINCT FROM s.p_seco
                    AND d.p_bruto IS NOT DISTINCT FROM s.p_bruto
                    AND d.largo IS NOT DISTINCT FROM s.largo
                    AND d.ancho IS NOT DISTINCT FROM s.ancho
                    AND d.alto IS NOT DISTINCT FROM s.alto
                WHERE d.id_caracteristicas_tecnicas IS NULL;
            """)

        if not df_time.empty:
            cur.execute("""
                INSERT INTO dim_tiempo (fecha_resolucion, vigencia_hasta, fecha_corte, anio, mes)
                SELECT DISTINCT t.fecha_resolucion, t.vigencia_hasta, t.fecha_corte, t.anio, t.mes
                FROM stg_tiempo t
                LEFT JOIN dim_tiempo d
                    ON d.fecha_resolucion IS NOT DISTINCT FROM t.fecha_resolucion
                    AND d.vigencia_hasta IS NOT DISTINCT FROM t.vigencia_hasta
                    AND d.fecha_corte IS NOT DISTINCT FROM t.fecha_corte
                WHERE d.id_tiempo IS NULL;
            """)

        # ---------- OBTENER MAPPINGS ids por lotes ----------
        map_emp = {}
        map_veh = {}
        map_ubi = {}
        map_comb = {}
        map_car = {}
        if not DRY_RUN:
            if not df_emp.empty:
                rucs = [safe_int(x) for x in df_emp["RUC"].tolist() if safe_int(x) is not None]
                if rucs:
                    cur.execute("SELECT id_empresa, ruc FROM dim_empresa WHERE ruc = ANY(%s::bigint[]);", (rucs,))
                    for idv, ruc in cur.fetchall():
                        map_emp[str(ruc)] = idv

            if not df_veh.empty:
                placas = [str(x) for x in df_veh["PLACA"].tolist() if x is not None and x != ""]
                if placas:
                    cur.execute("SELECT id_vehiculo, placa FROM dim_vehiculo WHERE placa = ANY(%s);", (placas,))
                    for idv, placa in cur.fetchall():
                        map_veh[str(placa)] = idv

            if not df_ubi.empty:
                ubis = [str(x) for x in df_ubi["UBIGEO"].tolist() if x is not None and x != ""]
                if ubis:
                    cur.execute("SELECT id_ubicacion, ubigeo FROM dim_ubicacion WHERE ubigeo = ANY(%s);", (ubis,))
                    for idv, ub in cur.fetchall():
                        map_ubi[str(ub)] = idv

            if not df_comb.empty:
                combs = [str(x) for x in df_comb["COMBUSTIBLE"].tolist() if x is not None and x != ""]
                if combs:
                    cur.execute("SELECT id_combustible, combustible FROM dim_combustible WHERE combustible = ANY(%s);", (combs,))
                    for idv, comb in cur.fetchall():
                        map_comb[str(comb)] = idv

            if not df_car.empty:
                for _, r in df_car.iterrows():
                    key = (safe_int(r.get("N_ASIENTOS")), safe_int(r.get("N_LLANTAS")), safe_int(r.get("N_EJES")),
                           safe_float(r.get("CARGA_UTIL")), safe_float(r.get("P_SECO")), safe_float(r.get("P_BRUTO")),
                           safe_float(r.get("LARGO")), safe_float(r.get("ANCHO")), safe_float(r.get("ALTO")))
                    cur.execute("""
                        SELECT id_caracteristicas_tecnicas FROM dim_caracteristicas_tecnicas
                        WHERE n_asientos IS NOT DISTINCT FROM %s
                          AND n_llantas IS NOT DISTINCT FROM %s
                          AND n_ejes IS NOT DISTINCT FROM %s
                          AND carga_util IS NOT DISTINCT FROM %s
                          AND p_seco IS NOT DISTINCT FROM %s
                          AND p_bruto IS NOT DISTINCT FROM %s
                          AND largo IS NOT DISTINCT FROM %s
                          AND ancho IS NOT DISTINCT FROM %s
                          AND alto IS NOT DISTINCT FROM %s
                    """, key)
                    rec = cur.fetchone()
                    if rec:
                        map_car[key] = rec[0]

        # ---------- preparar y bulk-insert hechos ----------
        hechos = []
        for _, r in chunk.iterrows():
            fecha_res = r.get("FECHA_RESOLUCION")
            vig = r.get("VIGENCIA_HASTA")
            fc = r.get("FECHA_CORTE")
            anio = fecha_res.year if hasattr(fecha_res, "year") else None
            mes = fecha_res.month if hasattr(fecha_res, "month") else None

            if DRY_RUN:
                id_t = None
            else:
                fr = safe_date(fecha_res)
                vg = safe_date(vig)
                fc_safe = safe_date(fc)
                cur.execute("INSERT INTO dim_tiempo (fecha_resolucion, vigencia_hasta, fecha_corte, anio, mes) VALUES (%s,%s,%s,%s,%s) RETURNING id_tiempo;",
                            (fr, vg, fc_safe, anio, mes))
                id_t = cur.fetchone()[0]

            id_emp = None
            if r.get("RUC"):
                id_emp = map_emp.get(str(safe_int(r.get("RUC"))))
                if id_emp is None and not DRY_RUN:
                    cur.execute("SELECT id_empresa FROM dim_empresa WHERE ruc = %s;", (safe_int(r.get("RUC")),))
                    rec = cur.fetchone()
                    if rec:
                        id_emp = rec[0]

            id_veh = None
            if r.get("PLACA"):
                id_veh = map_veh.get(str(r.get("PLACA")))
                if id_veh is None and not DRY_RUN:
                    cur.execute("SELECT id_vehiculo FROM dim_vehiculo WHERE placa = %s;", (str(r.get("PLACA")),))
                    rec = cur.fetchone()
                    if rec:
                        id_veh = rec[0]

            id_ubi = None
            if r.get("UBIGEO"):
                id_ubi = map_ubi.get(str(r.get("UBIGEO")))
                if id_ubi is None and not DRY_RUN:
                    cur.execute("SELECT id_ubicacion FROM dim_ubicacion WHERE ubigeo = %s;", (str(r.get("UBIGEO")),))
                    rec = cur.fetchone()
                    if rec:
                        id_ubi = rec[0]

            id_comb = None
            if r.get("COMBUSTIBLE"):
                id_comb = map_comb.get(str(r.get("COMBUSTIBLE")))
                if id_comb is None and not DRY_RUN:
                    cur.execute("SELECT id_combustible FROM dim_combustible WHERE combustible = %s;", (str(r.get("COMBUSTIBLE")),))
                    rec = cur.fetchone()
                    if rec:
                        id_comb = rec[0]

            car_key = (safe_int(r.get("N_ASIENTOS")), safe_int(r.get("N_LLANTAS")), safe_int(r.get("N_EJES")),
                       safe_float(r.get("CARGA_UTIL")), safe_float(r.get("P_SECO")), safe_float(r.get("P_BRUTO")),
                       safe_float(r.get("LARGO")), safe_float(r.get("ANCHO")), safe_float(r.get("ALTO")))
            id_car = map_car.get(car_key)
            if id_car is None and not DRY_RUN:
                cur.execute("""
                    SELECT id_caracteristicas_tecnicas FROM dim_caracteristicas_tecnicas
                    WHERE n_asientos IS NOT DISTINCT FROM %s
                      AND n_llantas IS NOT DISTINCT FROM %s
                      AND n_ejes IS NOT DISTINCT FROM %s
                      AND carga_util IS NOT DISTINCT FROM %s
                      AND p_seco IS NOT DISTINCT FROM %s
                      AND p_bruto IS NOT DISTINCT FROM %s
                      AND largo IS NOT DISTINCT FROM %s
                      AND ancho IS NOT DISTINCT FROM %s
                      AND alto IS NOT DISTINCT FROM %s
                """, car_key)
                rec = cur.fetchone()
                if rec:
                    id_car = rec[0]

            hechos.append((id_t, id_ubi, id_emp, id_veh, id_car, id_comb, r.get("PERMISO_OPER")))

        if hechos:
            execute_values(cur,
                           "INSERT INTO hecho_vehiculo_carga (id_tiempo, id_ubicacion, id_empresa, id_vehiculo, id_caracteristicas_tecnicas, id_combustible, permiso_oper) VALUES %s",
                           hechos, page_size=1000)

        # commit once per chunk
        conn.commit()
        return len(chunk)

    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise

try:
    if USE_CSV:
        # procesar CSV por chunks usando el iterator de pandas
        try:
            with open(DATA_PATH, 'r', encoding='utf-8-sig') as f:
                total_rows = sum(1 for _ in f) - 1
        except Exception:
            total_rows = None

        reader = pd.read_csv(
            DATA_PATH,
            dtype={"RUC": str, "N_CHASIS": str, "N_MOTOR": str, "PLACA": str},
            chunksize=CHUNK_SIZE,
            low_memory=False,
            encoding='utf-8-sig'
        )

        for idx, chunk in enumerate(reader):
            if idx < start_chunk:
                # ya procesado según checkpoint
                continue
            chunk_idx = idx
            start_row = idx * CHUNK_SIZE
            end_row = start_row + len(chunk)
            if total_rows is not None:
                print(f"\nProcesando filas {start_row+1} a {min(end_row, total_rows)} (chunk {chunk_idx})")
            else:
                print(f"\nProcesando chunk {chunk_idx} (filas {start_row+1} a {end_row})")
            # normal pipeline for CSV chunk
            chunk = chunk.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
            # dejar None en celdas vacías
            chunk = chunk.where(chunk.notnull(), None)
            # Normalizar columnas de chasis/motor para evitar notación científica
            for col in ("N_CHASIS", "N_MOTOR"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].apply(normalize_chasis)
            # procesar este chunk (inserciones, staging, hechos)
            rows_processed = process_chunk(chunk, chunk_idx)
            processed += rows_processed
            save_checkpoint({"last_chunk": chunk_idx, "rows_processed": processed})
            print(f"Chunk {chunk_idx} completado. Filas procesadas totales: {processed}")
    else:
        total_rows = pd.read_excel(DATA_PATH, engine="openpyxl").shape[0]

        for start_row in range(start_chunk * CHUNK_SIZE, total_rows, CHUNK_SIZE):
            chunk_idx = start_row // CHUNK_SIZE
            print(f"\nProcesando filas {start_row+1} a {min(start_row+CHUNK_SIZE, total_rows)} (chunk {chunk_idx})")

            chunk = pd.read_excel(
                DATA_PATH,
                engine="openpyxl",
                skiprows=range(1, start_row+1),
                nrows=CHUNK_SIZE,
                dtype={"RUC": str, "N_CHASIS": str, "N_MOTOR": str, "PLACA": str}
            )
            # dejar None en celdas vacías y normalizar
            chunk = chunk.rename(columns=lambda c: c.strip() if isinstance(c, str) else c)
            chunk = chunk.where(chunk.notnull(), None)
            for col in ("N_CHASIS", "N_MOTOR"):
                if col in chunk.columns:
                    chunk[col] = chunk[col].apply(normalize_chasis)
            rows_processed = process_chunk(chunk, chunk_idx)
            processed += rows_processed
            save_checkpoint({"last_chunk": chunk_idx, "rows_processed": processed})
            print(f"Chunk {chunk_idx} completado. Filas procesadas totales: {processed}")
        
        # factorizar el procesamiento por chunk en una función para reusar en CSV/Excel
        def process_chunk(chunk, chunk_idx):
            # normalizamos/limpiamos columnas ya realizados por el caller
            # Fechas
            for c in ["FECHA_RESOLUCION","VIGENCIA_HASTA","FECHA_CORTE"]:
                if c in chunk.columns:
                    chunk[c] = chunk[c].apply(parse_date_ymd)

            # ---------- Construir staging ----------
            df_emp = chunk[["RUC","RAZON_SOCIAL"]].copy() if {"RUC","RAZON_SOCIAL"}.issubset(chunk.columns) else pd.DataFrame(columns=["RUC","RAZON_SOCIAL"])
            if not df_emp.empty:
                df_emp["ESTADO_TRIBUTARIO"] = "ACTIVO"
                # dejar RUC como texto en staging; casteo a bigint al insertar en dim_empresa
                df_emp = df_emp.drop_duplicates(subset=["RUC"])

            veh_cols = [c for c in ["PLACA","ANIO_FAB","N_CHASIS","N_MOTOR","MARCA","SERVICIO","CLASE"] if c in chunk.columns]
            df_veh = chunk[veh_cols].copy() if veh_cols else pd.DataFrame(columns=veh_cols)
            if "ANIO_FAB" in df_veh.columns:
                df_veh["ANIO_FAB"] = df_veh["ANIO_FAB"].apply(lambda v: safe_int(v))
            if "PLACA" in df_veh.columns:
                df_veh = df_veh.drop_duplicates(subset=["PLACA"])

            ubi_cols = [c for c in ["UBIGEO","DEPARTAMENTO","PROVINCIA","DISTRITO"] if c in chunk.columns]
            df_ubi = chunk[ubi_cols].copy() if ubi_cols else pd.DataFrame(columns=ubi_cols)
            if "UBIGEO" in df_ubi.columns:
                df_ubi = df_ubi.drop_duplicates(subset=["UBIGEO"])

            df_comb = chunk[["COMBUSTIBLE"]].drop_duplicates() if "COMBUSTIBLE" in chunk.columns else pd.DataFrame(columns=["COMBUSTIBLE"])

            car_cols = [c for c in ["N_ASIENTOS","N_LLANTAS","N_EJES","CARGA_UTIL","P_SECO","P_BRUTO","LARGO","ANCHO","ALTO"] if c in chunk.columns]
            # crea df_car correctamente antes de las conversiones
            df_car = chunk[car_cols].copy() if car_cols else pd.DataFrame(columns=car_cols)
            # Convertir columnas enteras usando safe_int -> int or None (evita "2.0")
            for c in ["N_ASIENTOS","N_LLANTAS","N_EJES"]:
                if c in df_car.columns:
                    df_car[c] = df_car[c].apply(safe_int)
            # Convertir columnas float/numeric usando safe_float
            for c in ["CARGA_UTIL","P_SECO","P_BRUTO","LARGO","ANCHO","ALTO"]:
                if c in df_car.columns:
                    df_car[c] = df_car[c].apply(safe_float)
            if not df_car.empty:
                df_car = df_car.drop_duplicates()

            time_cols = [c for c in ["FECHA_RESOLUCION","VIGENCIA_HASTA","FECHA_CORTE"] if c in chunk.columns]
            df_time = chunk[time_cols].copy() if time_cols else pd.DataFrame(columns=time_cols)
            if "FECHA_CORTE" in df_time.columns:
                df_time["ANIO"] = pd.to_datetime(df_time["FECHA_CORTE"], errors='coerce').dt.year
                df_time["MES"] = pd.to_datetime(df_time["FECHA_CORTE"], errors='coerce').dt.month
            else:
                df_time["ANIO"] = None
                df_time["MES"] = None
            if not df_time.empty:
                df_time = df_time.drop_duplicates()

            # ---------- COPY a staging ----------
            staging_map = {
                "df_emp": ("stg_empresa", ["RUC","RAZON_SOCIAL","ESTADO_TRIBUTARIO"]),
                "df_veh": ("stg_vehiculo", veh_cols),
                "df_ubi": ("stg_ubicacion", ubi_cols),
                "df_comb": ("stg_combustible", ["COMBUSTIBLE"]),
                "df_car": ("stg_carac", car_cols),
                "df_time": ("stg_tiempo", ["FECHA_RESOLUCION","VIGENCIA_HASTA","FECHA_CORTE","ANIO","MES"])
            }

            for df_name, (stg_table, cols) in staging_map.items():
                df = locals()[df_name]
                if df.empty: continue
                if DRY_RUN:
                    print(f"[dry-run] would COPY {len(df)} rows into {stg_table} ({','.join(cols)})")
                else:
                    # preparar copia: formatear columnas enteras para evitar '3.0' en el CSV
                    df_copy = df[cols].copy()
                    int_cols = {"ANIO_FAB", "N_ASIENTOS", "N_LLANTAS", "N_EJES"}
                    for ic in int_cols:
                        if ic in df_copy.columns:
                            df_copy[ic] = df_copy[ic].apply(lambda v: str(int(v)) if v is not None and not (isinstance(v, float) and pd.isna(v)) else None)
                    buf = df_to_buffer_tab(df_copy)
                    cur.copy_expert(f"COPY {stg_table} ({','.join([c.lower() for c in cols])}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')", buf)

            # NO commit aquí: commit al final del chunk tras insertar hechos

            # ---------- INSERT dimensiones ----------
            if not df_emp.empty:
                # casteamos s.ruc a bigint para compararlo con dim_empresa.ruc (bigint)
                cur.execute("""
                    INSERT INTO dim_empresa (ruc, razon_social, estado_tributario)
                    SELECT DISTINCT NULLIF(s.ruc,'')::bigint, s.razon_social, s.estado_tributario
                    FROM stg_empresa s
                    LEFT JOIN dim_empresa d ON d.ruc = NULLIF(s.ruc,'')::bigint
                    WHERE NULLIF(s.ruc,'') IS NOT NULL
                    AND d.ruc IS NULL;
                """)
            if not df_veh.empty:
                cur.execute("""
                    INSERT INTO dim_vehiculo (placa, anio_fab, n_chasis, n_motor, marca, servicio, clase)
                    SELECT DISTINCT s.placa, s.anio_fab, s.n_chasis, s.n_motor, s.marca, s.servicio, s.clase
                    FROM stg_vehiculo s
                    LEFT JOIN dim_vehiculo d ON d.placa = s.placa
                    WHERE s.placa IS NOT NULL AND d.placa IS NULL;
                """)
            if not df_ubi.empty:
                cur.execute("""
                    INSERT INTO dim_ubicacion (ubigeo, departamento, provincia, distrito)
                    SELECT DISTINCT s.ubigeo, s.departamento, s.provincia, s.distrito
                    FROM stg_ubicacion s
                    LEFT JOIN dim_ubicacion d ON d.ubigeo = s.ubigeo
                    WHERE s.ubigeo IS NOT NULL AND d.ubigeo IS NULL;
                """)
            if not df_comb.empty:
                cur.execute("""
                    INSERT INTO dim_combustible (combustible)
                    SELECT DISTINCT s.combustible
                    FROM stg_combustible s
                    LEFT JOIN dim_combustible d ON d.combustible = s.combustible
                    WHERE s.combustible IS NOT NULL AND d.combustible IS NULL;
                """)
            if not df_car.empty:
                cur.execute("""
                    INSERT INTO dim_caracteristicas_tecnicas 
                        (n_asientos, n_llantas, n_ejes, carga_util, p_seco, p_bruto, largo, ancho, alto)
                    SELECT DISTINCT
                        s.n_asientos,
                        s.n_llantas,
                        s.n_ejes,
                        s.carga_util,
                        s.p_seco,
                        s.p_bruto,
                        s.largo,
                        s.ancho,
                        s.alto
                    FROM stg_carac s
                    LEFT JOIN dim_caracteristicas_tecnicas d
                        ON d.n_asientos IS NOT DISTINCT FROM s.n_asientos
                        AND d.n_llantas IS NOT DISTINCT FROM s.n_llantas
                        AND d.n_ejes IS NOT DISTINCT FROM s.n_ejes
                        AND d.carga_util IS NOT DISTINCT FROM s.carga_util
                        AND d.p_seco IS NOT DISTINCT FROM s.p_seco
                        AND d.p_bruto IS NOT DISTINCT FROM s.p_bruto
                        AND d.largo IS NOT DISTINCT FROM s.largo
                        AND d.ancho IS NOT DISTINCT FROM s.ancho
                        AND d.alto IS NOT DISTINCT FROM s.alto
                    WHERE d.id_caracteristicas_tecnicas IS NULL;
                """)
            if not df_time.empty:
                cur.execute("""
                    INSERT INTO dim_tiempo (fecha_resolucion, vigencia_hasta, fecha_corte, anio, mes)
                        SELECT DISTINCT t.fecha_resolucion, t.vigencia_hasta, t.fecha_corte, t.anio, t.mes
                        FROM stg_tiempo t
                    LEFT JOIN dim_tiempo d
                        ON d.fecha_resolucion IS NOT DISTINCT FROM t.fecha_resolucion
                        AND d.vigencia_hasta IS NOT DISTINCT FROM t.vigencia_hasta
                        AND d.fecha_corte IS NOT DISTINCT FROM t.fecha_corte
                    WHERE d.id_tiempo IS NULL;
                """)
            conn.commit()

            # ---------- OBTENER MAPPINGS ids por lotes ----------
            map_emp = {}
            if not df_emp.empty:
                # convertimos RUC a enteros válidos y usamos cast a bigint[] en la consulta
                rucs = [safe_int(x) for x in df_emp["RUC"].tolist() if safe_int(x) is not None]
                if rucs:
                    cur.execute("SELECT id_empresa, ruc FROM dim_empresa WHERE ruc = ANY(%s::bigint[]);", (rucs,))
                    for idv, ruc in cur.fetchall():
                        map_emp[str(ruc)] = idv

            map_veh = {}
            if not df_veh.empty:
                placas = [str(x) for x in df_veh["PLACA"].tolist() if x is not None and x != ""]
                if placas:
                    cur.execute("SELECT id_vehiculo, placa FROM dim_vehiculo WHERE placa = ANY(%s);", (placas,))
                    for idv, placa in cur.fetchall():
                        map_veh[str(placa)] = idv

            map_ubi = {}
            if not df_ubi.empty:
                ubis = [str(x) for x in df_ubi["UBIGEO"].tolist() if x is not None and x != ""]
                if ubis:
                    cur.execute("SELECT id_ubicacion, ubigeo FROM dim_ubicacion WHERE ubigeo = ANY(%s);", (ubis,))
                    for idv, ub in cur.fetchall():
                        map_ubi[str(ub)] = idv

            map_comb = {}
            if not df_comb.empty:
                combs = [str(x) for x in df_comb["COMBUSTIBLE"].tolist() if x is not None and x != ""]
                if combs:
                    cur.execute("SELECT id_combustible, combustible FROM dim_combustible WHERE combustible = ANY(%s);", (combs,))
                    for idv, comb in cur.fetchall():
                        map_comb[str(comb)] = idv

            map_car = {}
            if not df_car.empty:
                for _, r in df_car.iterrows():
                    key = (safe_int(r.get("N_ASIENTOS")), safe_int(r.get("N_LLANTAS")), safe_int(r.get("N_EJES")),
                           safe_float(r.get("CARGA_UTIL")), safe_float(r.get("P_SECO")), safe_float(r.get("P_BRUTO")),
                           safe_float(r.get("LARGO")), safe_float(r.get("ANCHO")), safe_float(r.get("ALTO")))
                    cur.execute("""
                        SELECT id_caracteristicas_tecnicas FROM dim_caracteristicas_tecnicas
                        WHERE n_asientos IS NOT DISTINCT FROM %s
                          AND n_llantas IS NOT DISTINCT FROM %s
                          AND n_ejes IS NOT DISTINCT FROM %s
                          AND carga_util IS NOT DISTINCT FROM %s
                          AND p_seco IS NOT DISTINCT FROM %s
                          AND p_bruto IS NOT DISTINCT FROM %s
                          AND largo IS NOT DISTINCT FROM %s
                          AND ancho IS NOT DISTINCT FROM %s
                          AND alto IS NOT DISTINCT FROM %s
                    """, key)
                    rec = cur.fetchone()
                    if rec:
                        map_car[key] = rec[0]

            # ---------- preparar y bulk-insert hechos ----------
            hechos = []
            for _, r in chunk.iterrows():
                fecha_res = r.get("FECHA_RESOLUCION")
                vig = r.get("VIGENCIA_HASTA")
                fc = r.get("FECHA_CORTE")
                anio = fecha_res.year if hasattr(fecha_res, "year") else None
                mes = fecha_res.month if hasattr(fecha_res, "month") else None

                fr = safe_date(fecha_res)
                vg = safe_date(vig)
                fc_safe = safe_date(fc)
                cur.execute("INSERT INTO dim_tiempo (fecha_resolucion, vigencia_hasta, fecha_corte, anio, mes) VALUES (%s,%s,%s,%s,%s) RETURNING id_tiempo;",
                            (fr, vg, fc_safe, anio, mes))
                id_t = cur.fetchone()[0]

                id_emp = None
                if r.get("RUC"):
                    id_emp = map_emp.get(str(safe_int(r.get("RUC"))))
                    if id_emp is None:
                        cur.execute("SELECT id_empresa FROM dim_empresa WHERE ruc = %s;", (safe_int(r.get("RUC")),))
                        rec = cur.fetchone()
                        if rec: id_emp = rec[0]

                id_veh = None
                if r.get("PLACA"):
                    id_veh = map_veh.get(str(r.get("PLACA")))
                    if id_veh is None:
                        cur.execute("SELECT id_vehiculo FROM dim_vehiculo WHERE placa = %s;", (str(r.get("PLACA")),))
                        rec = cur.fetchone()
                        if rec:
                            id_veh = rec[0]

                id_ubi = None
                if r.get("UBIGEO"):
                    id_ubi = map_ubi.get(str(r.get("UBIGEO")))
                    if id_ubi is None:
                        cur.execute("SELECT id_ubicacion FROM dim_ubicacion WHERE ubigeo = %s;", (str(r.get("UBIGEO")),))
                        rec = cur.fetchone()
                        if rec:
                            id_ubi = rec[0]

                id_comb = None
                if r.get("COMBUSTIBLE"):
                    id_comb = map_comb.get(str(r.get("COMBUSTIBLE")))
                    if id_comb is None:
                        cur.execute("SELECT id_combustible FROM dim_combustible WHERE combustible = %s;", (str(r.get("COMBUSTIBLE")),))
                        rec = cur.fetchone()
                        if rec:
                            id_comb = rec[0]

                car_key = (safe_int(r.get("N_ASIENTOS")), safe_int(r.get("N_LLANTAS")), safe_int(r.get("N_EJES")),
                           safe_float(r.get("CARGA_UTIL")), safe_float(r.get("P_SECO")), safe_float(r.get("P_BRUTO")),
                           safe_float(r.get("LARGO")), safe_float(r.get("ANCHO")), safe_float(r.get("ALTO")))
                id_car = map_car.get(car_key)
                if id_car is None:
                    cur.execute("""
                        SELECT id_caracteristicas_tecnicas FROM dim_caracteristicas_tecnicas
                        WHERE n_asientos IS NOT DISTINCT FROM %s
                          AND n_llantas IS NOT DISTINCT FROM %s
                          AND n_ejes IS NOT DISTINCT FROM %s
                          AND carga_util IS NOT DISTINCT FROM %s
                          AND p_seco IS NOT DISTINCT FROM %s
                          AND p_bruto IS NOT DISTINCT FROM %s
                          AND largo IS NOT DISTINCT FROM %s
                          AND ancho IS NOT DISTINCT FROM %s
                          AND alto IS NOT DISTINCT FROM %s
                    """, car_key)
                    rec = cur.fetchone()
                    if rec: id_car = rec[0]

                hechos.append((id_t, id_ubi, id_emp, id_veh, id_car, id_comb, r.get("PERMISO_OPER")))

            if hechos:
                execute_values(cur,
                               "INSERT INTO hecho_vehiculo_carga (id_tiempo, id_ubicacion, id_empresa, id_vehiculo, id_caracteristicas_tecnicas, id_combustible, permiso_oper) VALUES %s",
                               hechos, page_size=1000)
                conn.commit()

            return len(chunk)

except Exception as e:
    print("Error durante procesamiento:", e)
    # Revertir la transacción del chunk fallido y NO actualizar el checkpoint
    try:
        conn.rollback()
    except Exception:
        pass

finally:
    try:
        drop_temp_staging()
    except Exception as e:
        print("⚠️ Problema al limpiar staging:", e)
    if cur: cur.close()
    if conn: conn.close()
    print("Conexión cerrada. Total filas procesadas (checkpoint):", processed)
