from pathlib import Path
import sqlite3
import pandas as pd

# =============== CONFIGURAÇÕES FIXAS ===============

CSV_PATH   = r"crop_yield.csv"
DB_PATH    = r"plantio.db"
TABLE_NAME = "plantio_raw"
IF_EXISTS  = "replace"
CHUNKSIZE  = 5000

COLUMN_RENAME_MAP = {
    "yield": "yield_kg_ha",
    "annual_rainfall": "rain_mm",
    "fertilizer": "fertilizer_kg_ha",
    "pesticide": "pesticide_kg_ha",
    "crop_year": "year",
}
# ===================================================


def sanitize_columns(cols):
    cleaned = (
        pd.Series(cols).astype(str).str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    seen = {}
    out = []
    for c in cleaned:
        if not c:
            c = "col"
        base = c
        i = 1
        while c in seen:
            i += 1
            c = f"{base}_{i}"
        seen[c] = True
        out.append(c)
    return out


def read_csv_auto(path, encoding=None, sep=None):
    encodings = [encoding] if encoding else ["utf-8", "latin-1"]
    seps = [sep] if sep else [",", ";"]
    last_exc = None
    for enc in encodings:
        for s in seps:
            try:
                df = pd.read_csv(path, encoding=enc, sep=s)
                return df, enc, s
            except Exception as e:
                last_exc = e
    raise RuntimeError(f"Falha ao ler CSV. Último erro: {last_exc}")


def load_csv_to_sqlite(csv_path: str, db_path: str, table: str, if_exists: str = "replace", chunksize: int = 5000):
    csv = Path(csv_path)
    if not csv.exists():
        raise FileNotFoundError(f"CSV não encontrado: {csv}")

    df, used_enc, used_sep = read_csv_auto(csv)

    if COLUMN_RENAME_MAP:
        norm_map = {k.lower(): v for k, v in COLUMN_RENAME_MAP.items()}
        new_cols = []
        for c in df.columns:
            c_norm = str(c).lower()
            new_cols.append(norm_map.get(c_norm, c))
        df.columns = new_cols

    df.columns = sanitize_columns(df.columns)

    for candidate in ["date", "data", "dt", "data_ref", "periodo", "periodo_ref"]:
        if candidate in df.columns:
            df[candidate] = pd.to_datetime(df[candidate], errors="coerce")

    conn = sqlite3.connect(db_path)
    try:
        # 1) grava a tabela a partir do CSV
        df.to_sql(table, conn, if_exists=if_exists, index=False, chunksize=chunksize)

        cur = conn.cursor()

        # 2) cria índices úteis (se existirem)
        for idx_col in ["farm_id", "field_id", "date"]:
            if idx_col in df.columns:
                cur.execute(f'CREATE INDEX IF NOT EXISTS idx_{table}_{idx_col} ON {table} ({idx_col});')

        # 3) adiciona coluna 'id' (se ainda não existir)
        #    obs: ALTER TABLE ADD COLUMN não permite PK; usamos rowid como fonte e criamos índice único.
        cur.execute(f'PRAGMA table_info("{table}");')
        existing_cols = [r[1].lower() for r in cur.fetchall()]
        if "id" not in existing_cols:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN id INTEGER;')
            # preenche id com o rowid atual
            cur.execute(f'UPDATE {table} SET id = rowid WHERE id IS NULL;')
            # índice único sobre id
            cur.execute(f'CREATE UNIQUE INDEX IF NOT EXISTS idx_{table}_id ON {table} (id);')

        conn.commit()
    finally:
        conn.close()

    print(f"[OK] CSV importado para '{db_path}' tabela '{table}' ({len(df)} linhas).")
    print(f"[INFO] encoding='{used_enc}', sep='{used_sep}', columns={list(df.columns)} + id")


def preview_rows(db_path: str, table: str, limit: int = 10):
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT {limit}", conn)
    finally:
        conn.close()
    print("\n=== PRÉVIA ===")
    print(df)


if __name__ == "__main__":
    load_csv_to_sqlite(CSV_PATH, DB_PATH, TABLE_NAME, if_exists=IF_EXISTS, chunksize=CHUNKSIZE)
    preview_rows(DB_PATH, TABLE_NAME, limit=10)
