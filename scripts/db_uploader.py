import os
import re
import csv
import uuid
import tempfile
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from pathlib import Path

# Load .env from project root (supports running from scripts/)
env_here = Path(__file__).resolve().parent / ".env"
env_root = Path(__file__).resolve().parents[1] / ".env"
if env_here.exists():
    load_dotenv(env_here)
elif env_root.exists():
    load_dotenv(env_root)

def _normalize(name: str) -> str:
    """Lowercase, trim, replace non-alnum with underscores, collapse repeats."""
    n = str(name).strip().lower()
    n = re.sub(r"[^0-9a-z]+", "_", n)
    n = re.sub(r"_+", "_", n).strip("_")
    return n

def _extract_week_num(week_num: str) -> str:
    """Return 4-digit week like '2531' from inputs like 'wk2531' or '2531'."""
    m = re.search(r"(\d{4})$", str(week_num))
    if not m:
        raise ValueError(f"Invalid week_num: {week_num}. Expect 'wk2531' or '2531'.")
    return m.group(1)

def _table_exists(cur, table_name: str) -> bool:
    cur.execute("""
        SELECT EXISTS (
          SELECT FROM pg_tables WHERE schemaname='public' AND tablename=%s
        )
    """, (table_name,))
    return cur.fetchone()[0]

def _find_latest_base_table(cur, tech: str, region: str, target_week4: str) -> tuple[str, int]:
    """
    Find the latest existing base table for given tech/region with week < target_week.
    Priority: regioned tables (e.g., nr_wk2530_bma), then non-region (e.g., nr_wk2530).
    Returns (base_table_name, base_week_int).
    """
    tech_l = str(tech).lower()
    region_l = str(region).lower()
    target = int(target_week4)

    # 1) Regioned tables first
    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname='public' AND tablename LIKE %s
    """, (f"{tech_l}_wk%_{region_l}",))
    regioned = [r[0] for r in cur.fetchall()]

    w_region = []
    pat_region = re.compile(rf"^{re.escape(tech_l)}_wk(\d{{4}})_{re.escape(region_l)}$", re.IGNORECASE)
    for name in regioned:
        m = pat_region.match(name)
        if m:
            wk = int(m.group(1))
            if wk < target:
                w_region.append((wk, name))
    if w_region:
        wk, name = max(w_region, key=lambda x: x[0])
        return name, wk

    # 2) Fallback: non-region
    cur.execute("""
        SELECT tablename FROM pg_tables
        WHERE schemaname='public' AND tablename LIKE %s
    """, (f"{tech_l}_wk%",))
    non_region = [r[0] for r in cur.fetchall()]

    w_nr = []
    pat_nr = re.compile(rf"^{re.escape(tech_l)}_wk(\d{{4}})$", re.IGNORECASE)
    for name in non_region:
        m = pat_nr.match(name)
        if m:
            wk = int(m.group(1))
            if wk < target:
                w_nr.append((wk, name))
    if w_nr:
        wk, name = max(w_nr, key=lambda x: x[0])
        return name, wk

    raise ValueError(f"No base table found for tech={tech_l}, region={region_l}, week<{target_week4}.")

def upload_to_postgres(region, tech, week_num, csv_path, required_columns=None):
    """
    Create a week table from latest prior base, then COPY CSV by name-mapped columns.
    - Mapping is by header name (normalized), not by position.
    - If CSV has extra columns, we auto-filter into a temp CSV with only mapped columns.
    - Table-only columns default to NULL/DEFAULT.
    """
    tech_l = str(tech).lower()
    region_l = str(region).lower()
    target_week4 = _extract_week_num(week_num)

    cell_column = {"lte": "cell_name", "nr": "nr_cell_name"}.get(tech_l, "cell_name")

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )
    cur = conn.cursor()
    try:
        # Choose base (prefers regioned)
        base_table, base_week = _find_latest_base_table(cur, tech_l, region_l, target_week4)

        table_name = f"{tech_l}_wk{target_week4}_{region_l}"
        if _table_exists(cur, table_name):
            raise ValueError(f"Table {table_name} already exists. Aborting upload.")

        # Create from base (LIKE)
        cur.execute(sql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL);").format(
            sql.Identifier(table_name), sql.Identifier(base_table)
        ))

        # Drop 'site' column if present; add later
        cur.execute(sql.SQL("ALTER TABLE {} DROP COLUMN IF EXISTS site;").format(
            sql.Identifier(table_name)
        ))

        # ---- Read CSV header (original + normalized) ----
        with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            csv_header_orig = next(reader)
        csv_header_norm = [_normalize(h) for h in csv_header_orig]

        # ---- Table columns and normalized list (ordinal order) ----
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
        """, (table_name,))
        table_cols = [r[0] for r in cur.fetchall()]
        table_cols_norm = [_normalize(c) for c in table_cols]

        # ---- Build mapping in CSV order (CSV header -> TABLE column) ----
        mapping_pairs = []  # list of (csv_norm, table_col)
        for h_norm in csv_header_norm:
            if h_norm in table_cols_norm:
                idx = table_cols_norm.index(h_norm)
                mapping_pairs.append((h_norm, table_cols[idx]))

        if not mapping_pairs:
            raise ValueError(f"No overlapping columns between CSV and table {table_name}.")

        # Required-column check (names as in table, case-insensitive OK)
        if required_columns:
            req_norm = {_normalize(r) for r in required_columns}
            missing_req = [r for r in req_norm if r not in set(csv_header_norm)]
            if missing_req:
                raise ValueError(f"Required columns missing in CSV: {missing_req}")

        # Identify extras / missings for logging
        extra_in_csv = [csv_header_orig[i] for i, h in enumerate(csv_header_norm) if h not in table_cols_norm]
        missing_in_csv = [c for c in table_cols if _normalize(c) not in csv_header_norm]

        if extra_in_csv:
            print(f"⚠ CSV has {len(extra_in_csv)} extra column(s) that are not in {table_name}: {extra_in_csv[:8]}{' ...' if len(extra_in_csv)>8 else ''}")
        if missing_in_csv:
            print(f"ℹ {len(missing_in_csv)} table column(s) absent in CSV (will default to NULL/DEFAULT): {missing_in_csv[:8]}{' ...' if len(missing_in_csv)>8 else ''}")

        # Column list for COPY (table side), in CSV order
        copy_cols_table = [tbl for (_, tbl) in mapping_pairs]

        # If CSV contains extras, we MUST filter to match COPY's expected field count
        needs_filter = len(extra_in_csv) > 0

        # Build norm->orig header mapping to write filtered CSV with original header names
        norm_to_orig = {}
        for h in csv_header_orig:
            n = _normalize(h)
            if n not in norm_to_orig:
                norm_to_orig[n] = h

        # CSV headers to keep (original names) aligned with copy_cols_table
        selected_orig_headers = [norm_to_orig[_normalize(tbl)] for tbl in copy_cols_table]

        # Path to read from for COPY
        copy_from_path = csv_path

        if needs_filter:
            tmp_path = Path(tempfile.gettempdir()) / f"{table_name}__filtered_{uuid.uuid4().hex}.csv"
            with open(csv_path, "r", encoding="utf-8-sig", newline="") as src, \
                 open(tmp_path, "w", encoding="utf-8-sig", newline="") as dst:
                reader = csv.DictReader(src)
                writer = csv.DictWriter(dst, fieldnames=selected_orig_headers)
                writer.writeheader()
                for row in reader:
                    # keep only selected headers; if missing value, write empty
                    out = {h: row.get(h, "") for h in selected_orig_headers}
                    writer.writerow(out)
            copy_from_path = str(tmp_path)
            print(f"ℹ Filtered CSV created for COPY: {copy_from_path} (kept {len(selected_orig_headers)} columns)")

        # COPY using explicit column list (table columns) from the filtered (or original) CSV
        copy_sql = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV HEADER DELIMITER ','").format(
            sql.Identifier(table_name),
            sql.SQL(", ").join(sql.Identifier(c) for c in copy_cols_table)
        )
        with open(copy_from_path, "r", encoding="utf-8-sig") as f:
            cur.copy_expert(copy_sql.as_string(cur), f)

        # Add 'site' back and populate based on cell column
        cur.execute(sql.SQL("ALTER TABLE {} ADD COLUMN site TEXT;").format(
            sql.Identifier(table_name)
        ))
        cur.execute(sql.SQL("""
            UPDATE {}
            SET site = CASE
                WHEN {} ~ '[A-Z]{{3}}\\d{{4}}|[A-Z]{{4}}\\d{{3}}'
                THEN substring({} FROM '([A-Z]{{3}}\\d{{4}}|[A-Z]{{4}}\\d{{3}})')
                ELSE 'No Site Name'
            END;
        """).format(
            sql.Identifier(table_name),
            sql.Identifier(cell_column),
            sql.Identifier(cell_column),
        ))

        conn.commit()
        print(f"{table_name} created using base {base_table} (week {base_week}) and data uploaded.")
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
