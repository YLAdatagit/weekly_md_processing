import argparse
import csv
import os
import psycopg2
import re

def _normalize(name: str) -> str:
    n = str(name).strip().lower()
    n = re.sub(r'[^0-9a-z]+', '_', n)
    n = re.sub(r'_+', '_', n).strip('_')
    return n

def main():
    ap = argparse.ArgumentParser(description="Preflight: verify CSV→Postgres table column mapping")
    ap.add_argument("--table", required=True, help="Target table name (e.g., nr_wk2531_bma)")
    ap.add_argument("--csv", required=True, help="CSV path")
    ap.add_argument("--required", nargs="*", default=[], help="Required columns (as in table naming)")
    args = ap.parse_args()

    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "postgres"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
    )
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
        """, (args.table,))
        table_cols = [r[0] for r in cur.fetchall()]
        table_cols_norm = [_normalize(c) for c in table_cols]

        with open(args.csv, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            csv_header_orig = next(reader)
        csv_header_norm = [_normalize(h) for h in csv_header_orig]

        mapping = []
        for i, h_norm in enumerate(csv_header_norm):
            if h_norm in table_cols_norm:
                idx = table_cols_norm.index(h_norm)
                mapping.append((i, csv_header_orig[i], table_cols[idx]))

        extra_in_csv = [csv_header_orig[i] for i,h in enumerate(csv_header_norm) if h not in table_cols_norm]
        missing_in_csv = [c for c in table_cols if _normalize(c) not in csv_header_norm]

        print("=== Preflight Report ===")
        print(f"Table: {args.table}")
        print(f"CSV:   {args.csv}")
        print(f"Table columns: {len(table_cols)}, CSV columns: {len(csv_header_orig)}")
        print("\n-- Mapping (CSV -> Table) --")
        for i, csv_col, tbl_col in mapping[:80]:
            print(f"[{i:03}] {csv_col}  ->  {tbl_col}")
        if len(mapping) > 80:
            print(f"... (showing 80 of {len(mapping)})")

        if extra_in_csv:
            print("\n-- CSV columns without matching table column (ignored) --")
            for c in extra_in_csv:
                print(f"  - {c}")

        if missing_in_csv:
            print("\n-- Table columns missing in CSV (will default/NULL) --")
            for c in missing_in_csv:
                print(f"  - {c}")

        req_norm = {_normalize(r) for r in args.required}
        missing_req = [r for r in req_norm if r not in set(csv_header_norm)]
        if missing_req:
            print("\n!! Missing required columns in CSV:")
            for r in missing_req:
                print(f"  - {r}")
            raise SystemExit(2)

        print("\nPreflight OK.")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
