# set_date.py — 1) extract RAR  2) make LTE/NR CSV  3) upload to Postgres  4) build GeoJSON
# Usage:
#   py set_date.py WK2531 BMA
#   py set_date.py WK2531 NEA

import argparse
import os
import importlib.util
from pathlib import Path

def _update_env(root: Path, week: str, region: str):
    env_path = root / ".env"
    week_line = f"WEEK_NUM={week}"
    region_line = f"region={region}"
    lines = env_path.read_text(encoding="utf-8").splitlines() if env_path.exists() else []
    def upsert(lines, key_prefix, new_line):
        found = False; out = []
        for line in lines:
            if line.startswith(key_prefix):
                out.append(new_line); found = True
            else:
                out.append(line)
        if not found:
            out.append(new_line)
        return out
    lines = upsert(lines, "WEEK_NUM=", week_line)
    lines = upsert(lines, "region=", region_line)
    env_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Updated {env_path}:\n  {week_line}\n  {region_line}")

def _load_config(root: Path):
    cfg_path = root / "config.py"
    if not cfg_path.exists():
        raise FileNotFoundError(f"config.py not found at {cfg_path}")
    spec = importlib.util.spec_from_file_location("config", cfg_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load config.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    if not hasattr(module, "MASTER_DB_CONFIG"):
        raise RuntimeError("MASTER_DB_CONFIG not defined in config.py")
    return module.MASTER_DB_CONFIG

def main():
    ap = argparse.ArgumentParser(description="Extract → CSV → Upload → GeoJSON for a given week & region")
    ap.add_argument("week", help="wk#### or #### (e.g., WK2531)")
    ap.add_argument("region", help="region code (e.g., BMA, NEA)")
    args = ap.parse_args()

    root = Path(__file__).resolve().parent
    week = args.week
    region = args.region

    # 0) Persist week/region for downstream code
    _update_env(root, week, region)

    # 1) Load config AFTER writing .env
    cfg = _load_config(root)

    # 2) Extract RAR and produce LTE/NR CSVs
    from scripts.masterdb_updater import process_master_db
    print("\n▶ Step 1–2: Extracting RAR and exporting LTE/NR CSVs...")
    lte_csv, nr_csv = process_master_db(cfg)
    print(f"LTE CSV: {lte_csv}")
    print(f"NR  CSV: {nr_csv}")

    # 3) Upload to Postgres (uploader handles column mismatches & filtering)
    from scripts.db_uploader import upload_to_postgres
    print("\n▶ Step 3: Uploading LTE...")
    upload_to_postgres(region, "lte", cfg["week_num"], lte_csv, required_columns=["cell_name", "decom_from"])

    print("▶ Uploading NR...")
    upload_to_postgres(region, "nr", cfg["week_num"], nr_csv, required_columns=["nr_cell_name", "decom_from"])

    # 4) Create GeoJSON
    from scripts.flattened_geojson import create_site_geojson
    print("\n▶ Step 4: Creating GeoJSON...")
    create_site_geojson(lte_csv, nr_csv, cfg["output_folder"], cfg["week_num"])

    print("\n✅ All done.")

if __name__ == "__main__":
    main()
