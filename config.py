from dotenv import load_dotenv
from pathlib import Path
from datetime import date
import os

# === Load .env from project root ===
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# === Helper: auto-generate week if missing ===
def get_week_num():
    val = os.getenv("WEEK_NUM")
    if val:
        return val
    # Auto-generate like WK2527
    iso_year, iso_week, _ = date.today().isocalendar()
    return f"WK{str(iso_year)[-2:]}{iso_week:02d}"

# === Load and validate all required configs ===
base_dir = os.getenv("BASE_DIR")
output_dir = os.getenv("OUTPUT_DIR")
week_num = get_week_num()

if not base_dir:
    raise ValueError("❌ BASE_DIR is not set in .env")
if not output_dir:
    raise ValueError("❌ OUTPUT_DIR is not set in .env")

# === Final config dict ===
MASTER_DB_CONFIG = {
    "rar_base_path": Path(base_dir),
    "week_num": week_num,
    "excel_filename_pattern": f"MasterDB updated process_BMA_{week_num}.xlsx",
    "rar_filename_pattern": f"MasterDB updated process_BMA_{week_num}.rar",
    "sheets_to_extract": {
        "LTE": f"MD_LTE_{week_num}.csv",
        "NR":  f"MD_NR_{week_num}.csv"
    },
    "output_folder": Path(output_dir)
}
