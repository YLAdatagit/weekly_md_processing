from pathlib import Path
from plyer import notification
from dotenv import load_dotenv
import logging
import traceback
import os
from datetime import datetime

from scripts.masterdb_updater import process_master_db
from scripts.db_uploader import upload_to_postgres  # uses the updated uploader
from config import MASTER_DB_CONFIG

# Load .env from project root
env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

region = (os.getenv("region") or "").lower()  # normalize for table names

# Logging
log_path = Path(__file__).resolve().parents[1] / "log"
log_path.mkdir(parents=True, exist_ok=True)
log_file = log_path / "sync_log.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def notify(title, message):
    try:
        notification.notify(title=title, message=message, timeout=5)
    except Exception:
        pass  # Non-blocking if GUI not available

def run_masterdb_sync():
    logging.info("=== MasterDB Sync Started ===")

    try:
        lte_csv, nr_csv = process_master_db(MASTER_DB_CONFIG)
    except Exception as e:
        logging.error(f" MasterDB extraction failed: {e}")
        notify("❌ Sync Failed", "Step: Extract MasterDB")
        return

    # LTE
    try:
        logging.info("Uploading LTE...")
        upload_to_postgres(region, "lte", MASTER_DB_CONFIG["week_num"], lte_csv)
    except Exception as e:
        logging.error(f" LTE upload failed: {e}")
        notify("❌ Sync Failed", "Step: Upload LTE")

    # NR
    try:
        logging.info("Uploading NR...")
        upload_to_postgres(region, "nr", MASTER_DB_CONFIG["week_num"], nr_csv)
    except Exception as e:
        logging.error(f" NR upload failed: {e}")
        notify("❌ Sync Failed", "Step: Upload NR")

    logging.info("=== MasterDB Sync Finished ===")

    try:
        from scripts.flattened_geojson import create_site_geojson
        logging.info("Creating geojson file...")
        create_site_geojson(lte_csv, nr_csv, MASTER_DB_CONFIG['output_folder'], MASTER_DB_CONFIG["week_num"])
    except Exception as e:
        logging.error(f" Creating geojson failed: {e}")
        notify("❌ Sync Failed", "Step: Create GeoJSON")

    logging.info("=== Creating geojson Finished ===")

if __name__ == "__main__":
    start = datetime.now()
    try:
        run_masterdb_sync()
        end = datetime.now()
        notify("✅ MasterDB Sync Completed", f"Duration: {end - start}")
    except Exception as e:
        notify("❌ MasterDB Sync Error", f"{type(e).__name__}: {str(e)}")
        traceback.print_exc()
