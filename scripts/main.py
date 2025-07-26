from config import MASTER_DB_CONFIG
from scripts.masterdb_updater import process_master_db
from scripts.db_uploader import upload_to_postgres
from scripts.flattened_geojson import create_site_geojson

import logging
from pathlib import Path
from plyer import notification
import traceback
from datetime import datetime

# === Logging Setup ===
log_path = Path(__file__).resolve().parents[1] / "log"
log_path.mkdir(parents=True, exist_ok=True)
log_file = log_path / "sync_log.log"

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# === Notification Helper ===
def notify(title, message):
    notification.notify(
        title=title,
        message=message,
        timeout=5
    )

# === Main Sync Function ===
def run_masterdb_sync():
    logging.info("=== MasterDB Sync Started ===")

    try:
        lte_csv, nr_csv = process_master_db(MASTER_DB_CONFIG)
    except Exception as e:
        logging.error(f" MasterDB extraction failed: {e}")
        notify("❌ Sync Failed", "Step: Extract MasterDB")
        return

    try:
        logging.info("Uploading LTE...")
        upload_to_postgres("lte", MASTER_DB_CONFIG["week_num"], lte_csv)
    except Exception as e:
        logging.error(f" LTE upload failed: {e}")
        notify("❌ Sync Failed", "Step: Upload LTE")

    try:
        logging.info("Uploading NR...")
        upload_to_postgres("nr", MASTER_DB_CONFIG["week_num"], nr_csv)
    except Exception as e:
        logging.error(f" NR upload failed: {e}")
        notify("❌ Sync Failed", "Step: Upload NR")

    logging.info("=== MasterDB Sync Finished ===")

    try:
        logging.info("Creating geojson file...")
        create_site_geojson(lte_csv, nr_csv, MASTER_DB_CONFIG['output_folder'], MASTER_DB_CONFIG["week_num"])
    except Exception as e:
        logging.error(f" Creating geojson failed: {e}")
        notify("❌ Sync Failed", "Step: Create GeoJSON")

    logging.info("=== Creating geojson Finished ===")

# === Wrapper with Duration + Final Notification ===
if __name__ == "__main__":
    start = datetime.now()
    try:
        run_masterdb_sync()
        end = datetime.now()
        notify("✅ MasterDB Sync Completed", f"Duration: {end - start}")
    except Exception as e:
        notify("❌ MasterDB Sync Error", f"{type(e).__name__}: {str(e)}")
        traceback.print_exc()
