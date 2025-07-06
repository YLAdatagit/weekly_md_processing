from config import  MASTER_DB_CONFIG
from scripts.masterdb_updater import process_master_db
from scripts.db_uploader import upload_to_postgres
from scripts.flattened_geojson import create_site_geojson

import logging
from pathlib import Path

# Ensure /log folder exists
log_path = Path(__file__).resolve().parents[1] / "log"
log_path.mkdir(parents=True, exist_ok=True)

# Define full log file path
log_file = log_path / "sync_log.log"

# Set up logging
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def run_masterdb_sync():
    logging.info("=== MasterDB Sync Started ===")

    try:
        lte_csv, nr_csv = process_master_db(MASTER_DB_CONFIG)
    except Exception as e:
        logging.error(f" MasterDB extraction failed: {e}")
        return  # don’t proceed if extraction failed

    try:
        logging.info("Uploading LTE...")
        upload_to_postgres("lte", MASTER_DB_CONFIG["week_num"], lte_csv)
    except Exception as e:
        logging.error(f" LTE upload failed: {e}")

    try:
        logging.info("Uploading NR...")
        upload_to_postgres("nr", MASTER_DB_CONFIG["week_num"], nr_csv)
    except Exception as e:
        logging.error(f" NR upload failed: {e}")

    logging.info("=== MasterDB Sync Finished ===\n")

    try:
        logging.info("Creating geojson file...")
        create_site_geojson(lte_csv, nr_csv, MASTER_DB_CONFIG['output_folder'], MASTER_DB_CONFIG["week_num"])
    except Exception as e:
        logging.error(f" Creating geojson failed: {e}")

    logging.info("=== Creating geojson Finished ===\n")    


if __name__ == "__main__":
    
    run_masterdb_sync()
