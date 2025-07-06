# Weekly MD Processing Pipeline

This project automates the extraction, transformation, and loading of "MasterDB" weekly data.

## Features

- Extracts weekly Excel files from RAR archives using WinRAR
- Cleans LTE and NR worksheets into CSV format
- Uploads cleaned CSVs into a PostgreSQL database
- Generates a flattened GeoJSON file for site data

## Requirements

- Python 3.9+
- WinRAR available at `C:\Program Files\WinRAR\WinRAR.exe`
- PostgreSQL database
- Python packages:
  - pandas
  - geopandas
  - shapely
  - psycopg2
  - python-dotenv
  - rarfile

## Setup

1. Install the required Python packages:
   ```bash
   pip install pandas geopandas shapely psycopg2 python-dotenv rarfile
   ```
2. Create a `.env` file in the project root with the following variables:
   ```bash
   BASE_DIR=/path/to/data
   OUTPUT_DIR=/path/to/output
   DB_NADB_USER=postgres   # used as both user and password
   WEEK_NUM=WK2523         # optional; auto-generated if omitted
   ```
3. Ensure WinRAR is installed and accessible at the path mentioned above.

## Usage

To run the entire process for a specific week:
```bash
python set_week.py WK2523
```
This updates the `WEEK_NUM` in `.env` and runs the sync.

You can also run the main script directly:
```bash
python -m scripts.main
```

Outputs are written to the directory configured in `OUTPUT_DIR` and logs are stored in `sync_log.log`.

