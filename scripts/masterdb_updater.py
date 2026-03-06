import os
import pandas as pd
import subprocess

def extract_with_winrar(rar_path, extract_to):
    if not os.path.exists(rar_path):
        raise FileNotFoundError(f"RAR file not found: {rar_path}")

    os.makedirs(extract_to, exist_ok=True)

    result = subprocess.run(
        ["C:\\Program Files\\WinRAR\\WinRAR.exe", "x", "-y", rar_path, extract_to],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        raise RuntimeError(f"Extraction failed: {result.stderr}")
    else:
        print(f" Extraction successful: {rar_path}")

def process_master_db(config):
    week = config["week_num"]

    rar_file = config["rar_filename_pattern"].format(week_num=week)
    excel_file = config["excel_filename_pattern"].format(week_num=week)

    rar_path = os.path.join(config["rar_base_path"], rar_file)
    extract_to = config["rar_base_path"]
    output_dir = config["output_folder"]

    # 1) Extract .xlsx from .rar using WinRAR CLI
    extract_with_winrar(rar_path, extract_to)

    # 2) Load Excel and export cleaned CSVs
    excel_path = os.path.join(extract_to, excel_file)
    os.makedirs(output_dir, exist_ok=True)

    result_paths = {}

    for sheet, csv_pattern in config["sheets_to_extract"].items():
        # Read raw (header=None), then promote row index=1 as header
        df = pd.read_excel(excel_path, sheet_name=sheet, header=None)
        df.columns = df.iloc[1]  # promote row 1 (Excel row 2) to header

        # Drop top rows: 0 (row1), 1 (header), 2 (row3), 3 (row4)
        df_cleaned = df.drop(index=[0, 1, 2, 3], errors="ignore").reset_index(drop=True)
        
        rename_map = {"eNodeB Name (NE Name)": "enodeb_name", "gNodeB Name (NE Name)": "gnodeb_name"}

        df_cleaned = df_cleaned.rename(columns={col: rename_map[col] for col in df_cleaned.columns if col in rename_map})

        csv_name = csv_pattern.format(week_num=week.upper().replace("WK", "W"))
        csv_path = os.path.join(output_dir, csv_name)

        # Write with utf-8-sig (Excel-friendly BOM)
        df_cleaned.to_csv(csv_path, index=False, encoding="utf-8-sig")
        result_paths[sheet.lower()] = csv_path
        print(f" Saved cleaned CSV: {csv_path}")

    return result_paths["lte"], result_paths["nr"]
