import os
import pandas as pd
import rarfile
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

    # 🔓 Step 1: Extract .xlsx from .rar using WinRAR CLI
    extract_with_winrar(rar_path, extract_to)

    # 📄 Step 2: Load Excel and export cleaned CSVs
    excel_path = os.path.join(extract_to, excel_file)
    os.makedirs(output_dir, exist_ok=True)

    result_paths = {}

    for sheet, csv_pattern in config["sheets_to_extract"].items():
        df = pd.read_excel(excel_path, sheet_name=sheet, header = None)
        # Step 2: Promote row 1 (index 1) as header
        df.columns = df.iloc[1]

        # Step 3: Drop rows 0 (Excel row 1), 1 (now header), 2 (Excel row 3), 3 (Excel row 4)
        df_cleaned = df.drop(index=[0, 1, 2, 3], errors='ignore')

        # Step 4: Reset index if needed
        df_cleaned = df_cleaned.reset_index(drop=True)

        csv_name = csv_pattern.format(week_num=week.upper().replace("WK", "W"))
        csv_path = os.path.join(output_dir, csv_name)

        df_cleaned.to_csv(csv_path, index=False)
        result_paths[sheet.lower()] = csv_path
        print(f" Saved cleaned CSV: {csv_path}")

    return result_paths["lte"], result_paths["nr"]


