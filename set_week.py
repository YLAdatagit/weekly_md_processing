import sys
from pathlib import Path
import subprocess
import re

def validate_week_format(week_str):
    return re.match(r"^WK\d{4}$", week_str) is not None

def update_env_week(week_str):
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.exists():
        print(f"❌ .env file not found at {env_path}")
        sys.exit(1)

    lines = env_path.read_text().splitlines()
    updated_lines = []
    found = False

    for line in lines:
        if line.startswith("WEEK_NUM="):
            updated_lines.append(f"WEEK_NUM={week_str}")
            found = True
        else:
            updated_lines.append(line)

    if not found:
        updated_lines.append(f"WEEK_NUM={week_str}")

    env_path.write_text("\n".join(updated_lines))
    print(f"✅ .env updated: WEEK_NUM={week_str}")

def run_main_script():
    print("🚀 Running scripts.main...")
    result = subprocess.run(["python", "-m", "scripts.main"], check=False)
    if result.returncode == 0:
        print("✅ scripts.main ran successfully.")
    else:
        print("❌ Error running scripts.main.")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python set_week.py WK2523")
        sys.exit(1)

    week_value = sys.argv[1].upper()

    if not validate_week_format(week_value):
        print("❌ Invalid WEEK_NUM format. Use like: WK2523 (2-digit year + 2-digit week)")
        sys.exit(1)

    update_env_week(week_value)
    run_main_script()
