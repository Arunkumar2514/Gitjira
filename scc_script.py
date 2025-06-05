import subprocess
import psycopg2
import re
import os
from datetime import datetime
import configparser
from pathlib import Path

# === CONFIGURATION ===

def load_config():
    config = configparser.ConfigParser()
    project_config = Path('.github_analyzer_config')
    if project_config.exists():
        config.read(project_config)
        return config

    raise FileNotFoundError("No .github_analyzer_config found in project root or home directory")
try:
        config = load_config()
        GITHUB_TOKEN = config.get('GITHUB', 'TOKEN')
        DB_CONFIG = {
            'dbname': config.get('DATABASE', 'NAME'),
            'user': config.get('DATABASE', 'USER'),
            'password': config.get('DATABASE', 'PASSWORD'),
            'host': config.get('DATABASE', 'HOST'),
            'port': config.get('DATABASE', 'PORT')
        }
except Exception as e:
        print(f"Config error: {e}")
        exit(1)

def run_scc(directory):
    print(f"[ℹ️] Running SCC in: {directory}")
    try:
        result = subprocess.run(
            ["scc"],
            cwd=directory,
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except Exception as e:
        print("[✘] Error running SCC:", str(e))
        return None

def parse_scc_output(output):
    lines = output.splitlines()

    # --- Parse language metrics ---
    start_index = next(i for i, line in enumerate(lines) if line.startswith("Language"))
    end_index = next(i for i, line in enumerate(lines) if lines[i].startswith("Total"))

    language_rows = []
    for line in lines[start_index+1:end_index]:
        parts = re.split(r'\s{2,}', line.strip())
        if len(parts) == 7:
            language_rows.append({
                "language": parts[0],
                "files": int(parts[1]),
                "lines": int(parts[2]),
                "blanks": int(parts[3]),
                "comments": int(parts[4]),
                "code": int(parts[5]),
                "complexity": int(parts[6])
            })

    # --- Parse cost estimates ---
    cost_line = next((l for l in lines if "Estimated Cost to Develop" in l), None)
    schedule_line = next((l for l in lines if "Estimated Schedule Effort" in l), None)
    people_line = next((l for l in lines if "Estimated People Required" in l), None)

    cost = float(re.sub(r'[^\d.]', '', cost_line.split()[-1])) if cost_line else 0.0
    schedule = float(re.sub(r'[^\d.]', '', schedule_line.split()[4])) if schedule_line else 0.0
    people = float(re.sub(r'[^\d.]', '', people_line.split()[4])) if people_line else 0.0

    estimates = {
        "cost_usd": cost,
        "schedule_months": schedule,
        "people_required": people
    }

    return language_rows, estimates

def insert_language_metrics(rows, conn, scanned_at, directory):
    cursor = conn.cursor()
    for row in rows:
        cursor.execute("""
            INSERT INTO scc_language_metrics
            (scanned_at, directory, language, files, lines, blanks, comments, code, complexity)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (scanned_at, directory, row["language"], row["files"], row["lines"],
              row["blanks"], row["comments"], row["code"], row["complexity"]))
    conn.commit()
    cursor.close()
    print(f"[✔] Inserted {len(rows)} language records.")

def insert_project_estimates(estimates, conn, scanned_at, directory):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO scc_project_estimates
        (scanned_at, directory, cost_usd, schedule_months, people_required)
        VALUES (%s, %s, %s, %s, %s)
    """, (scanned_at, directory, estimates["cost_usd"], estimates["schedule_months"], estimates["people_required"]))
    conn.commit()
    cursor.close()
    print(f"[✔] Inserted project estimate record.")

if __name__ == "__main__":
    directory = os.getcwd()
    output = run_scc(directory)
    if output:
        scanned_at = datetime.now()
        language_data, estimates_data = parse_scc_output(output)

        conn = psycopg2.connect(**DB_CONFIG)
        insert_language_metrics(language_data, conn, scanned_at, directory)
        insert_project_estimates(estimates_data, conn, scanned_at, directory)
        conn.close()
