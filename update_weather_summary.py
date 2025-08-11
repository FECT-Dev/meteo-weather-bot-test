import os
import re
import pdfplumber
import pandas as pd
import PyPDF2
from difflib import get_close_matches
from datetime import datetime, timedelta

# === CONFIG ===
reports_folder = "reports"
summary_file = "weather_summary.csv"

known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunegala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# Fix common OCR spelling issues
station_aliases = {
    "Maha llluppallama": "Maha Illuppallama",
    "Ratmalana": "Rathmalana",
    "Ratnapura": "Rathnapura",
}

def safe_number(v, is_rainfall=False):
    # Preserve NA and map TR -> 0.01
    raw = str(v).strip()
    up = raw.upper()
    if "NA" in up:
        return "NA"
    if "TR" in up or "TRACE" in up:
        return "0.01"

    if up in ["-", "--"]:
        return "0.0"

    # Clean typical OCR artifacts
    cleaned = raw.replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    cleaned = re.sub(r"[^\d.]", "", cleaned)

    if not cleaned or cleaned == ".":
        return ""

    try:
        f = float(cleaned)
        if not is_rainfall and (f < -10 or f > 60):
            return ""
        return str(f)
    except:
        return ""

def match_station(name):
    name = name.strip().title()
    if name in station_aliases:
        return station_aliases[name]
    best = get_close_matches(name.lower(), [s.lower() for s in known_stations], n=1, cutoff=0.3)
    if best:
        for s in known_stations:
            if s.lower() == best[0]:
                return s
    return None

new_rows = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder):
        continue

    pdf = os.path.join(folder, f"weather-{date_folder}.pdf")
    if not os.path.exists(pdf):
        continue

    print(f"\n📂 Processing: {pdf}")

    # Derive actual date (publish date minus 1)
    actual_date = date_folder
    try:
        with open(pdf, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text_all = "".join(page.extract_text() or "" for page in reader.pages)
            date_match = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", text_all)
            if date_match:
                header_date = date_match.group(0).replace("/", "-").replace(".", "-")
                published = datetime.strptime(header_date, "%Y-%m-%d")
                actual_date = (published - timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"📅 PDF date: {header_date} → Shifted to: {actual_date}")
            else:
                print("⚠️ No date found, using folder date.")
    except Exception as e:
        print(f"❌ Date parse error: {e}")

    valid_max, valid_min, valid_rain = {}, {}, {}
    unmatched_log = open(os.path.join(folder, "unmatched_stations.log"), "a", encoding="utf-8")

    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text() or ""

            # Capture: Station  Max  Min  Rainfall
            # Allow NA in any numeric slot; rainfall also allows TR.
            pattern = r"([A-Za-z][A-Za-z ]+?)\s+(NA|\d+\.\d+)\s+(NA|\d+\.\d+)\s+(NA|TR|\d+\.\d+)"
            for m in re.finditer(pattern, text):
                station = match_station(m.group(1))
                if station:
                    max_val = safe_number(m.group(2))
                    min_val = safe_number(m.group(3))
                    rain_val = safe_number(m.group(4), is_rainfall=True)

                    if max_val != "":
                        valid_max[station] = max_val
                    if min_val != "":
                        valid_min[station] = min_val
                    if rain_val != "":
                        valid_rain[station] = rain_val

                    print(f"✅ {station} ➜ Max:{max_val} Min:{min_val} Rain:{rain_val}")
                else:
                    unmatched_log.write(f"{date_folder} | NO MATCH: {m.group(1)}\n")

    unmatched_log.close()

    row_max = {"Date": actual_date, "Type": "Max"}
    row_min = {"Date": actual_date, "Type": "Min"}
    row_rain = {"Date": actual_date, "Type": "Rainfall"}

    for s in known_stations:
        row_max[s] = valid_max.get(s, "")
        row_min[s] = valid_min.get(s, "")
        row_rain[s] = valid_rain.get(s, "")

    new_rows.extend([row_max, row_min, row_rain])

# === SAVE FINAL ===
if new_rows:
    df = pd.DataFrame(new_rows)
    df = df.reindex(columns=["Date", "Type"] + known_stations)

    def compute_stats(row):
        # Skip NA/blank entries automatically by try/except
        nums = []
        for s in known_stations:
            try:
                val = row[s]
                if val == "NA" or val == "":
                    continue
                nums.append(float(val))
            except:
                pass
        if nums:
            return pd.Series({
                "Average": round(sum(nums) / len(nums), 1),
                "Max": round(max(nums), 1),
                "Min": round(min(nums), 1)
            })
        else:
            return pd.Series({"Average": "", "Max": "", "Min": ""})

    stats = df.apply(compute_stats, axis=1)
    df = pd.concat([df, stats], axis=1)

    if os.path.exists(summary_file):
        old_df = pd.read_csv(summary_file)
        df = pd.concat([old_df, df], ignore_index=True)
        df.drop_duplicates(subset=["Date", "Type"], keep="last", inplace=True)

    df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — {len(df)} rows")
else:
    print("⚠️ No rows added.")
