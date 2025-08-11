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

# Common aliases seen in PDFs
station_aliases = {
    "Maha llluppallama": "Maha Illuppallama",
    "Ratmalana": "Rathmalana",
    "Ratnapura": "Rathnapura",
    "Kurunagala": "Kurunegala",
}

def safe_number(v, is_rainfall=False):
    raw = str(v).strip()
    up = raw.upper()
    # keep NA as-is
    if "NA" in up:
        return "NA"
    # map TR/TRACE to 0.01
    if "TR" in up or "TRACE" in up:
        return "0.01"
    # map dashes to 0.0 (how dept prints “no rain” sometimes)
    if up in ["-", "--"]:
        return "0.0"

    # fix common OCR confusions
    cleaned = raw.replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    cleaned = re.sub(r"[^\d.]", "", cleaned)

    if cleaned in ("", "."):
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

    # --- detect actual date from header (then minus one day) ---
    actual_date = date_folder
    try:
        with open(pdf, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            all_text = "".join(page.extract_text() or "" for page in reader.pages)
            date_match = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", all_text)
            if date_match:
                header_date = date_match.group(0).replace("/", "-").replace(".", "-")
                published = datetime.strptime(header_date, "%Y-%m-%d")
                actual_date = (published - timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"📅 PDF date: {header_date} → {actual_date}")
    except Exception as e:
        print(f"❌ Date parse error: {e}")

    valid_max, valid_min, valid_rain = {}, {}, {}
    unmatched_log = open(os.path.join(folder, "unmatched_stations.log"), "a", encoding="utf-8")

    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text() or ""

            # ---- PASS 1: wide regex across the page ----
            # Allow integers or decimals, plus NA/TR in any slot.
            num = r"(?:NA|TR|\d+(?:\.\d+)?)"
            pattern = rf"([A-Za-z][A-Za-z ]+?)\s+{num}\s+{num}\s+{num}"
            for m in re.finditer(pattern, text):
                st_raw = m.group(1)
                vals = re.findall(num, m.group(0))
                if len(vals) < 3:
                    continue
                max_raw, min_raw, rain_raw = vals[:3]

                station = match_station(st_raw)
                if not station:
                    # try to trim trailing spaces and re-match
                    station = match_station(st_raw.split()[-1])

                if station:
                    max_val  = safe_number(max_raw)
                    min_val  = safe_number(min_raw)
                    rain_val = safe_number(rain_raw, is_rainfall=True)

                    if max_val != "":  valid_max[station]  = max_val
                    if min_val != "":  valid_min[station]  = min_val
                    if rain_val != "": valid_rain[station] = rain_val
                else:
                    unmatched_log.write(f"{date_folder} | NO MATCH: {st_raw}\n")

            # ---- PASS 2: per-line fallback for wrapped rows ----
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            for i, line in enumerate(lines):
                # Check if any known station name is in the line
                hits = [s for s in known_stations if s in line]
                if not hits:
                    continue
                station = hits[0]

                # Take this line + next line (handles wrap)
                combo = line
                if i + 1 < len(lines):
                    combo += " " + lines[i + 1]

                # Grab the last three numeric/NA/TR tokens
                tokens = re.findall(num, combo)
                if len(tokens) >= 3:
                    max_raw, min_raw, rain_raw = tokens[-3], tokens[-2], tokens[-1]
                    max_val  = safe_number(max_raw)
                    min_val  = safe_number(min_raw)
                    rain_val = safe_number(rain_raw, is_rainfall=True)

                    if station not in valid_max and max_val != "":   valid_max[station]  = max_val
                    if station not in valid_min and min_val != "":   valid_min[station]  = min_val
                    if station not in valid_rain and rain_val != "": valid_rain[station] = rain_val

    unmatched_log.close()

    # build rows
    row_max = {"Date": actual_date, "Type": "Max"}
    row_min = {"Date": actual_date, "Type": "Min"}
    row_rain = {"Date": actual_date, "Type": "Rainfall"}
    for s in known_stations:
        row_max[s]  = valid_max.get(s, "")
        row_min[s]  = valid_min.get(s, "")
        row_rain[s] = valid_rain.get(s, "")
    new_rows.extend([row_max, row_min, row_rain])

# === SAVE FINAL ===
if new_rows:
    df = pd.DataFrame(new_rows)
    df = df.reindex(columns=["Date", "Type"] + known_stations)

    def compute_stats(row):
        nums = []
        for s in known_stations:
            val = row.get(s, "")
            if val in ("", "NA"):
                continue
            try:
                nums.append(float(val))
            except:
                pass
        if nums:
            return pd.Series({
                "Average": round(sum(nums) / len(nums), 1),
                "Max": round(max(nums), 1),
                "Min": round(min(nums), 1)
            })
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
