import os
import re
import pdfplumber
import pandas as pd
import PyPDF2
from difflib import get_close_matches
from datetime import datetime, timedelta, date

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

# Aliases seen in PDFs
station_aliases = {
    "Maha llluppallama": "Maha Illuppallama",
    "Ratmalana": "Rathmalana",
    "Ratnapura": "Rathnapura",
    "Kurunagala": "Kurunegala",
    "Katugashota": "Katugasthota",
}

# One cell can be a number or NA / TRACE / TR (with possible punctuation/space between T and R)
TOKEN = r"(?:(?i:NA|TRACE|T\W*R)|\d+(?:\.\d+)?)"
TOKEN_RE = re.compile(TOKEN)

def safe_number(v, is_rainfall=False):
    raw = str(v).strip()
    up = raw.upper()
    # normalize variants like "T R", "T.R" etc.
    letters_only = re.sub(r"[^A-Z]", "", up)

    if letters_only == "NA":
        return "NA"
    if letters_only in ("TR", "TRACE"):
        return "0.01"

    # dept sometimes prints dashes
    if up in ["-", "--"]:
        return "0.0" if is_rainfall else ""

    # OCR fixes
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

def meteorological_block(text: str) -> str:
    """Extract only the Meteorological Stations table block."""
    low = text.lower()
    start = low.find("meteorological stations")
    if start == -1:
        start = 0
    stops = []
    for marker in ["hydro catchment areas", "rainfall stations", "other rainfall stations"]:
        p = low.find(marker, start + 1)
        if p != -1:
            stops.append(p)
    end = min(stops) if stops else len(text)
    return text[start:end]

def add_na_rows_for_missing_dates(new_rows, summary_file):
    """Ensure every date from the earliest known to yesterday exists; fill gaps with NA rows."""
    parsed_dates = {r["Date"] for r in new_rows if r.get("Type") == "Max"}

    existing_dates = set()
    if os.path.exists(summary_file):
        try:
            old_df = pd.read_csv(summary_file)
            if "Date" in old_df.columns:
                existing_dates = set(pd.to_datetime(old_df["Date"]).dt.strftime("%Y-%m-%d"))
        except Exception:
            pass

    all_known = parsed_dates | existing_dates
    if not all_known:
        return new_rows

    start_dt = min(datetime.strptime(d, "%Y-%m-%d").date() for d in all_known)
    end_dt = (datetime.now() - timedelta(days=1)).date()

    expected = {
        (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_dt - start_dt).days + 1)
    }

    missing = sorted(expected - all_known)
    if not missing:
        return new_rows

    for d in missing:
        row_max = {"Date": d, "Type": "Max"}
        row_min = {"Date": d, "Type": "Min"}
        row_rain = {"Date": d, "Type": "Rainfall"}
        for s in known_stations:
            row_max[s] = "NA"
            row_min[s] = "NA"
            row_rain[s] = "NA"
        new_rows.extend([row_max, row_min, row_rain])

    return new_rows

# ------------------ MAIN ------------------

new_rows = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder):
        continue

    pdf = os.path.join(folder, f"weather-{date_folder}.pdf")
    if not os.path.exists(pdf):
        continue

    print(f"\nProcessing: {pdf}")

    # Detect actual date from header (then minus one day)
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
    except Exception:
        pass

    valid_max, valid_min, valid_rain = {}, {}, {}
    log_path = os.path.join(folder, "unmatched_stations.log")
    unmatched_log = open(log_path, "a", encoding="utf-8")

    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            full_text = page.extract_text() or ""
            text = meteorological_block(full_text)

            # PASS A: structured capture — Station, Max, Min, Rainfall
            patt = re.compile(rf"([A-Za-z][A-Za-z ]+?)\s+({TOKEN})\s+({TOKEN})\s+({TOKEN})")
            for m in patt.finditer(text):
                st_raw, max_raw, min_raw, rain_raw = m.group(1, 2, 3, 4)
                station = match_station(st_raw)
                if station:
                    max_val  = safe_number(max_raw)
                    min_val  = safe_number(min_raw)
                    rain_val = safe_number(rain_raw, is_rainfall=True)
                    if max_val != "":  valid_max[station]  = max_val
                    if min_val != "":  valid_min[station]  = min_val
                    if rain_val != "": valid_rain[station] = rain_val
                else:
                    unmatched_log.write(f"{actual_date} | NO MATCH: {st_raw}\n")

            # PASS B: line-wrap fallback — use next line as continuation
            lines = [ln.strip() for ln in text.split("\n") if ln.strip()]
            for i, line in enumerate(lines):
                hit = next((s for s in known_stations if s in line), None)
                if not hit:
                    continue
                combo = line + (" " + lines[i + 1] if i + 1 < len(lines) else "")
                right = combo.split(hit, 1)[1] if hit in combo else combo
                tokens = TOKEN_RE.findall(right)
                if len(tokens) >= 3:
                    max_raw, min_raw, rain_raw = tokens[0], tokens[1], tokens[2]
                    max_val  = safe_number(max_raw)
                    min_val  = safe_number(min_raw)
                    rain_val = safe_number(rain_raw, is_rainfall=True)
                    if hit not in valid_max and max_val != "":   valid_max[hit]  = max_val
                    if hit not in valid_min and min_val != "":   valid_min[hit]  = min_val
                    if hit not in valid_rain and rain_val != "": valid_rain[hit] = rain_val

    unmatched_log.close()

    # Build rows for this date (default NA if missing/empty)
    row_max = {"Date": actual_date, "Type": "Max"}
    row_min = {"Date": actual_date, "Type": "Min"}
    row_rain = {"Date": actual_date, "Type": "Rainfall"}
    for s in known_stations:
        v_max = valid_max.get(s, "NA")
        v_min = valid_min.get(s, "NA")
        v_rain = valid_rain.get(s, "NA")
        row_max[s]  = v_max if v_max != "" else "NA"
        row_min[s]  = v_min if v_min != "" else "NA"
        row_rain[s] = v_rain if v_rain != "" else "NA"
    new_rows.extend([row_max, row_min, row_rain])

# Fill any missing dates between earliest known and yesterday with NA rows
new_rows = add_na_rows_for_missing_dates(new_rows, summary_file)

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

    # Merge with old CSV; keep latest rows (so real data later overrides previous NA)
    if os.path.exists(summary_file):
        old_df = pd.read_csv(summary_file)
        df = pd.concat([old_df, df], ignore_index=True)
        df.drop_duplicates(subset=["Date", "Type"], keep="last", inplace=True)

    df.sort_values(["Date", "Type"], inplace=True, ignore_index=True)
    df.to_csv(summary_file, index=False)
    print(f"Saved: {summary_file} — {len(df)} rows")
else:
    print("No rows added.")
