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

# ✅ Fix common OCR spelling issues
station_aliases = {
    "Maha llluppallama": "Maha Illuppallama",
    "Ratmalana": "Rathmalana",
    "Ratnapura": "Rathnapura",
}

def safe_number(v, is_rainfall=False):
    v = str(v).upper().strip()
    if "TR" in v or "TRACE" in v.lower():
        return "0.1"
    if v in ["-", "--"]:
        return "0.0"
    v = v.replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    v = re.sub(r"[^\d.]", "", v)
    if not v or v == ".":
        return ""
    try:
        f = float(v)
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

    actual_date = date_folder
    try:
        with open(pdf, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text = "".join(page.extract_text() or "" for page in reader.pages)
            date_match = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", text)
            if date_match:
                header_date = date_match.group(0).replace("/", "-").replace(".", "-")
                published = datetime.strptime(header_date, "%Y-%m-%d")
                shifted = published - timedelta(days=1)
                actual_date = shifted.strftime("%Y-%m-%d")
                print(f"📅 PDF date: {header_date} → Shifted to: {actual_date}")
            else:
                print("⚠️ No date found, using folder date.")
    except Exception as e:
        print(f"❌ Date parse error: {e}")

    valid_max, valid_min, valid_rain = {}, {}, {}
    unmatched_log = open(os.path.join(folder, "unmatched_stations.log"), "a")

    with pdfplumber.open(pdf) as pdf_obj:
        for page in pdf_obj.pages:
            text = page.extract_text() or ""
            lines = text.split("\n")
            skip_next = False

            for idx, line in enumerate(lines):
                if skip_next:
                    skip_next = False
                    continue

                if not re.search(r"\d", line):
                    continue

                parts = line.strip().split()
                if len(parts) < 2:
                    continue

                station, nums = None, []

                # ✅ Try 2-3 word names first
                for try_idx in range(2, 4):
                    name_try = " ".join(parts[:try_idx])
                    if match_station(name_try):
                        station = match_station(name_try)
                        nums = parts[try_idx:]
                        break

                # Fallback single word
                if not station:
                    if match_station(parts[0]):
                        station = match_station(parts[0])
                        nums = parts[1:]

                # Final fallback: merge next line for station
                if not station and idx + 1 < len(lines):
                    next_line = lines[idx + 1].strip()
                    combined = line.strip() + " " + next_line
                    parts = combined.split()
                    for try_idx in range(2, 4):
                        name_try = " ".join(parts[:try_idx])
                        if match_station(name_try):
                            station = match_station(name_try)
                            nums = parts[try_idx:]
                            skip_next = True
                            print(f"🔄 Merged for station: {combined}")
                            break

                if not station:
                    unmatched_log.write(f"{date_folder} | NO MATCH: {line}\n")
                    print(f"❌ NO MATCH: {line}")
                    continue

                # ✅ Split stuck numbers like '30.530.8'
                raw_nums = " ".join(nums)
                split_nums = re.findall(r"\d+\.\d+|\d+", raw_nums)
                print(f"⚡ {station}: raw nums {split_nums}")

                # Fallback: merge next line if not enough numbers
                if len(split_nums) < 3 and idx + 1 < len(lines):
                    next_line = lines[idx + 1]
                    more_nums = re.findall(r"\d+\.\d+|\d+", next_line)
                    if more_nums:
                        split_nums += more_nums
                        skip_next = True
                        print(f"🔄 Merged next line for nums: {next_line}")

                max_val = safe_number(split_nums[0]) if len(split_nums) >= 1 else ""
                min_val = safe_number(split_nums[1]) if len(split_nums) >= 2 else ""
                rain_val = safe_number(split_nums[2], is_rainfall=True) if len(split_nums) >= 3 else ""

                if max_val:
                    valid_max[station] = max_val
                if min_val:
                    valid_min[station] = min_val
                if rain_val:
                    valid_rain[station] = rain_val

                print(f"✅ {station} ➜ Max:{max_val} Min:{min_val} Rain:{rain_val}")

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
        nums = []
        for s in known_stations:
            try:
                nums.append(float(row[s]))
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
