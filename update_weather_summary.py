import os
import re
import pandas as pd
import camelot
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

def safe_number(v, is_rainfall=False):
    v = str(v).upper().strip()
    if "TR" in v:
        return "0.1"
    if v in ["-", "--"]:
        return "0.0"
    v = v.replace("O","0").replace("|","1").replace("I","1").replace("l","1")
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
    best = get_close_matches(name.lower(), [s.lower() for s in known_stations], n=1, cutoff=0.4)
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

    actual_date = date_folder  # fallback if header missing
    try:
        with open(pdf, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text = reader.pages[0].extract_text()
            date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", text)
            if date_match:
                header_date = date_match.group(0).replace(".", "-")
                published = datetime.strptime(header_date, "%Y-%m-%d")
                shifted = published - timedelta(days=1)
                actual_date = shifted.strftime("%Y-%m-%d")
                print(f"📅 PDF date: {header_date} → Shifted: {actual_date}")
    except Exception as e:
        print(f"❌ Date parse error: {e}")

    valid_max, valid_min, valid_rain = {}, {}, {}

    tables = camelot.read_pdf(pdf, pages="1", flavor="lattice")
    if len(tables) == 0:
        print("⚠️ Lattice gave nothing — trying stream...")
        tables = camelot.read_pdf(pdf, pages="1", flavor="stream")

    print(f"✅ Tables found: {len(tables)}")

    for idx, table in enumerate(tables):
        df = table.df
        df = df[df.iloc[:, 0].str.strip() != ""]
        df = df[~df.iloc[:, 0].str.contains("Station|Meteorological", case=False, na=False)]

        debug_file = os.path.join(folder, f"debug_table_{idx}.csv")
        df.to_csv(debug_file, index=False)

        for _, row in df.iterrows():
            # Combine all cells to catch split lines
            text_row = " ".join(str(cell) for cell in row).replace("\n"," ")
            name_match = re.findall(r"[A-Za-z][A-Za-z ]+", text_row)
            station = match_station(name_match[-1].strip().title()) if name_match else ""
            if not station:
                print(f"❌ NO MATCH: {text_row}")
                continue

            # Catch both decimals and integers
            nums = re.findall(r"\d+\.\d+|\d+", text_row)
            max_val = safe_number(nums[0]) if len(nums) >= 1 else ""
            min_val = safe_number(nums[1]) if len(nums) >= 2 else ""
            rain_val = safe_number(nums[2], is_rainfall=True) if len(nums) >= 3 else ""

            if max_val: valid_max[station] = max_val
            if min_val: valid_min[station] = min_val
            if rain_val: valid_rain[station] = rain_val

            print(f"✅ {station} ➜ Max:{max_val} Min:{min_val} Rain:{rain_val}")

    row_max = {"Date": actual_date, "Type": "Max"}
    row_min = {"Date": actual_date, "Type": "Min"}
    row_rain = {"Date": actual_date, "Type": "Rainfall"}

    for s in known_stations:
        row_max[s] = valid_max.get(s, "")
        row_min[s] = valid_min.get(s, "")
        row_rain[s] = valid_rain.get(s, "")

    new_rows.extend([row_max, row_min, row_rain])

# === FINAL SAVE ===
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
                "Average": round(sum(nums)/len(nums), 1),
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
