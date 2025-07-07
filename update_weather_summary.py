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

# Optional: fix common OCR errors
station_aliases = {
    "Maha llluppallama": "Maha Illuppallama",
    "Ratmalana": "Rathmalana",
    "Ratnapura": "Rathnapura",
}

def safe_number(v, is_rainfall=False):
    v = str(v).upper().strip()
    if "TR" in v or "Tr" in v or "TRACE" in v.lower():
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
            text = ""
            for page in reader.pages:
                text += page.extract_text() or ""
            date_match = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", text)
            if date_match:
                header_date = date_match.group(0).replace("/", "-").replace(".", "-")
                published = datetime.strptime(header_date, "%Y-%m-%d")
                shifted = published - timedelta(days=1)
                actual_date = shifted.strftime("%Y-%m-%d")
                print(f"📅 PDF date: {header_date} → Shifted to: {actual_date}")
            else:
                print("⚠️ No date found in PDF header, using folder date without shift.")
    except Exception as e:
        print(f"❌ Date parse error: {e}")

    valid_max, valid_min, valid_rain = {}, {}, {}

    # Force stream mode with better edge tolerance
    tables = camelot.read_pdf(
        pdf,
        pages="1",
        flavor="stream",
        edge_tol=500,
        strip_text="\n"
    )

    print(f"✅ Tables found: {len(tables)}")

    unmatched_log = open(os.path.join(folder, "unmatched_stations.log"), "a")

    for idx, table in enumerate(tables):
        df = table.df
        df = df[df.iloc[:, 0].str.strip() != ""]
        df = df[~df.iloc[:, 0].str.contains("Station|Meteorological", case=False, na=False)]

        debug_file = os.path.join(folder, f"debug_table_{idx}.csv")
        df.to_csv(debug_file, index=False)

        for _, row in df.iterrows():
            station_raw = str(row["Station"]) if "Station" in row else " ".join(str(c) for c in row)
            name_match = re.findall(r"[A-Za-z][A-Za-z ]+", station_raw)
            station = match_station(name_match[-1].strip().title()) if name_match else ""
            if not station:
                unmatched_log.write(f"{date_folder} | NO MATCH: {station_raw}\n")
                print(f"❌ NO MATCH: {station_raw}")
                continue

            max_val = safe_number(row["Max"]) if "Max" in row else ""
            min_val = safe_number(row["Min"]) if "Min" in row else ""
            rain_val = safe_number(row["Rainfall"], is_rainfall=True) if "Rainfall" in row else ""

            if not max_val or not min_val or not rain_val:
                text_row = " ".join(str(c) for c in row)
                nums = re.findall(r"\d+\.?\d*|\.\d+", text_row)
                print(f"⚡ Fallback nums for {station}: {nums}")
                if len(nums) >= 1 and not max_val:
                    max_val = safe_number(nums[0])
                if len(nums) >= 2 and not min_val:
                    min_val = safe_number(nums[1])
                if len(nums) >= 3 and not rain_val:
                    rain_val = safe_number(nums[2], is_rainfall=True)

            if max_val: valid_max[station] = max_val
            if min_val: valid_min[station] = min_val
            if rain_val: valid_rain[station] = rain_val

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
