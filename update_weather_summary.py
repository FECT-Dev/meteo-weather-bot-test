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
    v = str(v).upper().replace("O","0").replace("|","1").replace("I","1").replace("l","1").strip()
    v = re.sub(r"[^\d.]", "", v)
    if not v:
        return ""
    try:
        f = float(v)
        if not is_rainfall and (f < -10 or f > 60):
            return ""
        return str(f)
    except:
        return ""

def match_station(name):
    best = get_close_matches(name.lower(), [s.lower() for s in known_stations], n=1, cutoff=0.5)
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

    # === Extract date from PDF and shift it back 1 day ===
    with open(pdf, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        txt = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", txt)

        if date_match:
            header_date = date_match.group(0).replace(".", "-")
            published = datetime.strptime(header_date, "%Y-%m-%d")
            shifted = published - timedelta(days=1)
            actual_date = shifted.strftime("%Y-%m-%d")
            print(f"📅 PDF shows: {header_date} ➜ Shifted to: {actual_date}")
        else:
            actual_date = date_folder
            print(f"⚠️ No header date found, using folder: {actual_date}")

    valid_max, valid_min, valid_rain = {}, {}, {}

    tables = camelot.read_pdf(pdf, pages="1", flavor="lattice")
    if len(tables) == 0:
        tables = camelot.read_pdf(pdf, pages="1", flavor="stream")

    print(f"✅ Tables found: {len(tables)}")

    for idx, table in enumerate(tables):
        df = table.df

        df = df[df.iloc[:, 0].str.strip() != ""]
        df = df[~df.iloc[:, 0].str.contains("Station|Meteorological", case=False, na=False)]
        df = df.dropna(axis=1, how="all")
        df = df.loc[:, ~(df == "").all()]

        if df.shape[1] >= 4:
            df = df.iloc[:, :4]
            df.columns = ["Station", "Max", "Min", "Rainfall"]
        elif df.shape[1] == 3:
            df.columns = ["Station", "Max", "Min"]
        elif df.shape[1] == 2:
            df.columns = ["Station", "Rainfall"]
        else:
            print(f"⚠️ Skipping table {idx}: Unexpected columns ({df.shape[1]})")
            continue

        debug_file = os.path.join(folder, f"debug_table_{idx}.csv")
        df.to_csv(debug_file, index=False)

        for _, row in df.iterrows():
            name = re.findall(r"[A-Za-z][A-Za-z ]+", str(row["Station"]))
            s = match_station(name[-1].strip().title()) if name else ""
            if not s:
                print(f"❌ NO MATCH: {row['Station']}")
                continue

            if "Max" in row:
                val = safe_number(row["Max"])
                if val: valid_max[s] = val
            if "Min" in row:
                val = safe_number(row["Min"])
                if val: valid_min[s] = val
            if "Rainfall" in row:
                val = safe_number(row["Rainfall"], is_rainfall=True)
                if val: valid_rain[s] = val

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
