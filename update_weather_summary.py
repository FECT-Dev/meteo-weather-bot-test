import os
import re
import pandas as pd
import camelot
import PyPDF2
from difflib import get_close_matches

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
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").strip()
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
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    pdf_path = os.path.join(folder_path, f"weather-{date_folder}.pdf")
    if not os.path.exists(pdf_path):
        continue

    print(f"📂 Processing {pdf_path}")

    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        page_text = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", page_text)
        actual_date = date_match.group(0).replace(".", "-") if date_match else date_folder

    valid_max, valid_min, valid_rain = {}, {}, {}

    try:
        tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice")
        if len(tables) == 0:
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
        print(f"🔍 Tables found: {len(tables)}")

        for idx, table in enumerate(tables):
            df = table.df
            df.to_csv(os.path.join(folder_path, f"debug_table_{idx}.csv"), index=False)

            # Drop possible multi-header rows
            df = df[~df.iloc[:, 0].str.contains("Station|Meteorological", case=False, na=False)]

            if df.shape[1] >= 3:
                df.columns = ["Station", "Max", "Min", "Rainfall"][:df.shape[1]]
            elif df.shape[1] == 2:
                df.columns = ["Station", "Rainfall"]
            else:
                continue

            for _, row in df.iterrows():
                station_raw = str(row["Station"]).strip()
                matches = re.findall(r"[A-Za-z][A-Za-z ]+", station_raw)
                station_name = matches[-1].strip().title() if matches else ""
                station_name = match_station(station_name)

                if not station_name:
                    print(f"❌ NO MATCH: {station_raw}")
                    continue

                if "Max" in row:
                    max_val = safe_number(row["Max"])
                    if max_val:
                        valid_max[station_name] = max_val
                if "Min" in row:
                    min_val = safe_number(row["Min"])
                    if min_val:
                        valid_min[station_name] = min_val
                if "Rainfall" in row:
                    rain_val = safe_number(row["Rainfall"], is_rainfall=True)
                    if rain_val:
                        valid_rain[station_name] = rain_val

        row_max = {"Date": actual_date, "Type": "Max"}
        row_min = {"Date": actual_date, "Type": "Min"}
        row_rain = {"Date": actual_date, "Type": "Rainfall"}

        for s in known_stations:
            row_max[s] = valid_max.get(s, "")
            row_min[s] = valid_min.get(s, "")
            row_rain[s] = valid_rain.get(s, "")

        new_rows.extend([row_max, row_min, row_rain])

    except Exception as e:
        print(f"❌ Error: {e}")

# === FINAL SAVE ===
if new_rows:
    df = pd.DataFrame(new_rows)
    df = df.reindex(columns=["Date", "Type"] + known_stations)
    df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — {len(df)} rows")
else:
    print("⚠️ No new rows found.")
