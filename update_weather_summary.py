import os
import re
import pandas as pd
import camelot
from difflib import get_close_matches

# === CONFIG ===
reports_folder = "reports"
summary_file = "weather_summary.csv"

known_stations = [
    "Anuradhapura","Badulla","Bandarawela","Batticaloa","Colombo","Galle",
    "Hambanthota","Jaffna","Monaragala","Katugasthota","Katunayake","Kurunegala",
    "Maha Illuppallama","Mannar","Polonnaruwa","Nuwara Eliya","Pothuvil",
    "Puttalam","Rathmalana","Rathnapura","Trincomalee","Vavuniya","Mattala",
    "Mullaitivu"
]

# === HELPERS ===
def safe_number(v):
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    v = re.sub(r"[^\d.]", "", v)
    if "TR" in v or not v:
        return ""
    try:
        f = float(v)
        if f > 50 or f < -10:  # unreasonable temp or rainfall
            return ""
        return str(f)
    except:
        return ""

def fuzzy_match(station_raw):
    matches = get_close_matches(station_raw, known_stations, n=1, cutoff=0.75)
    return matches[0] if matches else None

# === HEADERS/JUNK WORDS ===
SKIP_WORDS = [
    "station", "stations", "rainfall", "(mm)", "mm", "rainfall(mm)",
    "rainfall (mm)", "mean", "temp", "temperature", "maximum", "minimum"
]
pattern = r"(" + "|".join(SKIP_WORDS) + r")"

# === LOAD EXISTING ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

# === MAIN LOOP ===
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)
        actual_date = date_folder.strip()
        if not re.match(r"\d{4}-\d{2}-\d{2}", actual_date):
            print(f"⚠️ Skipping {file}: folder name not valid date: {actual_date}")
            continue

        if not summary_df.empty and (summary_df["Date"] == actual_date).any():
            print(f"ℹ️ Skipping {actual_date} — already processed.")
            continue

        try:
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
            if not tables:
                print(f"⚠️ {file}: No table found.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            matched_stations = set()

            for idx, table in enumerate(tables):
                df = table.df

                if df.iloc[0].str.contains("Station").any():
                    df = df.drop(0)

                debug_table_path = os.path.join(folder_path, f"debug_table_{idx}.csv")
                df.to_csv(debug_table_path, index=False)

                if df.shape[1] >= 3:
                    df.columns = ["Station", "Max", "Min", "Rainfall"][:df.shape[1]]
                    table_type = "Temperature"
                elif df.shape[1] == 2:
                    df.columns = ["Station", "Rainfall"]
                    table_type = "RainfallOnly"
                else:
                    continue

                for _, row in df.iterrows():
                    station_raw = str(row["Station"]).replace("\n", " ").strip().title()
                    station_raw = re.sub(r"\s+", " ", station_raw)

                    print(f"🔍 RAW STATION: '{station_raw}'")

                    if len(station_raw) < 3:
                        continue

                    if re.search(pattern, station_raw.lower()):
                        print(f"⛔ SKIPPED HEADER: '{station_raw}'")
                        continue

                    station = fuzzy_match(station_raw)
                    if not station:
                        print(f"❌ UNMATCHED: '{station_raw}'")
                        continue

                    valid = False

                    if table_type == "Temperature":
                        max_val = safe_number(row["Max"])
                        min_val = safe_number(row["Min"])
                        if max_val and min_val:
                            row_max[station] = max_val
                            row_min[station] = min_val
                            valid = True
                        if "Rainfall" in row:
                            rain_val = safe_number(row["Rainfall"])
                            if rain_val:
                                row_rain[station] = rain_val
                                valid = True
                    elif table_type == "RainfallOnly":
                        rain_val = safe_number(row["Rainfall"])
                        if rain_val:
                            row_rain[station] = rain_val
                            valid = True

                    if valid:
                        matched_stations.add(station)
                        print(f"✅ SAVED: {station} | Max:{row_max.get(station,'')} Min:{row_min.get(station,'')} Rain:{row_rain.get(station,'')}")
                    else:
                        print(f"⛔ SKIPPED NO VALID NUMBERS: '{station_raw}' -> {station}")

            if matched_stations:
                for s in known_stations:
                    row_max.setdefault(s, "")
                    row_min.setdefault(s, "")
                    row_rain.setdefault(s, "")
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ {actual_date}: {len(matched_stations)} stations matched.")
            else:
                print(f"⚠️ {file}: No stations matched.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# === SAVE ===
if new_rows:
    final_df = pd.DataFrame(new_rows)
    columns_order = ["Date", "Type"] + known_stations
    final_df = final_df[columns_order]
    summary_df = pd.concat([summary_df, final_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — Total rows: {len(summary_df)}")
else:
    print("⚠️ No new data added.")
