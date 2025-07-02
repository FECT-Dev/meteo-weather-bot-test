import os
import re
import pandas as pd
import camelot

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

def safe_number(v):
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    v = re.sub(r"[^\d.]", "", v)
    if not v:
        return ""
    try:
        f = float(v)
        if f < -10 or f > 60:
            return ""
        return str(f)
    except:
        return ""

SKIP_WORDS = [
    "station", "stations", "rainfall", "(mm)", "mm", "rainfall(mm)",
    "rainfall (mm)", "mean", "temp", "temperature", "maximum", "minimum"
]
pattern = r"(" + "|".join(SKIP_WORDS) + r")"

new_rows = []

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

        try:
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
            if not tables:
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            matched_stations = set()

            for idx, table in enumerate(tables):
                df = table.df

                if df.iloc[0].str.contains("Station").any():
                    df = df.drop(0)

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

                    if len(station_raw) < 3 or re.search(pattern, station_raw.lower()):
                        continue

                    matches = [s for s in known_stations if s.lower() == station_raw.lower()]
                    if not matches:
                        continue

                    station = matches[0]
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

            if matched_stations:
                new_rows.extend([row_max, row_min, row_rain])

        except Exception as e:
            print(f"❌ Error: {e}")

# === FINAL: WRITE FRESH ONLY ===
if new_rows:
    cleaned_rows = []
    for row in new_rows:
        clean = {
            "Date": row["Date"],
            "Type": row["Type"]
        }
        for s in known_stations:
            clean[s] = row.get(s, "")
        cleaned_rows.append(clean)

    final_df = pd.DataFrame(cleaned_rows)
    final_df = final_df[["Date", "Type"] + known_stations]]

    # ✅ Completely overwrite — don’t read any old junk
    final_df.to_csv(summary_file, index=False)
    print(f"✅ Overwrote clean: {summary_file}")
else:
    print("⚠️ No new data added.")
