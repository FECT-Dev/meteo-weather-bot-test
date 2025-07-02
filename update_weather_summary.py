import os
import re
import pandas as pd
import camelot
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

def safe_number(v):
    """Clean common OCR issues in numeric cells"""
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    v = re.sub(r"[^\d.]", "", v)
    if "TR" in v or not v: return "0.0"
    try: return str(float(v))
    except: return "0.0"

def fuzzy_match(station_raw):
    """Fuzzy match station name"""
    matches = get_close_matches(station_raw, known_stations, n=1, cutoff=0.6)
    return matches[0] if matches else None

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

            # Create empty rows for this date
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            matched = 0

            for table in tables:
                df = table.df

                # Drop header if present
                if df.iloc[0].str.contains("Station").any():
                    df = df.drop(0)

                # Decide table type
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
                    if not station_raw or len(station_raw) < 3:
                        continue

                    station = fuzzy_match(station_raw)
                    if not station:
                        continue

                    if table_type == "Temperature":
                        row_max[station] = safe_number(row["Max"])
                        row_min[station] = safe_number(row["Min"])
                        if "Rainfall" in row:
                            row_rain[station] = safe_number(row["Rainfall"])
                    elif table_type == "RainfallOnly":
                        row_rain[station] = safe_number(row["Rainfall"])

                    matched += 1

            if matched:
                # Fill missing stations with blank
                for s in known_stations:
                    row_max.setdefault(s, "")
                    row_min.setdefault(s, "")
                    row_rain.setdefault(s, "")

                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ {actual_date}: {matched} stations matched.")
            else:
                print(f"⚠️ {file}: No valid stations matched.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# === SAVE ===
if new_rows:
    final_df = pd.DataFrame(new_rows)

    # Order: Date, Type, known_stations
    columns_order = ["Date", "Type"] + known_stations
    final_df = final_df[columns_order]

    summary_df = pd.concat([summary_df, final_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — Total rows: {len(summary_df)}")
else:
    print("⚠️ No new data added.")
