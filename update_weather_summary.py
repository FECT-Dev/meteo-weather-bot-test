import os
import re
import pandas as pd
import camelot

# === SETTINGS ===
reports_folder = "reports"
summary_file = "weather_summary.csv"

known_stations = [
    "Anuradhapura","Badulla","Bandarawela","Batticaloa","Colombo","Galle",
    "Hambanthota","Jaffna","Monaragala","Katugasthota","Katunayake","Kurunegala",
    "Maha Illuppallama","Mannar","Polonnaruwa","Nuwara Eliya","Pothuvil",
    "Puttalam","Rathmalana","Rathnapura","Trincomalee","Vavuniya","Mattala",
    "Mullaitivu"
]

def safe_number(v):
    """Fix common OCR weirdness."""
    v = v.upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    if "TR" in v: return "0.0"
    try: return str(float(v))
    except: return "0.0"

# === Load summary ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

# === Process PDFs ===
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path): continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"): continue

        pdf_path = os.path.join(folder_path, file)

        try:
            # === Extract date from PDF filename or folder name ===
            actual_date = date_folder  # safer than OCR date
            if not actual_date:
                print(f"⚠️ Skipping {file}: date not found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists.")
                continue

            # === Extract tables ===
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
            if not tables:
                print(f"⚠️ {file}: No tables found.")
                continue

            df = tables[0].df
            df.columns = ["Station", "Max", "Min", "Rainfall"]

            # Save raw table for debug
            df.to_csv(os.path.join(folder_path, "camelot_table_debug.csv"), index=False)

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            found = False
            unmatched = []

            for i, row in df.iterrows():
                if i == 0: continue  # skip header row
                station_raw = row["Station"].strip().title()
                match = [s for s in known_stations if s.lower() in station_raw.lower()]
                if not match:
                    unmatched.append(station_raw)
                    continue

                station = match[0]
                row_max[station] = safe_number(row["Max"])
                row_min[station] = safe_number(row["Min"])
                row_rain[station] = safe_number(row["Rainfall"])
                found = True

            if unmatched:
                with open(os.path.join(folder_path, "unmatched_stations_debug.txt"), "w") as f:
                    f.write("\n".join(unmatched))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added {actual_date} — Stations: {len(row_max)-2}")
            else:
                print(f"⚠️ {file}: No valid stations matched.")

        except Exception as e:
            print(f"❌ {file} — {e}")

# === Save result ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Final saved: {summary_file} — Total rows: {len(summary_df)}")
else:
    print("⚠️ No new valid data found.")
