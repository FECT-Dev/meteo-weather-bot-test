import os
import re
import pandas as pd
import camelot

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

def safe_number(v):
    """ Clean common OCR issues in numeric cells """
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    v = re.sub(r"[^\d.]", "", v)  # remove stray letters
    if "TR" in v or not v: return "0.0"
    try: return str(float(v))
    except: return "0.0"

# === LOAD existing ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

# === MAIN LOOP ===
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path): continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"): continue

        pdf_path = os.path.join(folder_path, file)

        try:
            # --- Use folder name as date ---
            actual_date = date_folder.strip()
            if not re.match(r"\d{4}-\d{2}-\d{2}", actual_date):
                print(f"⚠️ Skipping {file}: folder name not valid date: {actual_date}")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} — already processed.")
                continue

            # === Read with Camelot ===
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
            if not tables:
                print(f"⚠️ {file}: No table found.")
                continue

            df = tables[0].df
            df.columns = ["Station", "Max", "Min", "Rainfall"]
            if df.iloc[0].str.contains("Station").any():
                df = df.drop(0)

            # Debug save
            df.to_csv(os.path.join(folder_path, "camelot_table_debug.csv"), index=False)

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            unmatched = []
            matched = 0

            for _, row in df.iterrows():
                station_raw = str(row["Station"]).replace("\n"," ").strip().title()
                if not station_raw or len(station_raw) < 3:
                    continue

                # Exact match first
                matches = [s for s in known_stations if s.lower() == station_raw.lower()]
                if not matches:
                    # Partial fallback
                    matches = [s for s in known_stations if s.lower() in station_raw.lower()]

                if not matches:
                    unmatched.append(f"{station_raw} | {row['Max']} | {row['Min']} | {row['Rainfall']}")
                    continue

                station = matches[0]
                row_max[station] = safe_number(row["Max"])
                row_min[station] = safe_number(row["Min"])
                row_rain[station] = safe_number(row["Rainfall"])
                matched += 1

            if unmatched:
                with open(os.path.join(folder_path, "unmatched_stations_debug.txt"), "w") as f:
                    f.write("\n".join(unmatched))

            if matched:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ {actual_date}: {matched} stations matched.")
            else:
                print(f"⚠️ {file}: No valid stations matched.")

        except Exception as e:
            print(f"❌ Error: {file}: {e}")

# === SAVE ===
if new_rows:
    final_df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, final_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — Total rows: {len(summary_df)}")
else:
    print("⚠️ No new data added.")
