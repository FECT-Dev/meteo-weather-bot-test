import os
import re
import pandas as pd
import camelot
import PyPDF2

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
    original = v
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1").strip()
    v = re.sub(r"[^\d.]", "", v)
    if not v:
        print(f"❌ Empty for: '{original}'")
        return ""
    try:
        f = float(v)
        if not is_rainfall and (f == 0.0 or f < -10 or f > 60):
            print(f"❌ Invalid Max/Min: '{original}' ➜ {f}")
            return ""
        print(f"✅ Parsed: '{original}' ➜ {f}")
        return str(f)
    except:
        print(f"❌ Parse error: '{original}'")
        return ""

# === MAIN LOOP ===
new_rows = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    expected_pdf = f"weather-{date_folder}.pdf"
    pdf_path = os.path.join(folder_path, expected_pdf)

    print(f"\n📂 Checking folder: {folder_path}")
    print(f"Files in folder: {os.listdir(folder_path)}")
    print(f"Looking for: {expected_pdf} ➜ Exists: {os.path.exists(pdf_path)}")

    if not os.path.exists(pdf_path):
        print(f"⚠️ Skipping {date_folder}: PDF not found.")
        continue

    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        page_text = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", page_text)
        if date_match:
            actual_date = date_match.group(0).replace(".", "-")
        else:
            actual_date = date_folder.strip()
    print(f"📅 Using date: {actual_date}")

    try:
        tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")
        print(f"🔍 Stream tables found: {len(tables)}")

        if not tables or len(tables) == 0:
            print("⚠️ Trying flavor='lattice' instead...")
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice")
            print(f"🔍 Lattice tables found: {len(tables)}")

        if not tables or len(tables) == 0:
            print(f"❌ No tables found in {expected_pdf} ➜ skipping.")
            continue

        valid_max, valid_min, valid_rain = {}, {}, {}

        for idx, table in enumerate(tables):
            df = table.df
            print(f"\n=== TABLE {idx} RAW ===\n{df}\n")
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

            print(f"=== TABLE {idx} WITH HEADERS ===\n{df}\n")

            for _, row in df.iterrows():
                station_raw = str(row["Station"]).replace("\n", " ").strip()
                matches = re.findall(r"[A-Za-z]+", station_raw)
                english_station = matches[-1].title() if matches else ""

                print(f"🔍 Raw station: '{station_raw}' ➜ English: '{english_station}'")

                if not english_station or english_station not in known_stations:
                    print(f"❌ Not matched: '{english_station}'")
                    continue

                if table_type == "Temperature":
                    max_val = safe_number(row["Max"], is_rainfall=False)
                    min_val = safe_number(row["Min"], is_rainfall=False)
                    rain_val = safe_number(row["Rainfall"], is_rainfall=True) if "Rainfall" in row else ""
                    if max_val:
                        valid_max[english_station] = max_val
                    if min_val:
                        valid_min[english_station] = min_val
                    if rain_val:
                        valid_rain[english_station] = rain_val

                elif table_type == "RainfallOnly":
                    rain_val = safe_number(row["Rainfall"], is_rainfall=True)
                    if rain_val:
                        valid_rain[english_station] = rain_val

        # ✅ Always save 3 rows per date
        row_max = {"Date": actual_date, "Type": "Max"}
        row_min = {"Date": actual_date, "Type": "Min"}
        row_rain = {"Date": actual_date, "Type": "Rainfall"}

        row_max.update(valid_max)
        row_min.update(valid_min)
        row_rain.update(valid_rain)

        new_rows.extend([row_max, row_min, row_rain])
        print(f"✅ {actual_date}: Added Max, Min, Rainfall rows.")
        print(f"✅ Stations with data: Max={len(valid_max)}, Min={len(valid_min)}, Rainfall={len(valid_rain)}")

    except Exception as e:
        print(f"❌ Error processing {expected_pdf}: {e}")

# === FINAL SAVE ===
if new_rows:
    cleaned_rows = []
    for row in new_rows:
        clean = {"Date": row["Date"], "Type": row["Type"]}
        for s in known_stations:
            clean[s] = row.get(s, "")
        cleaned_rows.append(clean)

    final_df = pd.DataFrame(cleaned_rows)
    final_df = final_df.reindex(columns=["Date", "Type"] + known_stations)
    final_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — {len(final_df)} rows")
else:
    print("⚠️ No new data added.")
