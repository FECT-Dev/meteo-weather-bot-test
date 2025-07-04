import os
import re
import pandas as pd
import camelot
import PyPDF2
from difflib import get_close_matches

# === CONFIG ===
reports_folder = "reports"
summary_file = "weather_summary.csv"
fallback_file = "fallback.csv"

known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunegala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

def safe_number(v, is_rainfall=False):
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1").strip()
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

def clean_station_name(name):
    name = name.lower()
    if "mahalluppallama" in name:
        return "maha illuppallama"
    if "kattunayake" in name:
        return "katunayake"
    return name

def match_station(name):
    name = clean_station_name(name)
    best = get_close_matches(name, [s.lower() for s in known_stations], n=1, cutoff=0.5)
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
    print(f"\n📂 Checking {folder_path}")

    if not os.path.exists(pdf_path):
        print(f"⚠️ Skipping {date_folder}: PDF not found.")
        continue

    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        page_text = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", page_text)
        actual_date = date_match.group(0).replace(".", "-") if date_match else date_folder.strip()
    print(f"📅 Date: {actual_date}")

    valid_max, valid_min, valid_rain = {}, {}, {}

    try:
        tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice")
        print(f"🔍 Lattice tables: {len(tables)}")

        if len(tables) == 0:
            print("⚠️ No tables found with lattice mode.")
            continue

        for idx, table in enumerate(tables):
            df = table.df
            debug_file = os.path.join(folder_path, f"debug_table_{idx}.csv")
            df.to_csv(debug_file, index=False)
            print(f"✅ Saved debug: {debug_file}")

            # Skip header rows
            if "Station" in df.iloc[0, 0] or "Meteorological" in df.iloc[0, 0]:
                df = df.iloc[1:]

            for _, row in df.iterrows():
                station_raw = str(row[0]).strip()
                matches = re.findall(r"[A-Za-z][A-Za-z ]+", station_raw)
                english_station = matches[-1].strip().title() if matches else ""
                english_station = match_station(english_station)
                if not english_station:
                    print(f"❌ NO MATCH for: {station_raw}")
                    continue

                max_val = safe_number(row[1])
                min_val = safe_number(row[2])
                rain_val = safe_number(row[3], is_rainfall=True) if len(row) >= 4 else ""

                if max_val: valid_max[english_station] = max_val
                if min_val: valid_min[english_station] = min_val
                if rain_val: valid_rain[english_station] = rain_val

                print(f"✅ {english_station}: Max={max_val} Min={min_val} Rain={rain_val}")

        row_max = {"Date": actual_date, "Type": "Max"}
        row_min = {"Date": actual_date, "Type": "Min"}
        row_rain = {"Date": actual_date, "Type": "Rainfall"}

        for s in known_stations:
            row_max[s] = valid_max.get(s, "")
            row_min[s] = valid_min.get(s, "")
            row_rain[s] = valid_rain.get(s, "")

        new_rows.extend([row_max, row_min, row_rain])
        print(f"✅ {actual_date}: Max={len(valid_max)} Min={len(valid_min)} Rainfall={len(valid_rain)}")

    except Exception as e:
        print(f"❌ Error processing {pdf_path}: {e}")

# === FINAL SAVE ===
if new_rows:
    final_df = pd.DataFrame(new_rows)
    final_df = final_df.reindex(columns=["Date", "Type"] + known_stations)

    if os.path.exists(fallback_file):
        fallback_df = pd.read_csv(fallback_file)
        final_df = pd.concat([final_df, fallback_df], ignore_index=True)
        print(f"✅ Merged fallback.csv — {len(fallback_df)} rows.")

    # ✅ Remove old rows for same date to force overwrite
    final_df.sort_values(by="Date", inplace=True)
    final_df.drop_duplicates(subset=["Date", "Type"], keep="last", inplace=True)
    final_df.fillna("", inplace=True)

    print("✅ Final rows:")
    print(final_df.tail(5))

    final_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} — {len(final_df)} rows")

else:
    print("⚠️ No new data added.")
