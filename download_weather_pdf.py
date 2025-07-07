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
    v = str(v).upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
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

def fuzzy_station(name):
    name = name.lower().strip()
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
    if not os.path.exists(pdf_path):
        print(f"⚠️ Skipping {date_folder}: PDF not found.")
        continue

    print(f"\n📂 Processing {pdf_path}")

    # Extract date
    with open(pdf_path, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        page_text = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", page_text)
        actual_date = date_match.group(0).replace(".", "-") if date_match else date_folder
    print(f"📅 Using date: {actual_date}")

    valid_max, valid_min, valid_rain = {}, {}, {}

    try:
        # Try lattice first
        tables = camelot.read_pdf(pdf_path, pages="1", flavor="lattice")
        if len(tables) == 0:
            print("⚠️ No lattice tables, trying stream...")
            tables = camelot.read_pdf(pdf_path, pages="1", flavor="stream")

        if len(tables) == 0:
            print("❌ No tables found, skipping PDF.")
            continue

        for idx, table in enumerate(tables):
            df = table.df
            debug_file = os.path.join(folder_path, f"debug_table_{idx}.csv")
            df.to_csv(debug_file, index=False)
            print(f"📄 Saved debug: {debug_file}")

            if df.shape[1] >= 3:
                df.columns = ["Station", "Max", "Min", "Rainfall"]
            elif df.shape[1] == 2:
                df.columns = ["Station", "Rainfall"]
            else:
                continue

            if "Station" in df.iloc[0].to_string():
                df = df.iloc[1:]

            for _, row in df.iterrows():
                raw = str(row["Station"]).strip()
                matches = re.findall(r"[A-Za-z][A-Za-z ]+", raw)
                possible = matches[-1].strip() if matches else ""
                station = fuzzy_station(possible)
                if not station:
                    continue

                max_val = safe_number(row.get("Max", ""))
                min_val = safe_number(row.get("Min", ""))
                rain_val = safe_number(row.get("Rainfall", ""), is_rainfall=True)

                if max_val: valid_max[station] = max_val
                if min_val: valid_min[station] = min_val
                if rain_val: valid_rain[station] = rain_val

        row_max = {"Date": actual_date, "Type": "Max"}
        row_min = {"Date": actual_date, "Type": "Min"}
        row_rain = {"Date": actual_date, "Type": "Rainfall"}

        for s in known_stations:
            row_max[s] = valid_max.get(s, "")
            row_min[s] = valid_min.get(s, "")
            row_rain[s] = valid_rain.get(s, "")

        new_rows.extend([row_max, row_min, row_rain])
        print(f"✅ Added rows for {actual_date}")

    except Exception as e:
        print(f"❌ Error processing {pdf_path}: {e}")

# === SAVE CSV ===
if new_rows:
    final_df = pd.DataFrame(new_rows)
    final_df = final_df.reindex(columns=["Date", "Type"] + known_stations)
    final_df.drop_duplicates(subset=["Date", "Type"], keep="last", inplace=True)
    final_df.to_csv(summary_file, index=False)
    print(f"✅ Saved summary: {summary_file} — {len(final_df)} rows")
else:
    print("⚠️ No data added.")
