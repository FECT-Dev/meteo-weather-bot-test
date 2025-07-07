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

    print(f"📂 Processing {pdf}")

    with open(pdf, "rb") as f:
        reader = PyPDF2.PdfReader(f)
        txt = reader.pages[0].extract_text()
        date_match = re.search(r"\d{4}\.\d{2}\.\d{2}", txt)
        actual_date = date_match.group(0).replace(".", "-") if date_match else date_folder

    valid_max, valid_min, valid_rain = {}, {}, {}

    # === Try lattice first, fallback to stream ===
    tables = camelot.read_pdf(pdf, pages="1", flavor="lattice")
    if len(tables) == 0:
        tables = camelot.read_pdf(pdf, pages="1", flavor="stream")
    print(f"✅ Tables found: {len(tables)}")

    for idx, table in enumerate(tables):
        df = table.df

        # === Drop empty rows and extra header rows ===
        df = df[df.iloc[:, 0].str.strip() != ""]
        df = df[~df.iloc[:, 0].str.contains("Station|Meteorological", case=False, na=False)]

        # === Drop trailing empty columns ===
        df = df.dropna(axis=1, how="all")
        df = df.loc[:, ~(df == "").all()]
        print(f"✅ Table shape after cleanup: {df.shape}")

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

        # === Debug output ===
        debug_file = os.path.join(folder, f"debug_table_{idx}.csv")
        df.to_csv(debug_file, index=False)
        print(f"📄 Saved: {debug_file}")

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

    # === Always write 3 rows per date ===
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
    df.to_csv(summary_file, index=False)
    print(f"✅ Updated: {summary_file} — {len(df)} rows")
else:
    print("⚠️ No rows added.")
