import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

date_pattern = re.compile(r"ending at 0830SLTS? on this date\s*(\d{4}\.\d{2}\.\d{2})")
line_pattern = re.compile(r"^([A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)$")

if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

records = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)
        try:
            reader = PdfReader(pdf_path)
            text = "\n".join(p.extract_text() for p in reader.pages if p.extract_text())

            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {file}, skipping...")
                continue
            actual_date = date_match.group(1).replace(".", "-")

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                continue

            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    station = station.strip()
                    row_rain[station] = rain_val
                    row_max[station] = max_val
                    row_min[station] = min_val

            records.extend([row_rain, row_max, row_min])

        except Exception as e:
            print(f"❌ Failed to parse {file}: {e}")

if records:
    new_df = pd.DataFrame(records)
    if not summary_df.empty:
        summary_df = pd.concat([summary_df, new_df], ignore_index=True)
    else:
        summary_df = new_df

    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No new records to add.")
