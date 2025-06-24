import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Pattern to extract lines: Station + Max + Min + Rain
line_pattern = re.compile(r"([A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)")
# Pattern to extract actual date
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLTS? on this date\s*([\d\.]+)")

# Load existing summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_data = []

# Loop through folders like reports/2025-06-19
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for filename in os.listdir(folder_path):
        if not filename.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)

        try:
            reader = PdfReader(pdf_path)
            text = "\n".join(p.extract_text() for p in reader.pages if p.extract_text())

            # Extract date from text
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Skipping {filename}, date not found")
                continue

            actual_date = date_match.group(1).replace(".", "-")

            # Skip if already added
            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already in summary.")
                continue

            # Dict to hold one day's record
            record_max, record_min, record_rain = {"Date": actual_date, "Type": "Max"}, {"Date": actual_date, "Type": "Min"}, {"Date": actual_date, "Type": "Rainfall"}

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    station = station.strip()
                    record_max[station] = max_val
                    record_min[station] = min_val
                    record_rain[station] = rain_val

            new_data.extend([record_rain, record_max, record_min])

        except Exception as e:
            print(f"❌ Error reading {pdf_path}: {e}")

# Update CSV
if new_data:
    df = pd.DataFrame(new_data)
    if summary_df.empty:
        summary_df = df
    else:
        summary_df = pd.concat([summary_df, df], ignore_index=True)

    summary_df.to_csv(summary_file, index=False)
    print("✅ Summary updated:", summary_file)
else:
    print("⚠️ No new data found.")
