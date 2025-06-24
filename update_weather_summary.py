import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Pattern for station lines and PDF date
line_pattern = re.compile(r"([A-Za-z \-]+)\s+(\d{1,2}\.\d|TR)\s+(\d{1,2}\.\d|TR)\s+(\d{1,3}\.\d|TR)")
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLST on this date\s*([\d\.]+)")

# Load existing summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame(columns=["Date", "Type", "Station", "Value"])

new_rows = []

# Loop through PDFs in reports/
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

            # Extract date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}, skipping.")
                continue

            actual_date = date_match.group(1).replace(".", "-")

            # Skip if already exists
            if (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Data for {actual_date} already exists.")
                continue

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_val, min_val, rain_val = match.groups()

                    for typ, val in [("Rainfall", rain_val), ("Max", max_val), ("Min", min_val)]:
                        new_rows.append({
                            "Date": actual_date,
                            "Type": typ,
                            "Station": station.strip(),
                            "Value": val.strip()
                        })

        except Exception as e:
            print(f"❌ Error reading {pdf_path}: {e}")

# Append and save
if new_rows:
    updated = pd.concat([summary_df, pd.DataFrame(new_rows)], ignore_index=True)
    updated.to_csv(summary_file, index=False)
    print("✅ Summary updated:", summary_file)
else:
    print("⚠️ No new data found.")
