import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Patterns
line_pattern = re.compile(r"^([A-Za-z \-]+)\s+(\d{1,2}\.\d)\s+(\d{1,2}\.\d)\s+(\d{1,3}\.\d)")
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLST on this date\s*([\d\.]+)")

# Temporary list to store new data
records = []

# Loop through reports
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
            text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())

            # Get actual date from PDF content
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}, skipping.")
                continue

            actual_date = date_match.group(1).replace(".", "-")

            # Skip if already processed
            if os.path.exists(summary_file):
                existing = pd.read_csv(summary_file)
                if (existing["Date"] == actual_date).any():
                    print(f"ℹ️ Skipping {actual_date}, already processed.")
                    continue

            # Extract lines
            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_temp, min_temp, rainfall = match.groups()
                    station = station.strip()
                    records.append({"Date": actual_date, "Type": "Rainfall", station: float(rainfall)})
                    records.append({"Date": actual_date, "Type": "Max", station: float(max_temp)})
                    records.append({"Date": actual_date, "Type": "Min", station: float(min_temp)})

        except Exception as e:
            print(f"❌ Error reading {pdf_path}: {e}")

# Create final DataFrame and pivot
if records:
    df = pd.DataFrame(records)
    summary = df.groupby(["Date", "Type"]).agg(lambda x: x.iloc[0]).reset_index()
    summary = summary.fillna("...")
    summary.to_csv(summary_file, index=False)
    print("✅ Summary table updated:", summary_file)
else:
    print("⚠️ No new data found. Summary not updated.")
