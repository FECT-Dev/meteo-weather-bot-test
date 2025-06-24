import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Updated regex to handle station names, TR values, and extra spacing
line_pattern = re.compile(r"([A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)")
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLST on this date\s*([\d\.]+)")

# Load existing summary if available
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame(columns=["Date", "Type", "Station", "Value"])

new_rows = []

# Go through reports/YYYY-MM-DD folders
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

            # Extract actual date from PDF content
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}, skipping.")
                continue

            actual_date = date_match.group(1).replace(".", "-")

            # Skip if already in summary
            if (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_temp, min_temp, rainfall = match.groups()
                    station = station.strip()

                    for data_type, value in [
                        ("Rainfall", rainfall),
                        ("Max", max_temp),
                        ("Min", min_temp),
                    ]:
                        new_rows.append({
                            "Date": actual_date,
                            "Type": data_type,
                            "Station": station,
                            "Value": value
                        })

        except Exception as e:
            print(f"❌ Error reading {pdf_path}: {e}")

# Append new data and save
if new_rows:
    new_df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, new_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No new records found.")
