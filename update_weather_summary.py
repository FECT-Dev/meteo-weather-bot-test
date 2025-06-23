import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Load or create the summary DataFrame
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame(columns=["Date", "Type", "Station", "Value"])

# Regex to extract station data and actual date from text
line_pattern = re.compile(r"^([A-Za-z \-]+)\s+(\d{1,2}\.\d)\s+(\d{1,2}\.\d)\s+(\d{1,3}\.\d)")
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLST on this date\s*([\d\.]+)")

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

            # Extract actual date inside PDF
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}, skipping")
                continue

            actual_date = date_match.group(1).replace(".", "-")
            if (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Data for {actual_date} already exists. Skipping.")
                continue

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_temp, min_temp, rainfall = match.groups()
                    station = station.strip()

                    summary_df = pd.concat([
                        summary_df,
                        pd.DataFrame([
                            {"Date": actual_date, "Type": "Rainfall", "Station": station, "Value": rainfall},
                            {"Date": actual_date, "Type": "Max", "Station": station, "Value": max_temp},
                            {"Date": actual_date, "Type": "Min", "Station": station, "Value": min_temp},
                        ])
                    ], ignore_index=True)

        except Exception as e:
            print(f"❌ Failed to read {pdf_path}: {e}")

# Save updated summary
summary_df.to_csv(summary_file, index=False)
print("✅ Summary table updated:", summary_file)
