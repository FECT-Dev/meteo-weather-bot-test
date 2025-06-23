import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Initialize or load the summary table
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame(columns=["Date", "Station", "Type", "Value"])

# Pattern to extract station lines
line_pattern = re.compile(r"^([A-Za-z \-]+)\s+(\d{1,2}\.\d)\s+(\d{1,2}\.\d)\s+(\d{1,3}\.\d)")

# Go through each PDF in reports/YYYY-MM-DD
for date_folder in sorted(os.listdir(reports_folder)):
    date_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(date_path):
        continue

    for file in os.listdir(date_path):
        if not file.endswith(".pdf"):
            continue

        date_str = date_folder  # from folder name
        if (summary_df["Date"] == date_str).any():
            continue  # Skip if already added

        pdf_path = os.path.join(date_path, file)

        try:
            reader = PdfReader(pdf_path)
            text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_temp, min_temp, rainfall = match.groups()
                    summary_df = pd.concat([
                        summary_df,
                        pd.DataFrame([
                            {"Date": date_str, "Station": station.strip(), "Type": "Max", "Value": max_temp},
                            {"Date": date_str, "Station": station.strip(), "Type": "Min", "Value": min_temp},
                            {"Date": date_str, "Station": station.strip(), "Type": "Rainfall", "Value": rainfall},
                        ])
                    ], ignore_index=True)

        except Exception as e:
            print(f"❌ Failed to read {pdf_path}: {e}")

# Save updated summary
summary_df.to_csv(summary_file, index=False)
print("✅ Summary updated:", summary_file)
