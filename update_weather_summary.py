import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Regex patterns
line_pattern = re.compile(r"^([A-Za-z][A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)$")
date_pattern = re.compile(r"ending at 0830SLTS? on this date\s*(\d{4})[.\-](\d{2})[.\-](\d{2})")

# Load existing summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)

        try:
            # Convert PDF to image
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join([pytesseract.image_to_string(img) for img in images])

            # Extract actual date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {file}")
                continue

            actual_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"
            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    row_rain[station] = rain_val.replace("TR", "0.0")
                    row_max[station] = max_val.replace("TR", "0.0")
                    row_min[station] = min_val.replace("TR", "0.0")

            new_rows.extend([row_rain, row_max, row_min])

        except Exception as e:
            print(f"❌ Failed to process {pdf_path}: {e}")

# Save CSV
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print("✅ Summary table updated:", summary_file)
else:
    print("⚠️ No valid records found.")
