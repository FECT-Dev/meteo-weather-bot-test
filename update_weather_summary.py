import os
import re
import pandas as pd
import pytesseract
from pdf2image import convert_from_path

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Match lines like: "Anuradhapura 32.7 24.8 0.0"
line_pattern = re.compile(r"^([A-Za-z][A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)$")
# Match date like: 2025.06.19 or 2025-06-19
date_pattern = re.compile(r"ending at 0830SLTS? on this date\s*(\d{4})[.\-](\d{2})[.\-](\d{2})")

# Load existing data
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# Scan all folders in /reports
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)

        try:
            # Use OCR to extract text
            images = convert_from_path(pdf_path)
            text = "\n".join(pytesseract.image_to_string(img) for img in images)

            # Extract actual date from content
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}, skipping.")
                continue

            year, month, day = date_match.groups()
            actual_date = f"{year}-{month}-{day}"

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            # Create rows for Rainfall / Max / Min
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            for line in text.splitlines():
                line = line.strip()
                match = line_pattern.match(line)
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    station = station.strip()
                    row_rain[station] = rain_val.replace("TR", "0.0")
                    row_max[station] = max_val.replace("TR", "0.0")
                    row_min[station] = min_val.replace("TR", "0.0")

            new_rows.extend([row_rain, row_max, row_min])

        except Exception as e:
            print(f"❌ Error reading {filename}: {e}")

# Save updated summary
if new_rows:
    new_df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, new_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No valid data found.")
