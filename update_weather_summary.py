import os
import re
import pandas as pd
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Match station lines (e.g. "Anuradhapura 32.7 24.8 0.0")
line_pattern = re.compile(r"^([A-Za-z][A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)$")
date_pattern = re.compile(r"ending at 0830SLTS? on this date\s*(\d{4})[.\-](\d{2})[.\-](\d{2})")

# Load previous summary if exists
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# Loop over each date folder in /reports
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for filename in os.listdir(folder_path):
        if not filename.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)

        try:
            # Convert PDF to image
            images = convert_from_path(pdf_path)
            full_text = ""
            for image in images:
                gray = image.convert("L")  # grayscale
                text = pytesseract.image_to_string(gray, lang="eng")
                full_text += text + "\n"

            # Extract date from content
            date_match = date_pattern.search(full_text)
            if not date_match:
                print(f"⚠️ Skipping {filename}, date not found")
                continue

            year, month, day = date_match.groups()
            actual_date = f"{year}-{month}-{day}"

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            # Prepare rows for Rainfall / Max / Min
            row_rain, row_max, row_min = {"Date": actual_date, "Type": "Rainfall"}, {"Date": actual_date, "Type": "Max"}, {"Date": actual_date, "Type": "Min"}

            for line in full_text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    station = station.strip()
                    row_rain[station] = rain_val.replace("TR", "0.0")
                    row_max[station] = max_val.replace("TR", "0.0")
                    row_min[station] = min_val.replace("TR", "0.0")

            new_rows.extend([row_rain, row_max, row_min])

        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

# Save to CSV
if new_rows:
    df_new = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df_new], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print("✅ Summary updated.")
else:
    print("⚠️ No new data added.")
