import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Regex to match: Station | Max | Min | Rain
line_pattern = re.compile(
    r"^([A-Za-z][A-Za-z \-]{2,})\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,3}(?:\.\d)?)$"
)

# Date format: ending at 0830SLT on this date 2025.06.20
date_pattern = re.compile(
    r"ending at 0830SLTS? on this date[\s:]*([0-9]{4})[.\-/]([0-9]{2})[.\-/]([0-9]{2})"
)

# Load existing CSV
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# Iterate through report folders
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for filename in os.listdir(folder_path):
        if not filename.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)
        try:
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join(pytesseract.image_to_string(img) for img in images)

            print(f"\n🔍 OCR preview from {filename}:\n" + "\n".join(text.splitlines()[:20]))

            # Extract actual date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}")
                continue

            actual_date = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            found = False
            for line in text.splitlines():
                line = line.strip()
                match = line_pattern.match(line)
                if match:
                    station = match.group(1).strip().title()
                    max_val = match.group(2).replace("TR", "0.0")
                    min_val = match.group(3).replace("TR", "0.0")
                    rain_val = match.group(4).replace("TR", "0.0")

                    row_max[station] = max_val
                    row_min[station] = min_val
                    row_rain[station] = rain_val
                    found = True

            if found:
                new_rows.extend([row_rain, row_max, row_min])
            else:
                print(f"❌ No matching station lines found in {filename}")

        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")

# Save output
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No valid station data found.")
