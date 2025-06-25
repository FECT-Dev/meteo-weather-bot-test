import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from PIL import Image

# Folder setup
reports_folder = "reports"
summary_file = "weather_summary.csv"

# Known station names (English only) for validation
known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunagala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# Regex for station data
line_pattern = re.compile(rf"^({'|'.join(re.escape(s) for s in known_stations)})\s+(TR|\d+\.\d)\s+(TR|\d+\.\d)\s+(TR|\d+\.\d)$")
date_pattern = re.compile(r"0830\s*SLTS?.*?(?:date)?\s*[:\-]?\s*([0-9]{{4}})[.\-/ ]([0-9]{{1,2}})[.\-/ ]([0-9]{{1,2}})", re.IGNORECASE)

# Image cleaner
def clean_image(img):
    return img.convert("L").point(lambda x: 0 if x < 150 else 255, "1")

# Load existing summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# Loop through PDFs
for folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)

        try:
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join(pytesseract.image_to_string(clean_image(img), lang="eng") for img in images)

            # Extract date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {file}")
                continue

            y, m, d = map(int, date_match.groups())
            actual_date = f"{y:04d}-{m:02d}-{d:02d}"

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} (already added)")
                continue

            # Initialize rows
            row_rain, row_max, row_min = {"Date": actual_date, "Type": "Rainfall"}, {"Date": actual_date, "Type": "Max"}, {"Date": actual_date, "Type": "Min"}
            found = False

            for line in text.splitlines():
                line = line.strip()
                match = line_pattern.match(line)
                if match:
                    station, max_v, min_v, rain_v = match.groups()
                    station = station.strip()
                    row_max[station] = max_v.replace("TR", "0.0")
                    row_min[station] = min_v.replace("TR", "0.0")
                    row_rain[station] = rain_v.replace("TR", "0.0")
                    found = True

            if found:
                new_rows.extend([row_rain, row_max, row_min])
            else:
                print(f"❌ No valid stations found in {file}")

        except Exception as e:
            print(f"❌ Error reading {file}: {e}")

# Save
if new_rows:
    final_df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, final_df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print("✅ Weather summary updated:", summary_file)
else:
    print("⚠️ No valid data to update.")
