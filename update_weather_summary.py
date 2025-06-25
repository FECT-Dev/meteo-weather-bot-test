import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from PIL import Image

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Regex patterns
line_pattern = re.compile(
    r"^([A-Za-z][A-Za-z \-]{2,})\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,3}(?:\.\d)?)$"
)
date_pattern = re.compile(
    r"0830\s*SLTS?.*?(?:date)?\s*[:\-]?\s*([0-9]{4})[\.\-/ ]([0-9]{1,2})[\.\-/ ]([0-9]{1,2})",
    re.IGNORECASE
)

# Image preprocessing to improve OCR
def clean_ocr_image(image: Image.Image) -> Image.Image:
    return image.convert("L").point(lambda x: 0 if x < 150 else 255, "1")

# Load existing summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# Go through each PDF
for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.lower().endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)

        try:
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join(
                pytesseract.image_to_string(clean_ocr_image(img), lang="eng")
                for img in images
            )

            print(f"\n📄 OCR preview from '{file}':\n" + "\n".join(text.splitlines()[:15]))

            # Extract date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {file}. Skipping.")
                continue

            year, month, day = map(int, date_match.groups())
            actual_date = f"{year:04d}-{month:02d}-{day:02d}"

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            found_station = False

            for line in text.splitlines():
                line = line.strip()
                match = line_pattern.match(line)
                if match:
                    station = match.group(1).strip().title()
                    # skip invalid garbage lines
                    if not station.replace(" ", "").isalpha():
                        continue

                    max_val = match.group(2).replace("TR", "0.0")
                    min_val = match.group(3).replace("TR", "0.0")
                    rain_val = match.group(4).replace("TR", "0.0")

                    row_max[station] = max_val
                    row_min[station] = min_val
                    row_rain[station] = rain_val
                    found_station = True

            if found_station:
                new_rows.extend([row_rain, row_max, row_min])
            else:
                print(f"❌ No station lines matched in {file}.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# Save final summary
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No valid station data found.")
