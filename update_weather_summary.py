import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Station data pattern (station + max + min + rain)
line_pattern = re.compile(
    r"^([A-Za-z][A-Za-z \-]{2,})\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,2}(?:\.\d)?)\s+"
    r"(TR|\d{1,3}(?:\.\d)?)$"
)

# Primary date pattern (tolerates OCR errors)
date_pattern = re.compile(
    r"0830\s*SLTS?.*?(?:date)?\s*[:\-]?\s*([0-9]{4})[\.\-/ ]([0-9]{1,2})[\.\-/ ]([0-9]{1,2})",
    re.IGNORECASE
)

# Fallback date: just find the first YYYY.MM.DD or similar
fallback_date = re.compile(r"([0-9]{4})[.\-/ ]([0-9]{1,2})[.\-/ ]([0-9]{1,2})")

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
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join(pytesseract.image_to_string(img) for img in images)

            # Save OCR output for debugging
            with open("ocr_debug_output.txt", "w", encoding="utf-8") as f:
                f.write(text)

            print(f"\n📄 Preview from '{file}':\n" + "\n".join(text.splitlines()[:15]))

            # Extract date
            date_match = date_pattern.search(text)
            if not date_match:
                date_match = fallback_date.search(text)

            if not date_match:
                print(f"⚠️ Could not extract date from {file}")
                continue

            y, m, d = date_match.groups()
            actual_date = f"{int(y):04d}-{int(m):02d}-{int(d):02d}"

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
                    station, max_val, min_val, rain_val = match.groups()
                    station = station.strip().title()
                    row_max[station] = max_val.replace("TR", "0.0")
                    row_min[station] = min_val.replace("TR", "0.0")
                    row_rain[station] = rain_val.replace("TR", "0.0")
                    found_station = True

            if found_station:
                new_rows.extend([row_rain, row_max, row_min])
            else:
                print(f"❌ No station data found in {file}. Check OCR.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# Save CSV
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No valid station data found.")
