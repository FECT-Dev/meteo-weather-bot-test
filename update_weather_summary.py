import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
from difflib import get_close_matches

# === SETTINGS ===
reports_folder = "reports"
summary_file = "weather_summary.csv"

known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunegala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# === OCR Image Cleaner ===
def clean_image(img):
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

line_pattern = re.compile(r"^(.+?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)$")

for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for file in os.listdir(folder_path):
        if not file.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, file)

        try:
            images = convert_from_path(pdf_path, dpi=300)
            text = "\n".join(
                pytesseract.image_to_string(clean_image(img), lang="eng", config="--psm 6")
                for img in images
            )

            # Save OCR
            with open(os.path.join(folder_path, "ocr_debug_output.txt"), "w") as f:
                f.write(text)

            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if m:
                        y, mth, d = map(int, m.groups())
                        actual_date = f"{y:04d}-{mth:02d}-{d:02d}"
                        break

            if not actual_date:
                print(f"⚠️ Skipping {file}: no date found")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists — skip")
                continue

            row_max, row_min, row_rain = {"Date": actual_date, "Type": "Max"}, {"Date": actual_date, "Type": "Min"}, {"Date": actual_date, "Type": "Rainfall"}
            found = False
            unmatched_debug = []

            for line in text.splitlines():
                m = line_pattern.match(line.strip())
                if not m:
                    continue

                station_raw = m.group(1).strip()
                max_val, min_val, rain_val = m.group(2), m.group(4), m.group(6)

                matches = get_close_matches(station_raw.title(), known_stations, n=1, cutoff=0.6)
                if not matches:
                    unmatched_debug.append(f"NO MATCH: {station_raw} => {max_val} {min_val} {rain_val}")
                    continue

                station = matches[0]
                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found = True

            if unmatched_debug:
                with open(os.path.join(folder_path, "ocr_unmatched_debug.txt"), "w") as f:
                    f.write("\n".join(unmatched_debug))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added: {actual_date} ({len(row_max)-2} stations)")
            else:
                print(f"⚠️ {file}: nothing matched")

        except Exception as e:
            print(f"❌ Error: {e}")

if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Final summary saved: {summary_file} ({len(summary_df)} rows)")
else:
    print("⚠️ Nothing new")
