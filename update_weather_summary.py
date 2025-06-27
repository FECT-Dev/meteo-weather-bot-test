import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
from difflib import get_close_matches

reports_folder = "reports"
summary_file = "weather_summary.csv"

# ✅ Known station names
known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunagala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# 🧼 Preprocess image for better OCR
def clean_image(img: Image.Image) -> Image.Image:
    img = img.convert("L")  # grayscale
    return img.point(lambda x: 0 if x < 160 else 255, "1")  # binarize

# 📄 Load existing summary
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

# 🔁 Loop through folders
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
                pytesseract.image_to_string(clean_image(img), lang="eng", config="--psm 6")
                for img in images
            )

            # 💾 Debug OCR output
            with open(os.path.join(folder_path, "ocr_debug_output.txt"), "w", encoding="utf-8") as f:
                f.write(text)

            # 📅 Extract correct date from line with 0830 and period
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    match = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if match:
                        y, m, d = map(int, match.groups())
                        actual_date = f"{y:04d}-{m:02d}-{d:02d}"
                        break

            if not actual_date:
                print(f"⚠️ Date not found in {file}")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            found_station = False

            for line in text.splitlines():
                parts = line.strip().split()
                if len(parts) < 4:
                    continue

                station_raw = " ".join(parts[:-3])
                max_val, min_val, rain_val = parts[-3:]

                # ⚠️ Check numeric values
                if not all(re.fullmatch(r"(TR|\d{1,2}(\.\d)?)", v) for v in [max_val, min_val, rain_val]):
                    continue

                # 🎯 Try fuzzy match to station list
                match = get_close_matches(station_raw.strip().title(), known_stations, n=1, cutoff=0.75)
                if not match:
                    print(f"❌ Unmatched station: {station_raw}")
                    continue

                station = match[0]
                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found_station = True

            if found_station:
                new_rows.extend([row_max, row_min, row_rain])
            else:
                print(f"⚠️ No valid station data found in {file}")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# 💾 Save summary
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No new valid station data found.")
