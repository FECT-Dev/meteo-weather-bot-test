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
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunagala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

# === CLEAN OCR ===
def clean_image(img):
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

# === LOAD EXISTING ===
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# === PROCESS PDFS ===
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

            # === Save OCR for troubleshooting ===
            debug_out = os.path.join(folder_path, "ocr_debug_output.txt")
            with open(debug_out, "w", encoding="utf-8") as f:
                f.write(text)

            # === Find DATE ONLY from trusted line ===
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    match = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if match:
                        y, m, d = map(int, match.groups())
                        actual_date = f"{y:04d}-{m:02d}-{d:02d}"
                        break

            if not actual_date:
                print(f"⚠️ Skipping {file} — date not found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} — already exists.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            found = False

            for line in text.splitlines():
                parts = line.strip().split()
                if len(parts) < 4:
                    continue

                station_raw = " ".join(parts[:-3])
                max_val, min_val, rain_val = parts[-3:]

                # === Check values: must be TR or decimal ===
                if not all(re.fullmatch(r"TR|\d{1,3}(\.\d+)?", v) for v in [max_val, min_val, rain_val]):
                    continue

                # === Fuzzy match station ===
                matches = get_close_matches(station_raw.strip().title(), known_stations, n=1, cutoff=0.8)
                if not matches:
                    print(f"❌ Unmatched station: {station_raw}")
                    continue

                station = matches[0]
                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found = True

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added rows for {actual_date}")
            else:
                print(f"⚠️ No valid stations in {file}")

        except Exception as e:
            print(f"❌ Error: {e}")

# === SAVE SUMMARY ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Saved: {summary_file} (rows: {len(summary_df)})")
else:
    print("⚠️ No new data added.")
