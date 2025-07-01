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

# 🗂️ Common OCR station alias corrections
alias_map = {
    "Kurunagala": "Kurunegala",
    "Katu Gasthota": "Katugasthota",
    "Colombo.": "Colombo",
    "Nuwara Eli ya": "Nuwara Eliya",
    "Rathnapuura": "Rathnapura",
}

# === OCR CLEANER ===
def clean_image(img: Image.Image) -> Image.Image:
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

# === LOAD EXISTING ===
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

# === REGEX ===
line_pattern = re.compile(
    r"^(.+?)\s+"
    r"(TR|\d{1,3}(\.\d+)?)\s+"
    r"(TR|\d{1,3}(\.\d+)?)\s+"
    r"(TR|\d{1,3}(\.\d+)?)$"
)

# === PROCESS ===
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

            # Save debug OCR output
            debug_out = os.path.join(folder_path, "ocr_debug_output.txt")
            with open(debug_out, "w", encoding="utf-8") as f:
                f.write(text)

            # === DATE ===
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if m:
                        y, mth, d = map(int, m.groups())
                        actual_date = f"{y:04d}-{mth:02d}-{d:02d}"
                        break

            if not actual_date:
                fallback = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
                if fallback:
                    y, mth, d = map(int, fallback.groups())
                    actual_date = f"{y:04d}-{mth:02d}-{d:02d}"

            if not actual_date:
                print(f"⚠️ Skipping {file} — date not found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} — already exists.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            unmatched = []
            found = False

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if not match:
                    continue

                station_raw = match.group(1).strip()
                station_raw = alias_map.get(station_raw, station_raw)

                max_val = match.group(2)
                min_val = match.group(4)
                rain_val = match.group(6)

                matched = get_close_matches(station_raw.title(), known_stations, n=1, cutoff=0.7)
                if not matched:
                    unmatched.append(line)
                    continue

                station = matched[0]
                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found = True

            if unmatched:
                unmatched_file = os.path.join(folder_path, "ocr_unmatched_lines.txt")
                with open(unmatched_file, "w", encoding="utf-8") as f:
                    f.write("\n".join(unmatched))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added data for {actual_date}")
            else:
                print(f"⚠️ No valid stations in {file}")

        except Exception as e:
            print(f"❌ Error: {e}")

# === SAVE FINAL ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Final summary saved: {summary_file} ({len(summary_df)} rows)")
else:
    print("⚠️ No new valid data added.")
