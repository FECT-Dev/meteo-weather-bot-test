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

# === OCR Image Cleaner ===
def clean_image(img):
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

# === Load Existing Summary ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

# === Regex Pattern ===
line_pattern = re.compile(r"^(.+?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)$")

# === Process PDFs ===
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

            # 💾 Save OCR debug output
            debug_out = os.path.join(folder_path, "ocr_debug_output.txt")
            with open(debug_out, "w", encoding="utf-8") as f:
                f.write(text)

            # === Extract date: trusted line first ===
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    match = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if match:
                        y, m, d = map(int, match.groups())
                        actual_date = f"{y:04d}-{m:02d}-{d:02d}"
                        break

            if not actual_date:
                # Fallback: first valid date anywhere
                fallback = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", text)
                if fallback:
                    y, m, d = map(int, fallback.groups())
                    actual_date = f"{y:04d}-{m:02d}-{d:02d}"

            if not actual_date:
                print(f"⚠️ Skipping {file} — no date found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists — skipping.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            unmatched_lines = []
            found = False

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if not match:
                    continue

                station_raw = match.group(1).strip()
                max_val = match.group(2)
                min_val = match.group(4)
                rain_val = match.group(6)

                # 🔑 Fuzzy match with stricter cutoff to avoid junk
                matches = get_close_matches(
                    station_raw.title(), known_stations, n=1, cutoff=0.80
                )
                if not matches or len(station_raw) < 4:
                    unmatched_lines.append(line)
                    continue

                station = matches[0]

                # ✅ Validate numbers again
                if not all(re.fullmatch(r"TR|\d{1,3}(\.\d+)?", v) for v in [max_val, min_val, rain_val]):
                    unmatched_lines.append(line)
                    continue

                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found = True

            if unmatched_lines:
                with open(os.path.join(folder_path, "ocr_unmatched_lines.txt"), "w", encoding="utf-8") as f:
                    f.write("\n".join(unmatched_lines))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added {len(row_max)-2} stations for {actual_date}")
            else:
                print(f"⚠️ No valid station rows in {file}")

        except Exception as e:
            print(f"❌ Error: {e}")

# === Save Final CSV ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Final saved: {summary_file} ({len(summary_df)} rows)")
else:
    print("⚠️ No new valid data found.")
