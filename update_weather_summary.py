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

alias_map = {
    "Kurunagala": "Kurunegala",
    "Nuwara Eli ya": "Nuwara Eliya",
    "Katu Gasthota": "Katugasthota"
}

def clean_image(img):
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

# === LOAD ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

line_pattern = re.compile(r"^(.+?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)\s+(TR|\d{1,3}(\.\d+)?)$")

# === LOOP ===
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

            # Save OCR output
            with open(os.path.join(folder_path, "ocr_debug_output.txt"), "w") as f:
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
                print(f"⚠️ {file}: date not found")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already done")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}

            found = False
            unmatched = []

            for line in text.splitlines():
                m = line_pattern.match(line.strip())
                if not m:
                    continue

                station_raw = m.group(1).strip()
                station_raw = alias_map.get(station_raw, station_raw)

                max_val = m.group(2)
                min_val = m.group(4)
                rain_val = m.group(6)

                station_guess = get_close_matches(station_raw.title(), known_stations, n=1, cutoff=0.7)
                if not station_guess:
                    unmatched.append(line)
                    continue

                station = station_guess[0]
                row_max[station] = max_val.replace("TR", "0.0")
                row_min[station] = min_val.replace("TR", "0.0")
                row_rain[station] = rain_val.replace("TR", "0.0")
                found = True

            if unmatched:
                with open(os.path.join(folder_path, "ocr_unmatched_lines.txt"), "w") as f:
                    f.write("\n".join(unmatched))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added {actual_date} ({len(row_max)-2} stations)")
            else:
                print(f"⚠️ {file}: no stations matched")

        except Exception as e:
            print(f"❌ Error: {e}")

# === SAVE ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Done: {summary_file}")
else:
    print("⚠️ Nothing new")
