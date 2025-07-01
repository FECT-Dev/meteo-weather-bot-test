import os
import re
import pandas as pd
from pdf2image import convert_from_path
import pytesseract
from PIL import Image
from difflib import get_close_matches

reports_folder = "reports"
summary_file = "weather_summary.csv"

known_stations = [
    "Anuradhapura", "Badulla", "Bandarawela", "Batticaloa", "Colombo", "Galle",
    "Hambanthota", "Jaffna", "Monaragala", "Katugasthota", "Katunayake", "Kurunegala",
    "Maha Illuppallama", "Mannar", "Polonnaruwa", "Nuwara Eliya", "Pothuvil",
    "Puttalam", "Rathmalana", "Rathnapura", "Trincomalee", "Vavuniya", "Mattala",
    "Mullaitivu"
]

def clean_image(img):
    img = img.convert("L")
    return img.point(lambda x: 0 if x < 160 else 255, "1")

summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

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

            # Save raw OCR for troubleshooting
            with open(os.path.join(folder_path, "ocr_debug_output.txt"), "w") as f:
                f.write(text)

            # Extract date only from trusted line
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if m:
                        y, mth, d = map(int, m.groups())
                        actual_date = f"{y:04d}-{mth:02d}-{d:02d}"
                        break

            if not actual_date:
                print(f"⚠️ Skipping {file}: date not found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists — skip.")
                continue

            row_max, row_min, row_rain = {"Date": actual_date, "Type": "Max"}, {"Date": actual_date, "Type": "Min"}, {"Date": actual_date, "Type": "Rainfall"}
            found = False
            unmatched_debug = []

            for line in text.splitlines():
                parts = line.strip().split()
                numbers = [p for p in parts if re.fullmatch(r"(TR|\d{1,3}(\.\d+)?)", p)]
                if len(numbers) != 3:
                    continue

                station_candidate = line
                for num in numbers:
                    station_candidate = station_candidate.replace(num, "").strip()

                match = get_close_matches(station_candidate.title(), known_stations, n=1, cutoff=0.7)
                if not match:
                    unmatched_debug.append(f"Unmatched: {station_candidate} -> {numbers} -> line: {line}")
                    continue

                station = match[0]
                row_max[station] = numbers[0].replace("TR", "0.0")
                row_min[station] = numbers[1].replace("TR", "0.0")
                row_rain[station] = numbers[2].replace("TR", "0.0")
                found = True

            if unmatched_debug:
                with open(os.path.join(folder_path, "ocr_unmatched_debug.txt"), "w") as f:
                    f.write("\n".join(unmatched_debug))

            if found:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added: {actual_date}")
            else:
                print(f"⚠️ {file}: no valid stations matched.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Final summary saved: {summary_file} ({len(summary_df)} rows)")
else:
    print("⚠️ No new valid data found.")
