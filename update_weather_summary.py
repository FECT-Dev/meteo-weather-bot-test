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

# === Official stations list ===
known_stations = [
    "Anuradhapura","Badulla","Bandarawela","Batticaloa","Colombo","Galle",
    "Hambanthota","Jaffna","Monaragala","Katugasthota","Katunayake","Kurunegala",
    "Maha Illuppallama","Mannar","Polonnaruwa","Nuwara Eliya","Pothuvil",
    "Puttalam","Rathmalana","Rathnapura","Trincomalee","Vavuniya","Mattala",
    "Mullaitivu"
]

# === OCR helper ===
def clean_image(img: Image.Image) -> Image.Image:
    return img.convert("L").point(lambda x: 0 if x < 160 else 255, "1")

# === Clean numeric safely ===
def safe_number(v):
    v = v.upper().replace("O", "0").replace("|", "1").replace("I", "1").replace("l", "1")
    if v == "TR": return "0.0"
    try: return str(float(v))
    except: return "0.0"

# === Load ===
summary_df = pd.read_csv(summary_file) if os.path.exists(summary_file) else pd.DataFrame()
new_rows = []

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

            # Save OCR text for inspection
            with open(os.path.join(folder_path, "ocr_debug_output.txt"), "w") as f:
                f.write(text)

            # === Date only from trusted line ===
            actual_date = None
            for line in text.splitlines():
                if "0830" in line and "period" in line.lower():
                    m = re.search(r"(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})", line)
                    if m:
                        y, mth, d = map(int, m.groups())
                        actual_date = f"{y:04d}-{mth:02d}-{d:02d}"
                        break

            if not actual_date:
                print(f"⚠️ {file} — Skipped: date not found.")
                continue

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} — already exists.")
                continue

            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}
            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            found_station = False
            unmatched = []

            for line in text.splitlines():
                numbers = re.findall(r"(TR|\d{1,3}(?:\.\d+)?)", line)
                numbers = [n[0] if isinstance(n, tuple) else n for n in numbers]
                if len(numbers) != 3:
                    continue

                station_guess = line
                for num in numbers:
                    station_guess = station_guess.replace(num, "")
                station_guess = station_guess.strip()

                cutoff = 0.70 if len(station_guess) > 4 else 0.5
                match = get_close_matches(station_guess.title(), known_stations, n=1, cutoff=cutoff)
                if not match:
                    unmatched.append(f"{station_guess} | {numbers}")
                    continue

                station = match[0]
                row_max[station] = safe_number(numbers[0])
                row_min[station] = safe_number(numbers[1])
                row_rain[station] = safe_number(numbers[2])
                found_station = True

            if unmatched:
                with open(os.path.join(folder_path, "ocr_unmatched_debug.txt"), "w") as f:
                    f.write("\n".join(unmatched))

            if found_station:
                new_rows.extend([row_max, row_min, row_rain])
                print(f"✅ Added {actual_date} — {len(row_max)-2} stations")
            else:
                print(f"⚠️ {file} — No valid stations matched.")

        except Exception as e:
            print(f"❌ Error processing {file}: {e}")

# === Save result ===
if new_rows:
    df = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Saved summary: {summary_file} ({len(summary_df)} rows)")
else:
    print("⚠️ No new valid data found.")
