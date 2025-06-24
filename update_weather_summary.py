import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Extract English lines only: e.g. "Anuradhapura 32.7 24.8 0.0"
line_pattern = re.compile(r"^([A-Za-z \-]+?)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,2}\.\d)\s+(TR|\d{1,3}\.\d)$")
date_pattern = re.compile(r"ending at 0830SLTS? on this date\s*(\d{4}\.\d{2}\.\d{2})")

# Load summary
if os.path.exists(summary_file):
    summary_df = pd.read_csv(summary_file)
else:
    summary_df = pd.DataFrame()

new_rows = []

for date_folder in sorted(os.listdir(reports_folder)):
    folder_path = os.path.join(reports_folder, date_folder)
    if not os.path.isdir(folder_path):
        continue

    for filename in os.listdir(folder_path):
        if not filename.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)

        try:
            reader = PdfReader(pdf_path)
            text = "\n".join(
                page.extract_text() for page in reader.pages if page.extract_text()
            )

            # Extract English date
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Date not found in {filename}. Skipping.")
                continue

            actual_date = date_match.group(1).replace(".", "-")

            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ {actual_date} already exists. Skipping.")
                continue

            row_rain = {"Date": actual_date, "Type": "Rainfall"}
            row_max = {"Date": actual_date, "Type": "Max"}
            row_min = {"Date": actual_date, "Type": "Min"}

            for line in text.splitlines():
                line = line.strip()
                match = line_pattern.match(line)
                if match:
                    station, max_val, min_val, rain_val = match.groups()
                    row_rain[station] = rain_val
                    row_max[station] = max_val
                    row_min[station] = min_val

            new_rows.extend([row_rain, row_max, row_min])

        except Exception as e:
            print(f"❌ Failed to read {pdf_path}: {e}")

# Save final summary
if new_rows:
    df_new = pd.DataFrame(new_rows)
    summary_df = pd.concat([summary_df, df_new], ignore_index=True)
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary updated: {summary_file}")
else:
    print("⚠️ No new data added.")
