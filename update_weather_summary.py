import os
import re
import pandas as pd
from PyPDF2 import PdfReader

reports_folder = "reports"
summary_file = "weather_summary.csv"

# Regex patterns
line_pattern = re.compile(r"([A-Za-z \-]+)\s+(\d{1,2}\.\d|\bTR\b)\s+(\d{1,2}\.\d|\bTR\b)\s+(\d{1,3}\.\d|\bTR\b)")
date_pattern = re.compile(r"for the 24 hour period ending at 0830SLST on this date\s*([\d\.]+)")

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

    for filename in os.listdir(folder_path):
        if not filename.endswith(".pdf"):
            continue

        pdf_path = os.path.join(folder_path, filename)

        try:
            reader = PdfReader(pdf_path)
            text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())

            print(f"\n📄 Processing: {filename}")
            # Extract date inside PDF
            date_match = date_pattern.search(text)
            if not date_match:
                print(f"⚠️ Could not find date in {filename}")
                continue

            actual_date = date_match.group(1).replace(".", "-")
            if not actual_date:
                continue

            # Skip already processed date
            if not summary_df.empty and (summary_df["Date"] == actual_date).any():
                print(f"ℹ️ Skipping {actual_date} (already in summary)")
                continue

            stations_data = {"Date": actual_date, "Rainfall": {}, "Max": {}, "Min": {}}

            for line in text.splitlines():
                match = line_pattern.match(line.strip())
                if match:
                    station, max_temp, min_temp, rainfall = match.groups()
                    station = station.strip()

                    def parse(value):
                        return value if value == "TR" else float(value)

                    stations_data["Rainfall"][station] = parse(rainfall)
                    stations_data["Max"][station] = parse(max_temp)
                    stations_data["Min"][station] = parse(min_temp)

            # Add each type (Rainfall, Max, Min) as a row
            for measure in ["Rainfall", "Max", "Min"]:
                row = {"Date": actual_date, "Type": measure}
                row.update(stations_data[measure])
                new_rows.append(row)

        except Exception as e:
            print(f"❌ Failed to read {pdf_path}: {e}")

# Save new data
if new_rows:
    new_df = pd.DataFrame(new_rows)

    # Append to previous
    if not summary_df.empty:
        summary_df = pd.concat([summary_df, new_df], ignore_index=True)
    else:
        summary_df = new_df

    # Fill blanks
    summary_df = summary_df.fillna("...")

    summary_df.to_csv(summary_file, index=False)
    print("✅ Summary updated with", len(new_rows), "new rows.")
else:
    print("⚠️ No new records found. Nothing added.")
