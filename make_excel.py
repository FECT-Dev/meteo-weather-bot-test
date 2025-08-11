import pandas as pd

INPUT = "weather_summary.csv"
OUTPUT = "weather_summary.xlsx"

df = pd.read_csv(INPUT)

# Optional: ensure Date sorted
if "Date" in df.columns:
    df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    df.sort_values(["Date", "Type"], inplace=True, ignore_index=True)
    df["Date"] = df["Date"].dt.strftime("%Y-%m-%d")

# Write Excel (needs openpyxl)
df.to_excel(OUTPUT, index=False)
print(f"Wrote: {OUTPUT} ({len(df)} rows)")
