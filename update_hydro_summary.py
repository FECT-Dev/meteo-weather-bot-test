import os
import re
import pdfplumber
import pandas as pd
import PyPDF2
from datetime import datetime, timedelta
from typing import Dict, List, Set

# === CONFIG ===
REPORTS_DIR = "reports"
OUTPUT_CSV = "hydrocatchment_summary.csv"
VARIABLE_NAME = "Rainfall"  # shown in the table's "Variable" column

# Recognize numbers, NA, TRACE/TR (with possible punctuation/spacing)
TOKEN = r"(?:(?i:NA|TRACE|T\W*R)|\d+(?:\.\d+)?)"
TOKEN_RE = re.compile(TOKEN)

print(f"[hydro] CWD={os.getcwd()}")
print(f"[hydro] Looking for reports in: {os.path.abspath(REPORTS_DIR)}")

def normalize_value(v: str) -> str:
    """
    Normalize a rainfall token to a CSV-friendly string.
    - 'NA' -> 'NA'
    - 'TR'/'TRACE' (any punctuation/spacing) -> '0.01'
    - '-'/'--' -> '' (missing)
    - numeric -> 'float' string
    """
    raw = str(v or "").strip()
    up = raw.upper()
    letters_only = re.sub(r"[^A-Z]", "", up)

    if letters_only == "NA":
        return "NA"
    if letters_only in ("TR", "TRACE"):
        return "0.01"
    if up in ("-", "--"):
        return ""

    # OCR cleanups
    cleaned = (raw
        .replace("O", "0")
        .replace("|", "1")
        .replace("I", "1")
        .replace("l", "1"))
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    if cleaned in ("", "."):
        return ""
    try:
        return str(float(cleaned))
    except Exception:
        return ""

def extract_pdf_date(pdf_path: str, fallback_date: str) -> str:
    """
    Read header date from PDF and convert to actual date = header - 1 day.
    If detection fails, fallback to folder date (already YYYY-MM-DD).
    Example: header '2025-08-10' -> save row as '2025-08-09'
    """
    actual_date = fallback_date
    try:
        with open(pdf_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            text = "".join(page.extract_text() or "" for page in reader.pages)
        m = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", text)
        if m:
            header = m.group(1).replace("/", "-").replace(".", "-")
            published = datetime.strptime(header, "%Y-%m-%d")
            actual_date = (published - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        pass
    return actual_date

def slice_hydro_block(txt: str) -> str:
    """Return only the 'Hydro Catchment Areas' section from a page's text."""
    low = txt.lower()
    start = low.find("hydro catchment")
    if start == -1:
        return ""
    # stop before the next major section
    stops = []
    for marker in (
        "rainfall stations",
        "other rainfall stations",
        "meteorological stations",
        "agro",
        "agro-meteorological",
        "appendix",
        "reservoir",
    ):
        p = low.find(marker, start + 1)
        if p != -1:
            stops.append(p)
    end = min(stops) if stops else len(txt)
    return txt[start:end]

def parse_hydro_block(block: str) -> Dict[str, str]:
    """
    Parse a Hydro Catchment section and return {station: value}.
    Works whether station/value share a line, wrap to the next, or columns are squashed.
    """
    results: Dict[str, str] = {}

    # Clean headers/footers
    lines = []
    for ln in (block or "").splitlines():
        s = ln.strip()
        if not s:
            continue
        if any(k in s.lower() for k in ["hydro catchment", "station", "area", "mm", "rainfall during"]):
            # header-ish line; skip
            continue
        lines.append(s)

    # Strategy 1: strict trailing token pattern per line
    line_pat = re.compile(rf"^([A-Za-z][A-Za-z .\-()'/]*)\s+({TOKEN})\s*(?:mm)?$")
    for ln in lines:
        m = line_pat.match(ln)
        if m:
            name_raw, val_raw = m.group(1).strip(), m.group(2)
            name = re.sub(r"\s+", " ", name_raw).title()
            if name:
                results.setdefault(name, normalize_value(val_raw))

    # Strategy 2: wrapped lines (name on one line; value-only next)
    for i, ln in enumerate(lines):
        # skip if already captured
        if any(ln.startswith(k) for k in results.keys()):
            continue
        if re.match(r"^[A-Za-z].{1,80}$", ln) and i + 1 < len(lines):
            nxt = lines[i + 1]
            tok = TOKEN_RE.fullmatch(nxt.strip())
            if tok:
                name = re.sub(r"\s+", " ", ln.strip()).title()
                results.setdefault(name, normalize_value(tok.group(0)))

    # Strategy 3: inline pairs in wide/squashed lines
    pair_pat = re.compile(rf"([A-Za-z][A-Za-z .\-()'/]{{2,}}?)\s+({TOKEN})(?:\s*mm)?")
    for ln in lines:
        for m in pair_pat.finditer(ln):
            name = re.sub(r"\s+", " ", m.group(1).strip()).title()
            val = normalize_value(m.group(2))
            if name:
                results.setdefault(name, val)

    return results

def collect_from_pdf(pdf_path: str) -> Dict[str, str]:
    """Open a PDF and merge Hydro Catchment pairs across all pages."""
    acc: Dict[str, str] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text() or ""
            blk = slice_hydro_block(t)
            if not blk:
                continue
            got = parse_hydro_block(blk)
            for k, v in got.items():
                if k not in acc and v != "":
                    acc[k] = v
    return acc

def ensure_all_dates(rows: List[dict]) -> List[dict]:
    """Fill gaps from earliest seen date to yesterday with NA rows."""
    if not rows:
        return rows
    dates = {r["Date"] for r in rows}
    start = min(datetime.strptime(d, "%Y-%m-%d") for d in dates)
    end = (datetime.now() - timedelta(days=1))
    expected = {(start + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end - start).days + 1)}
    missing = sorted(expected - dates)

    # Build union of all seen station names to construct NA rows
    stations: Set[str] = set()
    for r in rows:
        stations.update(k for k in r.keys() if k not in ("Date", "Variable", "Total", "Average", "Max", "Min"))

    for d in missing:
        row = {"Date": d, "Variable": VARIABLE_NAME}
        for s in stations:
            row[s] = "NA"
        rows.append(row)
    return rows

def compute_stats(row: dict, station_cols: List[str]) -> None:
    nums = []
    for s in station_cols:
        v = row.get(s, "")
        if v in ("", "NA"):
            continue
        try:
            nums.append(float(v))
        except Exception:
            pass
    if nums:
        row["Total"] = round(sum(nums), 1)
        row["Average"] = round(sum(nums) / len(nums), 1)
        row["Max"] = round(max(nums), 1)
        row["Min"] = round(min(nums), 1)
    else:
        row["Total"] = ""
        row["Average"] = ""
        row["Max"] = ""
        row["Min"] = ""

def main():
    new_rows: List[dict] = []
    station_union: Set[str] = set()

    date_folders = [d for d in sorted(os.listdir(REPORTS_DIR)) if os.path.isdir(os.path.join(REPORTS_DIR, d))]
    print(f"[hydro] Found {len(date_folders)} date folders under reports/")
    for date_folder in date_folders:
        folder = os.path.join(REPORTS_DIR, date_folder)
        pdf_path = os.path.join(folder, f"weather-{date_folder}.pdf")
        if not os.path.exists(pdf_path):
            print(f"[hydro] Skipping {date_folder}: missing {pdf_path}")
            continue
        else:
            print(f"[hydro] Using PDF: {pdf_path}")

        actual_date = extract_pdf_date(pdf_path, date_folder)
        data = collect_from_pdf(pdf_path)
        print(f"[hydro] Parsed {len(data)} hydro stations for {actual_date}")

        station_union.update(data.keys())
        row = {"Date": actual_date, "Variable": VARIABLE_NAME}
        row.update(data)
        new_rows.append(row)

    # Merge with previous CSV to remember historical stations
    prior_df = None
    prior_cols: List[str] = []
    if os.path.exists(OUTPUT_CSV):
        try:
            prior_df = pd.read_csv(OUTPUT_CSV)
            prior_cols = [c for c in prior_df.columns if c not in ("Date", "Variable", "Total", "Average", "Max", "Min")]
        except Exception:
            prior_df = None
    station_union.update(prior_cols)

    # Fill missing dates (to yesterday)
    new_rows = ensure_all_dates(new_rows)

    # Build DataFrame with stable column order
    station_cols = sorted(station_union)
    ordered_cols = ["Date", "Variable"] + station_cols + ["Total", "Average", "Max", "Min"]

    df_new = pd.DataFrame(new_rows)
    # compute stats per row
    for i in range(len(df_new)):
        row = df_new.iloc[i].to_dict()
        compute_stats(row, station_cols)
        for k in ("Total", "Average", "Max", "Min"):
            df_new.at[i, k] = row.get(k, "")

    df_new = df_new.reindex(columns=ordered_cols)

    # Merge with old CSV; keep latest per Date
    if prior_df is not None:
        df = pd.concat([prior_df, df_new], ignore_index=True)
        df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    else:
        df = df_new

    df.sort_values(["Date"], inplace=True, ignore_index=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[hydro] Saved: {OUTPUT_CSV} — {len(df)} rows, {len(station_cols)} stations")

if __name__ == "__main__":
    main()
