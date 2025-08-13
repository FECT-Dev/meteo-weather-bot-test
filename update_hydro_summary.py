import os, re, warnings
import pandas as pd
import pdfplumber, PyPDF2
from datetime import datetime, timedelta
from typing import Dict, List, Set

# Camelot is optional but preferred for this table
try:
    import camelot
    HAVE_CAMELOT = True
except Exception:
    HAVE_CAMELOT = False
    warnings.warn("camelot not available; will use text fallback")

REPORTS_DIR = "reports"
OUTPUT_CSV  = "hydrocatchment_summary.csv"
VARIABLE    = "Rainfall"

TOKEN = r"(?:(?i:NA|TRACE|T\W*R)|\d+(?:\.\d+)?)"
TOKEN_RE = re.compile(TOKEN)

# --- value normalizer --------------------------------------------------------
def norm_val(v: str) -> str:
    raw = ("" if v is None else str(v)).strip()
    if not raw:
        return ""
    up = raw.upper()
    letters = re.sub(r"[^A-Z]", "", up)
    if letters == "NA":
        return "NA"
    if letters in ("TR", "TRACE"):
        return "0.01"
    if up in ("-", "--"):
        return ""

    # OCR tidy
    cleaned = (raw.replace("O", "0")
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

# --- date from header (header − 1 day) --------------------------------------
def actual_date_from_pdf(pdf_path: str, fallback: str) -> str:
    out = fallback
    try:
        with open(pdf_path, "rb") as f:
            r = PyPDF2.PdfReader(f)
            txt = "".join(p.extract_text() or "" for p in r.pages)
        m = re.search(r"(\d{4}[./-]\d{2}[./-]\d{2})", txt)
        if m:
            hdr = m.group(1).replace("/", "-").replace(".", "-")
            pub = datetime.strptime(hdr, "%Y-%m-%d")
            out = (pub - timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception:
        pass
    return out

# --- find pages containing the hydro table ----------------------------------
def pages_with_hydro(pdf_path: str) -> List[int]:
    pages = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, p in enumerate(pdf.pages, start=1):
            t = (p.extract_text() or "").lower()
            if "hydro catchment" in t:
                pages.append(i)
    return pages

# --- parse using Camelot (preferred) ----------------------------------------
def parse_hydro_with_camelot(pdf_path: str, pages: List[int]) -> Dict[str, str]:
    if not HAVE_CAMELOT or not pages:
        return {}
    acc: Dict[str, str] = {}

    # Try lattice then stream
    page_spec = ",".join(str(p) for p in pages)
    tables = []
    try:
        tables = camelot.read_pdf(pdf_path, pages=page_spec, flavor="lattice", strip_text=" \n")
    except Exception:
        tables = []
    if not tables:
        try:
            tables = camelot.read_pdf(pdf_path, pages=page_spec, flavor="stream", strip_text=" \n")
        except Exception:
            tables = []

    for tb in tables:
        df = tb.df.replace(r"\s+", " ", regex=True).fillna("")
        # Heuristic: drop header rows that contain "Rainfall Stations" or "mm"
        df = df[~df.apply(lambda r: r.astype(str).str.lower().str.contains("hydro|rainfall stations|mm").any(), axis=1)]

        for _, row in df.iterrows():
            # scan cells like [Castlereigh, 1.0, Randenigala, 0.0, ...]
            cells = [c.strip() for c in row.tolist() if c.strip()]
            i = 0
            while i < len(cells):
                name = cells[i]
                val  = cells[i + 1] if i + 1 < len(cells) else ""
                # allow either order (rare)
                if TOKEN_RE.fullmatch(val or ""):
                    station = re.sub(r"\s+", " ", name).strip().title()
                    v = norm_val(val)
                    if station and v != "" and not station.lower().startswith("page "):
                        acc.setdefault(station, v)
                    i += 2
                elif TOKEN_RE.fullmatch(name or "") and i + 1 < len(cells):
                    station = re.sub(r"\s+", " ", val).strip().title()
                    v = norm_val(name)
                    if station and v != "" and not station.lower().startswith("page "):
                        acc.setdefault(station, v)
                    i += 2
                else:
                    i += 1
    return acc

# --- parse using text fallback ----------------------------------------------
def parse_hydro_with_text(pdf_path: str, pages: List[int]) -> Dict[str, str]:
    acc: Dict[str, str] = {}
    with pdfplumber.open(pdf_path) as pdf:
        for pno in pages:
            page = pdf.pages[pno - 1]
            text = page.extract_text() or ""
            low = text.lower()
            s = low.find("hydro catchment")
            if s == -1:
                continue
            # end before next block headline if present
            e = min([x for x in [
                low.find("meteorological stations", s + 1),
                low.find("rainfall stations", s + 1),
                low.find("other rainfall stations", s + 1)
            ] if x != -1] or [len(text)])
            block = text[s:e]

            # brute-force: look for (...letters...) (value) pairs anywhere
            # names can include spaces, hyphens, parentheses, slashes, '
            pair = re.compile(rf"([A-Za-z][A-Za-z .\-()'/]{{2,}}?)\s+({TOKEN})(?:\s*mm)?")
            for m in pair.finditer(block):
                name = re.sub(r"\s+", " ", m.group(1).strip()).title()
                val  = norm_val(m.group(2))
                if name and val != "":
                    acc.setdefault(name, val)
    return acc

# --- compute row stats -------------------------------------------------------
def add_stats(row: dict, station_cols: List[str]) -> None:
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
        row["Total"]   = round(sum(nums), 1)
        row["Average"] = round(sum(nums) / len(nums), 1)
        row["Max"]     = round(max(nums), 1)
        row["Min"]     = round(min(nums), 1)
    else:
        row["Total"] = row["Average"] = row["Max"] = row["Min"] = ""

# --- main --------------------------------------------------------------------
def main():
    all_rows: List[dict] = []
    station_union: Set[str] = set()

    for date_folder in sorted(os.listdir(REPORTS_DIR)):
        folder = os.path.join(REPORTS_DIR, date_folder)
        if not os.path.isdir(folder):
            continue
        pdf_path = os.path.join(folder, f"weather-{date_folder}.pdf")
        if not os.path.exists(pdf_path):
            continue

        pages = pages_with_hydro(pdf_path)
        if not pages:
            print(f"[hydro] No 'Hydro Catchment' pages found in {pdf_path}")
            continue

        data: Dict[str, str] = {}
        # 1) Camelot
        data.update(parse_hydro_with_camelot(pdf_path, pages))
        # 2) Fallback to text if Camelot missed something
        if not data:
            data.update(parse_hydro_with_text(pdf_path, pages))

        # Still nothing? log and keep a blank row for the date
        act_date = actual_date_from_pdf(pdf_path, date_folder)
        print(f"[hydro] {act_date}: parsed {len(data)} stations from {os.path.basename(pdf_path)}")

        row = {"Date": act_date, "Variable": VARIABLE}
        row.update(data)
        all_rows.append(row)
        station_union.update(data.keys())

    # Include any previous columns so headers remain stable
    prior_df = None
    if os.path.exists(OUTPUT_CSV):
        try:
            prior_df = pd.read_csv(OUTPUT_CSV)
            for c in prior_df.columns:
                if c not in ("Date", "Variable", "Total", "Average", "Max", "Min"):
                    station_union.add(c)
        except Exception:
            prior_df = None

    # Build DataFrame
    station_cols = sorted(station_union)
    ordered = ["Date", "Variable"] + station_cols + ["Total", "Average", "Max", "Min"]
    df_new = pd.DataFrame(all_rows).reindex(columns=ordered)

    # Stats per row
    for i in range(len(df_new)):
        add_stats(df_new.loc[i], station_cols)

    # Merge with old CSV, keep last per Date
    if prior_df is not None:
        df = pd.concat([prior_df, df_new], ignore_index=True)
        df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    else:
        df = df_new

    df.sort_values(["Date"], inplace=True, ignore_index=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[hydro] Saved {OUTPUT_CSV}: {len(df)} rows, {len(station_cols)} stations")

if __name__ == "__main__":
    print(f"[hydro] CWD={os.getcwd()}  reports={os.path.abspath(REPORTS_DIR)}")
    main()
