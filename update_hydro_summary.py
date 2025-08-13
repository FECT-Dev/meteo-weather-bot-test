# update_hydro_summary.py  (FINAL)
import os, re, warnings
import pandas as pd
import pdfplumber, PyPDF2
from datetime import datetime, timedelta
from typing import Dict, List

# Prefer Camelot for table parsing; fallback to text
try:
    import camelot
    HAVE_CAMELOT = True
except Exception:
    HAVE_CAMELOT = False
    warnings.warn("camelot not available; using text fallback only")

REPORTS_DIR = "reports"
OUTPUT_CSV  = "hydrocatchment_summary.csv"

# === Fixed English station columns (order) ===
STATIONS = [
    "Castlereigh", "Norton", "Maussakele", "Canyon", "Lakshapana",
    "Upper Kotmale", "Kotmale", "Victoria", "Randenigala", "Rantambe",
    "Bowatenna", "Ukuwela", "Samanala Wewa", "Kukuleganga",
    "Maskeliya (DOM)", "Inginiyagala"
]

def _key(s: str) -> str:
    return re.sub(r"[^a-z]", "", (s or "").lower())

# Common variants -> canonical English
ALIASES = {
    "castlereigh": "Castlereigh", "castlereagh": "Castlereigh",
    "norton": "Norton",
    "maussakele": "Maussakele", "maussakelle": "Maussakele",
    "mausakelle": "Maussakele", "mausakele": "Maussakele",
    "canyon": "Canyon",
    "lakshapana": "Lakshapana", "laxapana": "Lakshapana",
    "upperkotmale": "Upper Kotmale", "upperkotmala": "Upper Kotmale",
    "kotmale": "Kotmale", "kotmala": "Kotmale",
    "victoria": "Victoria", "victoriya": "Victoria",
    "randenigala": "Randenigala",
    "rantambe": "Rantambe", "randambe": "Rantambe",
    "bowatenna": "Bowatenna", "bowatanna": "Bowatenna", "bowathenna": "Bowatenna",
    "ukuwela": "Ukuwela", "ukuwella": "Ukuwela",
    "samanalawewa": "Samanala Wewa", "samanalawawa": "Samanala Wewa", "samanalawewa": "Samanala Wewa",
    "kukuleganga": "Kukuleganga", "kukuleganaga": "Kukuleganga", "kukule ganga": "Kukuleganga",
    "maskeliyadom": "Maskeliya (DOM)", "maskeliya": "Maskeliya (DOM)", "maskeliya(dom)": "Maskeliya (DOM)", "maskeliyad o m": "Maskeliya (DOM)",
    "inginiyagala": "Inginiyagala",
}
ALLOWED = set(STATIONS)

TOKEN = r"(?:(?i:NA|TRACE|T\W*R)|\d+(?:\.\d+)?)"
TOKEN_RE = re.compile(TOKEN)

def english_only(s: str) -> str:
    parts = re.findall(r"[A-Za-z()\- ]+", s or "")
    return re.sub(r"\s+", " ", " ".join(parts)).strip()

def canon_station(name: str) -> str:
    k = _key(english_only(name))
    if k in ALIASES:
        c = ALIASES[k]
        return c if c in ALLOWED else ""
    for c in STATIONS:
        if _key(c) == k:
            return c
    return ""

def norm_val(v: str) -> str:
    raw = ("" if v is None else str(v)).strip()
    if not raw: return ""
    up = raw.upper()
    letters = re.sub(r"[^A-Z]", "", up)
    if letters == "NA": return "NA"
    if letters in ("TR","TRACE"): return "0.01"
    if up in ("-","--"): return ""
    cleaned = (raw.replace("O","0").replace("|","1").replace("I","1").replace("l","1"))
    cleaned = re.sub(r"[^\d.]", "", cleaned)
    if cleaned in ("","."): return ""
    try:
        return str(float(cleaned))
    except Exception:
        return ""

def actual_date_from_pdf(pdf_path: str, fallback: str) -> str:
    """Header date − 1 day; fallback to folder date (YYYY-MM-DD)."""
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

def pages_with_hydro(pdf_path: str) -> List[int]:
    pgs = []
    with pdfplumber.open(pdf_path) as pdf:
        for i, p in enumerate(pdf.pages, start=1):
            t = (p.extract_text() or "").lower()
            if "hydro catchment" in t:
                pgs.append(i)
    return pgs

def parse_hydro_with_camelot(pdf_path: str, pages: List[int]) -> Dict[str, str]:
    if not HAVE_CAMELOT or not pages:
        return {}
    acc: Dict[str, str] = {}
    page_spec = ",".join(map(str, pages))

    def read_tables(flavor: str):
        try:
            return camelot.read_pdf(pdf_path, pages=page_spec, flavor=flavor, strip_text=" \n")
        except Exception:
            return []
    tables = read_tables("lattice") or read_tables("stream")

    for tb in tables:
        df = tb.df.replace(r"\s+", " ", regex=True).fillna("")
        # drop header-ish rows
        hdr_mask = df.apply(lambda r: r.astype(str).str.lower().str.contains("hydro|rainfall stations|mm|area").any(), axis=1)
        df = df[~hdr_mask]
        for _, row in df.iterrows():
            cells = [c.strip() for c in row.tolist() if c.strip()]
            i = 0
            while i < len(cells):
                name_cell = cells[i]
                val_cell  = cells[i+1] if i+1 < len(cells) else ""
                if TOKEN_RE.fullmatch(val_cell or ""):
                    st = canon_station(name_cell)
                    if st:
                        v = norm_val(val_cell)
                        if v != "" and st not in acc:
                            acc[st] = v
                    i += 2
                elif TOKEN_RE.fullmatch(name_cell or "") and i+1 < len(cells):
                    st = canon_station(val_cell)
                    if st:
                        v = norm_val(name_cell)
                        if v != "" and st not in acc:
                            acc[st] = v
                    i += 2
                else:
                    i += 1
    return acc

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
            e = min([x for x in [
                low.find("meteorological stations", s+1),
                low.find("rainfall stations", s+1),
                low.find("other rainfall stations", s+1),
                low.find("appendix", s+1),
                low.find("reservoir", s+1),
            ] if x != -1] or [len(text)])
            block = text[s:e]

            pair = re.compile(rf"([A-Za-z()\- ]{{3,}}?)\s+({TOKEN})(?:\s*mm)?")
            for m in pair.finditer(block):
                name = english_only(m.group(1))
                st = canon_station(name)
                if not st:
                    continue
                v = norm_val(m.group(2))
                if v != "" and st not in acc:
                    acc[st] = v
    return acc

def compute_stats(row: dict) -> None:
    nums = []
    for s in STATIONS:
        v = row.get(s, "")
        if v in ("", "NA"):
            continue
        try:
            nums.append(float(v))
        except Exception:
            pass
    if nums:
        row["Total"]   = round(sum(nums), 1)
        row["Average"] = round(sum(nums)/len(nums), 1)
        row["Max"]     = round(max(nums), 1)
        row["Min"]     = round(min(nums), 1)
    else:
        row["Total"] = row["Average"] = row["Max"] = row["Min"] = ""

def main():
    rows: List[dict] = []

    for date_folder in sorted(os.listdir(REPORTS_DIR)):
        folder = os.path.join(REPORTS_DIR, date_folder)
        if not os.path.isdir(folder):
            continue
        pdf_path = os.path.join(folder, f"weather-{date_folder}.pdf")
        if not os.path.exists(pdf_path):
            continue

        pages = pages_with_hydro(pdf_path)
        if not pages:
            continue

        data: Dict[str, str] = {}
        d1 = parse_hydro_with_camelot(pdf_path, pages)
        data.update(d1)
        if not data:
            d2 = parse_hydro_with_text(pdf_path, pages)
            data.update(d2)

        act_date = actual_date_from_pdf(pdf_path, date_folder)

        row = {"Date": act_date}
        for st in STATIONS:
            row[st] = data.get(st, "")
        compute_stats(row)
        rows.append(row)

    if not rows:
        print("[hydro] No rows produced.")
        return

    df_new = pd.DataFrame(rows)
    df_new = df_new.reindex(columns=["Date"] + STATIONS + ["Total","Average","Max","Min"])

    if os.path.exists(OUTPUT_CSV):
        old = pd.read_csv(OUTPUT_CSV)
        df = pd.concat([old, df_new], ignore_index=True)
        df.drop_duplicates(subset=["Date"], keep="last", inplace=True)
    else:
        df = df_new

    df.sort_values(["Date"], inplace=True, ignore_index=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"[hydro] Saved {OUTPUT_CSV} — {len(df)} rows")

if __name__ == "__main__":
    print(f"[hydro] CWD={os.getcwd()} reports={os.path.abspath(REPORTS_DIR)}")
    main()
