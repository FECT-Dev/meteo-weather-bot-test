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

# Recognize values like numbers, NA, TRACE, TR (with possible punctuation/spacing)
TOKEN = r"(?:(?i:NA|TRACE|T\W*R)|\d+(?:\.\d+)?)"
TOKEN_RE = re.compile(TOKEN)


def normalize_value(v: str) -> str:
    """Normalize a rainfall token to a CSV-friendly string.
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
        .r
