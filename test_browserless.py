import os
import time
import requests

# ---- Configurable settings ----
REGION_URL = "https://production-sfo.browserless.io"  # other regions: production-lon, production-ams
TARGET_URL = "https://meteo.gov.lk/"
TIMEOUT = 90          # seconds
EXTRA_PARAMS = ""     # e.g. "&proxy=residential&proxyCountry=lk" (check whether your plan allows it)
# -------------------------------

token = os.environ["BROWSERLESS_TOKEN"]
start = time.time()
try:
    r = requests.post(
        f"{REGION_URL}/content?token={token}{EXTRA_PARAMS}",
        json={"url": TARGET_URL},
        timeout=TIMEOUT,
    )
    print(f"Browserless: HTTP {r.status_code} in {time.time() - start:.1f}s")
    print(f"Page length: {len(r.text)} characters")
    print(r.text[:300])
except Exception as e:
    print(f"Browserless test FAILED after {time.time() - start:.1f}s: {e}")