import os
import time
import requests

# ---- Configurable settings ----
REGION_URL = "https://production-sfo.browserless.io"  # other regions: production-lon, production-ams
TARGET_URLS = ["https://example.com/", "https://meteo.gov.lk/"]  # control site first, then the real one
TIMEOUT = 60          # seconds per URL
EXTRA_PARAMS = ""     # e.g. "&proxy=residential&proxyCountry=lk" (check whether your plan allows it)
# -------------------------------

token = os.environ["BROWSERLESS_TOKEN"]

for url in TARGET_URLS:
    start = time.time()
    try:
        r = requests.post(
            f"{REGION_URL}/content?token={token}{EXTRA_PARAMS}",
            json={"url": url},
            timeout=TIMEOUT,
        )
        print(f"{url} -> HTTP {r.status_code} in {time.time() - start:.1f}s, {len(r.text)} characters")
        print(r.text[:200])
    except Exception as e:
        print(f"{url} -> FAILED after {time.time() - start:.1f}s: {e}")