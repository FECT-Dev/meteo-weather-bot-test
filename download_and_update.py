import os
import sys
import time
from datetime import datetime, timedelta
import subprocess

RETRY_EVERY_MIN = int(os.getenv("RETRY_EVERY_MIN", "10"))
RETRY_WINDOW_MIN = int(os.getenv("RETRY_WINDOW_MIN", "45"))

def pdf_is_available() -> bool:
    """
    Checks if today's PDF is available on meteo.gov.lk.
    For simplicity, we'll try running the downloader in 'check only' mode
    or you can put Selenium HEAD logic here.
    """
    from download_weather_pdf import check_pdf_available
    return check_pdf_available()

def run_downloader_and_updater():
    """Run the existing download and summary update scripts."""
    subprocess.run([sys.executable, "download_weather_pdf.py"], check=True)
    subprocess.run([sys.executable, "update_weather_summary.py"], check=True)

if __name__ == "__main__":
    deadline = datetime.now() + timedelta(minutes=RETRY_WINDOW_MIN)
    while True:
        if pdf_is_available():
            run_downloader_and_updater()
            sys.exit(0)
        if datetime.now() >= deadline:
            print("PDF not available within retry window; exiting.")
            sys.exit(78)  # Or 0 if you don't want to fail the workflow
        print(f"PDF not yet available, retrying in {RETRY_EVERY_MIN} minutes...")
        time.sleep(RETRY_EVERY_MIN * 60)
