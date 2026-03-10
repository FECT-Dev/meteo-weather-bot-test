import os
import time
import tempfile
import shutil
import glob
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# === Auto-install matching ChromeDriver ===
chromedriver_autoinstaller.install()

# === CONFIG ===
today = datetime.now().strftime('%Y-%m-%d')
download_path = os.path.join(os.getcwd(), "downloads", today)
os.makedirs(download_path, exist_ok=True)

# Folder to move final PDF
today_folder = os.path.join("reports", today)
os.makedirs(today_folder, exist_ok=True)

# === Chrome Options ===
chrome_options = Options()
chrome_options.add_argument("--headless=new")  # Headless mode
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "plugins.always_open_pdf_externally": True  # Avoid PDF preview
})

# Temporary user data folder
temp_user_data_dir = tempfile.mkdtemp()
chrome_options.add_argument(f"--user-data-dir={temp_user_data_dir}")

# === Initialize Driver ===
driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(300)  # Increase timeout to 5 minutes

def wait_for_download(folder, timeout=120):
    """
    Wait for a PDF to appear in the folder, up to timeout seconds.
    """
    end_time = time.time() + timeout
    while time.time() < end_time:
        pdf_files = glob.glob(os.path.join(folder, "*.pdf"))
        if pdf_files:
            return pdf_files
        time.sleep(1)
    return []

try:
    # === Open Website with Retry ===
    max_retries = 3
    for attempt in range(max_retries):
        try:
            driver.get("https://meteo.gov.lk/")
            break
        except Exception as e:
            print(f"Attempt {attempt+1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(5)

    wait = WebDriverWait(driver, 20)

    # === Switch to English if button exists ===
    try:
        english_link = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "English"))
        )
        english_link.click()
        time.sleep(2)
        print("Switched to English.")
    except Exception:
        print("English button not found, continuing...")

    # === Navigate to Agromet / Weather Data ===
    agromet_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Agromet')]"))
    )
    agromet_button.click()
    print("Clicked Agromet / Weather Data.")

    # === Click Other Weather Data ===
    other_weather_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Other Weather Data')]"))
    )
    other_weather_button.click()
    print("Clicked Other Weather Data.")

    # === Click 24 Hour Weather Report PDF ===
    pdf_link = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'24 Hour Weather Report')]"))
    )
    pdf_link.click()
    print("Clicked 24 Hour Weather Report link.")

    # === Wait for download to finish ===
    print("Waiting for PDF download to complete...")
    pdf_files = wait_for_download(download_path, timeout=120)  # Wait up to 2 minutes

    if not pdf_files:
        print("Download failed or timed out.")
    else:
        for file in pdf_files:
            dst = os.path.join(today_folder, f"weather-{today}.pdf")
            shutil.move(file, dst)
        print(f"PDF successfully downloaded and moved to: {dst}")

finally:
    driver.quit()
    print("Chrome closed.")
