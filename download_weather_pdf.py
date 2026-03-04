import os
import time
import tempfile
import shutil
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import chromedriver_autoinstaller

# Auto-install matching ChromeDriver version!
chromedriver_autoinstaller.install()

# === CONFIG ===
today = datetime.now().strftime('%Y-%m-%d')
download_path = os.path.join(os.getcwd(), "downloads", today)
os.makedirs(download_path, exist_ok=True)

# === Chrome options ===
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "plugins.always_open_pdf_externally": True
})

temp_user_data_dir = tempfile.mkdtemp()
chrome_options.add_argument(f"--user-data-dir={temp_user_data_dir}")

driver = webdriver.Chrome(options=chrome_options)

try:
    driver.get("https://meteo.gov.lk/")
    wait = WebDriverWait(driver, 20)

    # Switch language if needed
    try:
        english_link = wait.until(
            EC.element_to_be_clickable((By.LINK_TEXT, "English"))
        )
        english_link.click()
        time.sleep(2)
        print("🌐 Switched to English.")
    except Exception as e:
        print("English button not found, continuing...")

    # Click Agromet / Weather Data
    agromet_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Agromet')]"))
    )
    agromet_button.click()
    print("Clicked Agromet / Weather Data.")

    # Click Other Weather Data
    other_weather_button = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Other Weather Data')]"))
    )
    other_weather_button.click()
    print("Clicked Other Weather Data.")

    # Click 24 Hour Weather Report link
    pdf_link = wait.until(
        EC.element_to_be_clickable((By.XPATH, "//a[contains(text(),'24 Hour Weather Report')]"))
    )
    pdf_link.click()
    print("Clicked 24 Hour Weather Report.")

    # Wait for download to finish
    print("Waiting for download...")
    time.sleep(15)

finally:
    driver.quit()
    print("Chrome closed.")

# === Check and move PDF ===
print(f"Files in download folder: {os.listdir(download_path)}")

today_folder = os.path.join("reports", today)
os.makedirs(today_folder, exist_ok=True)

pdf_files = [f for f in os.listdir(download_path) if f.lower().endswith(".pdf")]

if pdf_files:
    for file in pdf_files:
        src = os.path.join(download_path, file)
        dst = os.path.join(today_folder, f"weather-{today}.pdf")
        shutil.move(src, dst)
    print(f"PDF moved to: {dst}")
else:
    print("No PDF found to move.")
