import os
import time
import tempfile
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# === CONFIG ===
temp_user_data_dir = tempfile.mkdtemp()
today = datetime.now().strftime('%Y-%m-%d')
download_path = os.path.join(os.getcwd(), "downloads", today)
os.makedirs(download_path, exist_ok=True)

chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "plugins.always_open_pdf_externally": True
})

driver = webdriver.Chrome(
    service=Service("./chromedriver-linux64/chromedriver"),
    options=chrome_options
)

try:
    driver.get("https://meteo.gov.lk/")
    wait = WebDriverWait(driver, 20)

    try:
        english_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "English")))
        english_link.click()
        time.sleep(2)
    except Exception as e:
        print("⚠️ Failed to switch language:", e)

    weather_data_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Weather Data')]")))
    weather_data_button.click()

    pdf_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Weather Report for the 24hour Period")))
    pdf_link.click()

    time.sleep(10)

finally:
    driver.quit()

print(f"✅ Downloaded to: {download_path}")
print(f"📂 Files in download path: {os.listdir(download_path)}")

# === Move to reports/YYYY-MM-DD/ ===
today_folder = os.path.join("reports", today)
os.makedirs(today_folder, exist_ok=True)

downloaded_files = [f for f in os.listdir(download_path) if f.endswith(".pdf")]
if downloaded_files:
    for file in downloaded_files:
        src = os.path.join(download_path, file)
        dst = os.path.join(today_folder, f"weather-{today}.pdf")
        os.rename(src, dst)
    print(f"✅ Moved PDF to: {today_folder}")
else:
    print("⚠️ No PDF found to move.")
