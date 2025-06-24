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

# Temporary Chrome profile
temp_user_data_dir = tempfile.mkdtemp()

# Today's folder
today = datetime.now().strftime('%Y-%m-%d')
download_path = os.path.join(os.getcwd(), "downloads", today)
os.makedirs(download_path, exist_ok=True)

# Chrome setup
chrome_options = Options()
chrome_options.binary_location = os.path.abspath("./chrome-linux64/chrome")
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-software-rasterizer")
chrome_options.add_argument(f"--user-data-dir={temp_user_data_dir}")
chrome_options.add_experimental_option("prefs", {
    "download.default_directory": download_path,
    "plugins.always_open_pdf_externally": True
})

# Start browser
driver = webdriver.Chrome(
    service=Service(os.path.abspath("./chromedriver-linux64/chromedriver")),
    options=chrome_options
)

try:
    driver.get("https://meteo.gov.lk/")
    wait = WebDriverWait(driver, 20)

    # ✅ Step 1: Switch to English (important!)
    try:
        english_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "English")))
        english_link.click()
        time.sleep(2)
    except Exception as e:
        print("⚠️ Failed to switch language:", e)

    # ✅ Step 2: Click "Weather Data"
    weather_data_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Weather Data')]")))
    weather_data_button.click()

    # ✅ Step 3: Click PDF link
    pdf_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Weather Report for the 24hour Period")))
    pdf_link.click()

    # ✅ Step 4: Wait for download
    time.sleep(10)

finally:
    driver.quit()

print(f"✅ Weather report downloaded to: {download_path}")
