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

# Create unique temp folder for Chrome user data
temp_user_data_dir = tempfile.mkdtemp()

# Create a folder for today's date
today = datetime.now().strftime('%Y-%m-%d')
download_path = os.path.join(os.getcwd(), "downloads", today)
os.makedirs(download_path, exist_ok=True)

# Configure Chrome
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
    service=Service(executable_path=os.path.abspath("./chromedriver-linux64/chromedriver")),
    options=chrome_options
)

# Visit the site
driver.get("https://meteo.gov.lk/")
wait = WebDriverWait(driver, 20)

# ✅ Step 0: Click "English" to switch language
try:
    english_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(text(), 'English')]")))
    english_link.click()
    print("✅ Switched to English version of the site")
    time.sleep(2)  # allow language switch
except Exception as e:
    print("⚠️ Could not switch to English:", e)

# ✅ Step 1: Click "Weather Data" button
weather_data_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Weather Data')]")))
weather_data_button.click()

# ✅ Step 2: Wait and click "Weather Report for the 24hour Period" link
pdf_link = wait.until(EC.element_to_be_clickable((By.LINK_TEXT, "Weather Report for the 24hour Period")))
pdf_link.click()

# ✅ Step 3: Wait for PDF to download
time.sleep(10)
driver.quit()

print(f"✅ Weather report downloaded to: {download_path}")
