from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import base64
import time
from datetime import datetime

# Set your chromedriver path here
CHROMEDRIVER_PATH = "/home/narutouzumaki/tools/chromedriver"
OUTPUT_PDF = f"output_{datetime.now().isoformat()}.pdf"
TARGET_URL = "https://www.youtube.com/feed/trending"

# Chrome options
options = Options()
options.add_argument("--headless=new")  # Required for PDF printing in recent versions
options.add_argument("--no-sandbox")
options.add_argument("--disable-gpu")

# Create driver
service = Service(executable_path=CHROMEDRIVER_PATH)
driver = webdriver.Chrome(service=service, options=options)

# Navigate to the page
driver.get(TARGET_URL)
time.sleep(3)  # Wait for JS content to load

# Use Chrome DevTools Protocol to print to PDF
pdf = driver.execute_cdp_cmd("Page.printToPDF", {
    "landscape": False,
    "printBackground": True,
    "paperWidth": 8.27,
    "paperHeight": 11.69
})

# Save PDF to file
with open(OUTPUT_PDF, "wb") as f:
    f.write(base64.b64decode(pdf['data']))

driver.quit()
print(f"✅ PDF saved to: {OUTPUT_PDF}")
