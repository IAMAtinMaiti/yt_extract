"""
yt_extract

Headless Chrome + Selenium script to capture the YouTube Trending page
as a local PDF file. This is the first building block for a broader
YouTube data extraction and analysis pipeline.
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import base64
import time
from datetime import datetime
from typing import Final

# Absolute path to your chromedriver binary.
# Update this value to match your local environment.
CHROMEDRIVER_PATH: Final[str] = "/home/narutouzumaki/tools/chromedriver"

# YouTube Trending endpoint to snapshot.
TARGET_URL: Final[str] = "https://www.youtube.com/feed/trending"


def create_trending_pdf(
    chromedriver_path: str = CHROMEDRIVER_PATH,
    target_url: str = TARGET_URL,
) -> str:
    """
    Render the YouTube Trending page to a PDF using headless Chrome.

    Args:
        chromedriver_path: Filesystem path to the chromedriver binary.
        target_url: The URL to open and capture as PDF.

    Returns:
        The path to the generated PDF file.
    """
    output_pdf = f"output_{datetime.now().isoformat()}.pdf"

    options = Options()
    options.add_argument("--headless=new")  # Required for PDF printing in recent versions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    service = Service(executable_path=chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # Navigate to the target page and allow JS content to render.
        driver.get(target_url)
        time.sleep(3)

        # Use Chrome DevTools Protocol to print the current page to PDF.
        pdf = driver.execute_cdp_cmd(
            "Page.printToPDF",
            {
                "landscape": False,
                "printBackground": True,
                "paperWidth": 8.27,
                "paperHeight": 11.69,
            },
        )

        # Persist PDF to disk.
        with open(output_pdf, "wb") as f:
            f.write(base64.b64decode(pdf["data"]))
    finally:
        driver.quit()

    return output_pdf


if __name__ == "__main__":
    pdf_path = create_trending_pdf()
    print(f"✅ PDF saved to: {pdf_path}")

