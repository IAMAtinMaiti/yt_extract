"""
yt_extract

Headless Chrome + Selenium script to capture the YouTube Trending page
as a local PDF file. This is the first building block for a broader
YouTube data extraction and analysis pipeline.
"""

import base64
import time
from datetime import datetime
from pathlib import Path
from typing import Final

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# YouTube Trending search results endpoint to snapshot.
TARGET_URL: Final[str] = "https://www.youtube.com/results?search_query=trending"


def create_trending_pdf(
    target_url: str = TARGET_URL,
) -> str:
    """
    Render the YouTube Trending page to a PDF using headless Chrome.

    Args:
        target_url: The URL to open and capture as PDF.

    Returns:
        The path to the generated PDF file.
    """
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    output_pdf = data_dir / f"output_{datetime.now().isoformat()}.pdf"

    options = Options()
    options.add_argument(
        "--headless=new"
    )  # Required for PDF printing in recent versions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")

    # Let Selenium Manager resolve and download the correct ChromeDriver
    # version for the locally installed Chrome.
    driver = webdriver.Chrome(options=options)

    try:
        # Navigate to the target page.
        driver.get(target_url)

        # Wait for at least one video tile to appear so we don't
        # capture an empty shell layout.
        try:
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.TAG_NAME, "ytd-video-renderer"))
            )
            # Give thumbnails/styles a brief extra moment to settle.
            time.sleep(2)
        except TimeoutException:
            # If we never see a video tile, fall back to a short
            # static wait so we still get *something* in the PDF.
            time.sleep(5)

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

    return str(output_pdf)


if __name__ == "__main__":
    pdf_path = create_trending_pdf()
    print(f"✅ PDF saved to: {pdf_path}")
