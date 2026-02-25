"""
yt_extract

Headless Chrome + Selenium script to capture the YouTube Trending page
as a local PDF file. This is the first building block for a broader
YouTube data extraction and analysis pipeline.
"""

import base64
import time
from datetime import datetime
from typing import Final

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# YouTube Trending endpoint to snapshot.
TARGET_URL: Final[str] = "https://www.youtube.com/feed/trending"


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
    output_pdf = f"output_{datetime.now().isoformat()}.pdf"

    options = Options()
    options.add_argument(
        "--headless=new"
    )  # Required for PDF printing in recent versions
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-gpu")

    # Let Selenium Manager resolve and download the correct ChromeDriver
    # version for the locally installed Chrome.
    driver = webdriver.Chrome(options=options)

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
