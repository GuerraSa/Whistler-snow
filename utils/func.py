from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright

def whistler_peak_scrape(url, selector):
    # Start Playwright
    with sync_playwright() as p:
        # Launch a headless browser
        browser = p.chromium.launch(headless=True)
        # Open a new browser tab
        page = browser.new_page()
        page.set_viewport_size({"width": 1920, "height": 1080})  # Set window size

        # scrape the reviews page
        page.goto(url)
        # wait up to 5 seconds for element to appear
        page.wait_for_selector(selector, timeout=5000)

        # Retrieve the HTML content
        html = page.content()

        # Close the browser
        browser.close()

    return html

def parse_whistler_date(date_str):
    """
    Parses 'December 8, 2025 3pm' and manually adds 8 hours to get UTC.
    """
    try:
        clean_str = date_str.strip().upper()

        # 1. Parse the local time (e.g., 3:00 PM)
        dt_local = datetime.strptime(clean_str, "%B %d, %Y %I%p")

        # 2. Add 8 hours to convert PST to UTC
        # (3pm PST + 8 hours = 11pm UTC)
        dt_utc = dt_local + timedelta(hours=8)

        # 3. Return as a string formatted for Notion
        # We manually append "Z" or "+00:00" so Notion knows it is UTC
        return dt_utc.isoformat() + "Z"

    except ValueError as e:
        print(f"Could not parse date: {e}")
        return None


