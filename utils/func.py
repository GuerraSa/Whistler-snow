import playwright.sync_api
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
    Parses 'December 8, 2025 3pm' using strptime.
    Format used: %B %d, %Y %I%p
    """
    try:
        # 1. Clean whitespace
        clean_str = date_str.strip()

        # 2. Convert to uppercase for the AM/PM code (%p)
        # strptime requires 'PM', but your data has 'pm'
        clean_str = clean_str.upper()

        # 3. Parse using strptime
        # %B = Full Month Name (December)
        # %d = Day of month (8)
        # %Y = Year with century (2025)
        # %I = Hour (12-hour clock) (03)
        # %p = AM or PM (PM)
        return datetime.strptime(clean_str, "%B %d, %Y %I%p")

    except ValueError as e:
        print(f"Could not parse date: {e}")
        return None


