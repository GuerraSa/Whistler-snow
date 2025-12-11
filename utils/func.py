import playwright.sync_api
from playwright.sync_api import sync_playwright

def whistler_peak_scrape(url):
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
        page.wait_for_selector('.day-container', timeout=5000)

        # Retrieve the HTML content
        html = page.content()
        print(html)

        # Close the browser
        browser.close()

