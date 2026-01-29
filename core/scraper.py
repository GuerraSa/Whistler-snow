from playwright.sync_api import sync_playwright


def scrape_dynamic_content(url, selector=None, timeout=60000):
    """
    Scrapes a URL using Playwright.
    """
    content = None
    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(viewport={"width": 1920, "height": 1080})

            page.goto(url, timeout=timeout)

            if selector:
                page.wait_for_selector(selector, timeout=timeout)

            content = page.content()
            browser.close()
        except Exception as e:
            print(f"❌ Scrape Error ({url}): {e}")

    return content