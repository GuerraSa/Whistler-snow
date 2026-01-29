import requests
import base64
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urljoin

from internal_tools import NotionClient
from cred import NOTION_TOKEN, GEMINI_API_KEY
import config
from core import scraper, utils


# --- GEMINI AI ---
def gemini_analyze_sky(image_url):
    print(f"      ✨ Asking Gemini to analyze: ...{image_url[-20:]}")
    try:
        img_resp = requests.get(image_url)
        if img_resp.status_code != 200: return None

        b64_image = base64.b64encode(img_resp.content).decode("utf-8")
        api_url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-1.5-flash:generateContent?key={GEMINI_API_KEY}"
        prompt = "Look at this ski resort webcam. Classify the sky condition into one word: Bluebird, Sunny, Cloudy, Overcast, Foggy, Night. If dark, say Night."
        payload = {"contents": [
            {"parts": [{"text": prompt}, {"inline_data": {"mime_type": "image/jpeg", "data": b64_image}}]}]}

        response = requests.post(api_url, json=payload)
        if response.status_code == 200:
            content = response.json()['candidates'][0]['content']['parts'][0]['text'].strip()
            print(f"      🤖 Gemini Result: {content}")
            for cond in ["Bluebird", "Sunny", "Foggy", "Cloudy", "Overcast", "Night"]:
                if cond in content: return cond
            return "Cloudy"
    except Exception as e:
        print(f"      ⚠️ AI Analysis failed: {e}")
    return None


# --- HELPER: Webcam Extraction ---
def extract_webcam_urls(page, indices, base_url):
    found = []
    try:
        page.wait_for_selector("#cam-gallery", timeout=5000)
        has_gallery = True
    except:
        has_gallery = False

    for i in indices:
        src = None
        if has_gallery:
            sel = f"#cam-gallery .thumbnail-list li:nth-child({i}) img"
            if page.locator(sel).count() > 0:
                src = page.locator(sel).first.get_attribute("src")
        else:
            sel = f".container_wind img, .webcam-image img, img[src*='webcam']"
            imgs = page.locator(sel).all()
            if len(imgs) >= i:
                src = imgs[i - 1].get_attribute("src")
        if src: found.append(urljoin(base_url, src))
    return found


# --- MAIN SYNC ---
def sync_conditions():
    print("--- 🎿 Syncing Ski Conditions & Webcams ---")

    client_stations = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Stations'])
    # RESPONSE FIX
    response = client_stations.query_database()
    stations = response.get("results", []) if isinstance(response, dict) else response

    client_conditions = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Ski Conditions'])

    with scraper.sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        page = context.new_page()

        for st in stations:
            props = st['properties']
            title_list = props.get('Name', {}).get('title', [])
            if not title_list: continue
            name = title_list[0]['text']['content']

            wp_url = props.get('WhistlerPeak URL', {}).get('url')
            rt = props.get('Webcams', {}).get('rich_text', [])
            webcams = [int(x.strip()) for x in rt[0]['plain_text'].split(',') if x.strip().isdigit()] if rt else []

            if not wp_url: continue
            print(f"Processing {name}...")

            try:
                page.goto(wp_url, timeout=60000)
                temp_val = None
                try:
                    temp_el = page.locator(".tempValue").first
                    if temp_el.count() > 0:
                        temp_val = utils.safe_float(temp_el.inner_text().replace("°C", ""))
                except:
                    pass

                condition = "Cloudy"
                imgs = []
                if webcams:
                    imgs = extract_webcam_urls(page, webcams, wp_url)
                    if imgs:
                        ai_result = gemini_analyze_sky(imgs[0])
                        if ai_result: condition = ai_result

                if temp_val is not None or imgs:
                    P = client_conditions.Props
                    row_props = {
                        "Name": P.title(f"{name} - {datetime.now().strftime('%H:%M')}"),
                        "Temperature": P.number(temp_val),
                        "Weather Station": P.relation([st['id']]),
                        "Condition": P.select(condition),
                        "Date": P.date(datetime.now().isoformat())
                    }
                    if imgs:
                        files_payload = [{"name": f"Cam {i + 1}", "type": "external", "external": {"url": u}} for i, u
                                         in enumerate(imgs)]
                        row_props["Files"] = {"files": files_payload}

                    client_conditions.add_row(properties=row_props)
                    print(f"✅ Uploaded {name} (Cond: {condition})")

            except Exception as e:
                print(f"Error {name}: {e}")

        browser.close()