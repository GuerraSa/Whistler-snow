import requests
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup
import time
from datetime import datetime, timedelta
import pytz
import re
from urllib.parse import urljoin
from google import genai
from google.genai import types
from cred import NOTION_ENDPOINT, HEADERS, GEMINI_API_KEY

# Configuration
station_db_id = '2c3e268796a880c3a15bc088669fbb2a'
conditions_db_id = '2dae268796a8808d87d6c970ab4d49ba'


# --- 1. NOTION CHECKS ---
def get_existing_entries(station_id, earliest_date_iso, db_id):
    """
    Fetches timestamps of existing entries for a specific station on or after a date.
    """
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {
        "filter": {
            "and": [
                {"property": "Weather Station", "relation": {"contains": station_id}},
                {"property": "Date", "date": {"on_or_after": earliest_date_iso}}
            ]
        }
    }
    existing_timestamps = set()
    has_more = True
    start_cursor = None

    while has_more:
        if start_cursor: payload["start_cursor"] = start_cursor
        response = requests.post(url, headers=HEADERS, json=payload)
        if response.status_code != 200: break
        data = response.json()
        for page in data.get("results", []):
            date_prop = page["properties"].get("Date", {}).get("date", {})
            if date_prop and date_prop.get("start"):
                existing_timestamps.add(date_prop.get("start"))
        has_more = data.get("has_more", False)
        start_cursor = data.get("next_cursor")
    return existing_timestamps


def get_all_stations(station_db_id):
    """Retrieves all station metadata from the Weather Stations database."""
    url = f"https://api.notion.com/v1/databases/{station_db_id}/query"
    response = requests.post(url, headers=HEADERS, json={})
    results = response.json().get("results", [])
    stations = []
    for page in results:
        props = page["properties"]
        name_title = props.get("Name", {}).get("title", [])
        if not name_title: continue

        station_name = name_title[0]["text"]["content"]
        ws = props.get("Weather System", {}).get("select")

        rt = props.get("Webcams", {}).get("rich_text", [])
        webcam_indices = []
        if rt:
            try:
                raw_text = rt[0]["plain_text"]
                webcam_indices = [
                    int(x.strip())
                    for x in raw_text.split(",")
                    if x.strip().isdigit() and int(x.strip()) > 0
                ]
            except ValueError:
                pass

        stations.append({
            "id": page["id"],
            "name": station_name,
            "weather_system_name": ws["name"] if ws else None,
            "wp_url": props.get("WhistlerPeak URL", {}).get("url"),
            "webcam_indices": webcam_indices
        })
    return stations


# --- 2. LOGIC & HELPERS ---
def safe_float(value, default=None):
    if value is None or (isinstance(value, str) and value.strip() == ""):
        return default
    try:
        return float(value)
    except ValueError:
        return default


def normalize_time_key(dt_obj):
    """Normalizes datetime to match graph keys (e.g., '12:30AM')."""
    hour = dt_obj.strftime("%I").lstrip("0")
    rest = dt_obj.strftime(":%M%p")
    return (hour + rest).upper()


def gemini_analyze_sky(image_url):
    print(f"      ✨ Asking Gemini to analyze: ...{image_url[-20:]}")

    try:
        client = genai.Client(api_key=GEMINI_API_KEY)

        img_resp = requests.get(image_url)
        if img_resp.status_code != 200:
            print("      ⚠️ Failed to download image for AI.")
            return None

        prompt = (
            "Look at this ski resort webcam. Classify the sky condition into exactly one of these words: "
            "Bluebird, Sunny, Cloudy, Overcast, Foggy, Night. "
            "If it is dark, say Night. 'Bluebird' means completely clear blue sky."
        )

        # FIX: Updated to 'gemini-1.5-flash-002' to resolve 404s
        try:
            response = client.models.generate_content(
                model="gemini-1.5-flash-002",
                contents=[
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_text(text=prompt),
                            types.Part.from_bytes(data=img_resp.content, mime_type="image/jpeg"),
                        ]
                    )
                ]
            )
        except Exception as e:
            # Fallback to the generic tag if 002 fails
            print(f"      ⚠️ '002' model failed, retrying generic 'gemini-1.5-flash'...")
            response = client.models.generate_content(
                model="gemini-1.5-flash",
                contents=[
                    types.Content(
                        role="user",
                        parts=[
                            types.Part.from_text(text=prompt),
                            types.Part.from_bytes(data=img_resp.content, mime_type="image/jpeg"),
                        ]
                    )
                ]
            )

        content = response.text.strip()
        print(f"      🤖 Gemini Result: {content}")

        valid_conditions = ["Bluebird", "Sunny", "Foggy", "Cloudy", "Overcast", "Night"]
        for cond in valid_conditions:
            if cond in content:
                return cond

        return None

    except Exception as e:
        print(f"      ⚠️ Gemini Analysis failed: {e}")
        return None


def infer_ski_condition(temp, wind_speed, precip_mm, rh, dt_object, ai_sky_override=None):
    """
    Infers condition string based on all available data points.
    """
    conditions = []

    t = temp if temp is not None else 0
    w = wind_speed if wind_speed is not None else 0
    p = precip_mm if precip_mm is not None else 0
    is_freezing = t <= 0

    # Night Check
    is_night_time = False
    if dt_object:
        hour = dt_object.hour
        if hour < 7 or hour >= 17:
            is_night_time = True

    # 1. PRECIPITATION (Highest Priority)
    if p > 0:
        if is_freezing:
            conditions.append("Snowing")
        else:
            conditions.append("Raining")

    # 2. SKY STATE
    else:
        if ai_sky_override:
            conditions.append(ai_sky_override)
        elif rh is not None:
            if rh < 40:
                conditions.append("Bluebird")
            elif rh < 60:
                conditions.append("Sunny")
            elif rh >= 95:
                conditions.append("Foggy")
            elif rh >= 85:
                conditions.append("Overcast")
            else:
                conditions.append("Cloudy")
        else:
            if is_night_time:
                conditions.append("Night")
            else:
                conditions.append("Fair")

    # 3. WIND
    if w > 60:
        conditions.append("Stormy")
    elif w > 35:
        conditions.append("Windy")

    return " & ".join(conditions)


def scrape_wind_chill_map(page, wp_url):
    """Scrapes the SVG graph to build a time -> wind chill map."""
    wc_map = {}
    if not wp_url: return wc_map
    try:
        if page.url != wp_url:
            page.goto(wp_url)
            try:
                page.wait_for_selector("path[aria-label*='Wind Chill']", timeout=5000)
            except:
                return wc_map

        elements = page.locator("path[aria-label*='Wind Chill']").all()
        for el in elements:
            label = el.get_attribute("aria-label")
            match = re.search(r"(\d{1,2}:\d{2}\s*[AP]M).*?(-?\d+(?:\.\d+)?)\.?\s*Wind Chill", label, re.IGNORECASE)
            if match:
                raw_time = match.group(1).replace(" ", "").upper()
                wc_map[raw_time] = float(match.group(2))
    except Exception:
        pass
    return wc_map


def get_webcam_images(context, url, indices):
    """
    Scrapes multiple images with a fallback for single-image pages like Pig Alley.
    """
    found = []
    page = context.new_page()
    print(f"   📷 checking for images at {url}...")
    try:
        page.goto(url)

        has_gallery = False
        try:
            page.wait_for_selector("#cam-gallery", timeout=8000)
            has_gallery = True
        except:
            print("      ⚠️ '#cam-gallery' not found, switching to fallback mode...")

        for i in indices:
            if has_gallery:
                # Standard gallery selector
                sel = f"#cam-gallery .thumbnail-list li:nth-child({i}) img"
            else:
                # Fallback: Just grab the i-th image on the page
                # Useful for pages like Pig Alley which might just have one <img> in a main div
                sel = f".container_wind img, .webcam-image img, img[src*='webcam']"

            # If we are in fallback mode, we select the list of ALL relevant images
            # and pick the one corresponding to index 'i' (converted to 0-based index)
            if not has_gallery:
                imgs = page.locator(sel).all()
                if len(imgs) >= i:
                    src = imgs[i - 1].get_attribute("src")  # index 1 -> list[0]
                    if src:
                        full_url = urljoin(page.url, src)
                        found.append(full_url)
                        print(f"      - Found Cam #{i} (Fallback): ...{full_url[-20:]}")
                else:
                    print(f"      - Cam #{i} not found in fallback images (found {len(imgs)}).")
            else:
                # Standard Gallery Logic
                if page.locator(sel).count() > 0:
                    src = page.locator(sel).first.get_attribute("src")
                    if src:
                        full_url = urljoin(page.url, src)
                        found.append(full_url)
                        print(f"      - Found Cam #{i}: ...{full_url[-20:]}")
                else:
                    print(f"      - Cam #{i} selector not found.")

        return found
    except Exception as e:
        print(f"      ⚠️ Webcam scrape error: {e}")
        return found
    finally:
        page.close()


# --- 3. MAIN SCRAPER ---
def get_station_data(meta, browser_context, latest_only=False):
    print(f"--- Processing: {meta['name']} ---")
    pst = pytz.timezone('America/Vancouver')
    now_pst = datetime.now(pst)
    extracted_entries = []

    page = browser_context.new_page()

    try:
        wc_map = scrape_wind_chill_map(page, meta['wp_url'])

        if meta['weather_system_name']:
            wb_url = "https://secure.whistlerblackcomb.com/weather/default.aspx"
            dd_name = meta['weather_system_name']

            page.goto(wb_url)

            target = None
            for sel in ["#ddlWH", "#ddlBC"]:
                opts = page.locator(f"{sel} option").all_inner_texts()
                if dd_name in [o.strip() for o in opts]:
                    target = sel
                    break

            if target:
                page.select_option(target, label=dd_name)
                page.wait_for_load_state("networkidle")
                time.sleep(1.5)

                soup = BeautifulSoup(page.content(), "html.parser")
                rows = soup.find_all("tr", class_=["GridRow", "GridRowAlt"])

                if latest_only and rows: rows = [rows[-1]]

                for row in rows:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    try:
                        dt = datetime.strptime(cells[0], "%m/%d/%Y %I:%M:%S %p")
                        final_dt = pst.localize(dt)
                    except ValueError:
                        final_dt = now_pst

                    lookup_key = normalize_time_key(final_dt)

                    extracted_entries.append({
                        "name_title": final_dt.strftime("%Y-%m-%d %I:%M%p %Z"),
                        "timestamp": final_dt.isoformat(),
                        "dt_object": final_dt,
                        "temp": safe_float(cells[1]),
                        "wind_chill": wc_map.get(lookup_key),
                        "avg_wind": safe_float(cells[2]),
                        "max_wind": safe_float(cells[3]),
                        "wind_dir": safe_float(cells[4]),
                        "rh": safe_float(cells[5]),
                        "bp": safe_float(cells[6]),
                        "base_snow": safe_float(cells[7]),
                        "new_snow": safe_float(cells[8]),
                        "precip": safe_float(cells[9]),
                        "sum_precip": safe_float(cells[10]),
                        "station_page_id": meta['id']
                    })

        elif meta['wp_url']:
            if page.url != meta['wp_url']: page.goto(meta['wp_url'])

            blocks = page.locator(".container_wind").all()
            if latest_only and blocks: blocks = [blocks[0]]

            for block in blocks:
                r_time = block.locator(".noWind-date").inner_text().strip()
                r_temp = block.locator(".tempValue").inner_text().strip()

                try:
                    t_obj = datetime.strptime(r_time, "%I:%M%p")
                    final_dt = now_pst.replace(hour=t_obj.hour, minute=t_obj.minute, second=0, microsecond=0)
                    if final_dt > now_pst + timedelta(minutes=30):
                        final_dt -= timedelta(days=1)
                except:
                    continue

                lookup_key = normalize_time_key(final_dt)

                extracted_entries.append({
                    "name_title": final_dt.strftime("%Y-%m-%d %I:%M%p %Z"),
                    "timestamp": final_dt.isoformat(),
                    "dt_object": final_dt,
                    "temp": safe_float(r_temp.replace("°C", "")),
                    "wind_chill": wc_map.get(lookup_key),
                    "avg_wind": None, "max_wind": None, "wind_dir": None,
                    "rh": None, "bp": None, "base_snow": None,
                    "new_snow": None, "precip": None, "sum_precip": None,
                    "station_page_id": meta['id']
                })

    except Exception as e:
        print(f"❌ Error scraping {meta['name']}: {e}")
    finally:
        page.close()

    if meta['webcam_indices'] and meta['wp_url'] and extracted_entries:
        extracted_entries.sort(key=lambda x: x['dt_object'])
        latest = extracted_entries[-1]

        imgs = get_webcam_images(browser_context, meta['wp_url'], meta['webcam_indices'])

        ai_condition = None
        if imgs:
            latest["image_urls"] = imgs
            ai_condition = gemini_analyze_sky(imgs[0])

        for entry in extracted_entries:
            is_latest = (entry == latest)
            override = ai_condition if is_latest else None

            entry["condition"] = infer_ski_condition(
                entry["temp"], entry["avg_wind"], entry["precip"], entry["rh"],
                entry["dt_object"], override
            )

    elif extracted_entries:
        for entry in extracted_entries:
            entry["condition"] = infer_ski_condition(
                entry["temp"], entry["avg_wind"], entry["precip"], entry["rh"],
                entry["dt_object"], None
            )

    return extracted_entries


# --- 4. UPLOAD ---
def create_condition_entry(data, db_id=conditions_db_id):
    url = "https://api.notion.com/v1/pages"
    props = {
        "Name": {"title": [{"text": {"content": data.get("name_title")}}]},
        "Date": {"date": {"start": data.get("timestamp")}},
        "Weather Station": {"relation": [{"id": data.get("station_page_id")}]},
        "Temperature": {"number": data.get("temp")},
        "Wind Chill": {"number": data.get("wind_chill")},
        "Avg. Wind Speed (km/h)": {"number": data.get("avg_wind")},
        "Max Wind Speed (km/h)": {"number": data.get("max_wind")},
        "Wind Direction": {"number": data.get("wind_dir")},
        "RH (%)": {"number": data.get("rh")},
        "BP (Mb)": {"number": data.get("bp")},
        "Base Snow (cm)": {"number": data.get("base_snow")},
        "New Snow (cm)": {"number": data.get("new_snow")},
        "Precipitation (mm)": {"number": data.get("precip")},
        "Sum Precipitation (mm)": {"number": data.get("sum_precip")},
        "Condition": {"select": {"name": data.get("condition")}}
    }

    if data.get("image_urls"):
        files_payload = []
        for i, u in enumerate(data.get("image_urls")):
            files_payload.append({
                "name": f"Cam {i + 1}",
                "type": "external",
                "external": {"url": u}
            })
        props["Files"] = {"files": files_payload}
        print(f"      📎 Attaching {len(files_payload)} images to upload.")

    requests.post(url, headers=HEADERS, json={"parent": {"database_id": db_id}, "properties": props})


# --- 5. EXECUTION ---
def process_station(station_meta, context, latest_only=False):
    data_list = get_station_data(station_meta, context, latest_only=latest_only)
    if not data_list: return

    data_list.sort(key=lambda x: x['dt_object'])
    earliest_iso = data_list[0]['timestamp']

    print(f"   🔍 Checking Notion history...")
    existing_set = get_existing_entries(station_meta['id'], earliest_iso, conditions_db_id)

    new_count = 0
    for data in data_list:
        if data['timestamp'] not in existing_set:
            print(f"      + Uploading: {data['name_title']} | Cond: {data['condition']}")
            create_condition_entry(data)
            new_count += 1
        else:
            if latest_only:
                print(f"      ⚠️ SKIPPING: Entry {data['name_title']} already exists.")

    if new_count > 0:
        print(f"   ✅ Uploaded {new_count} new entries for {station_meta['name']}")


def debug_sync_webcam_stations():
    print("🐞 Starting DEBUG Sync (Webcams Only, Latest Data Only)...")
    all_stations = get_all_stations(station_db_id)
    webcam_stations = [s for s in all_stations if s['webcam_indices']]
    print(f"📋 Found {len(webcam_stations)} webcam stations.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        for station_meta in webcam_stations:
            process_station(station_meta, context, latest_only=True)
        browser.close()
    print("\n🏁 Debug Sync Complete.")


def sync_all_stations():
    print("🚀 Starting Weather Sync...")
    stations = get_all_stations(station_db_id)
    print(f"📋 Found {len(stations)} stations to process.")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()
        for station_meta in stations:
            process_station(station_meta, context)
        browser.close()
    print("\n🏁 All Stations Synced.")



sync_all_stations()
debug_sync_webcam_stations()