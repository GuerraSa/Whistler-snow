import bs4
import re
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from internal_tools import NotionClient

# Internal Imports
import config
from cred import NOTION_TOKEN
from core import scraper, utils


# --- HELPER: Get Relation ID for Elevations ---
def get_forecast_relation_id(search_term):
    """Finds the Page ID for a specific elevation in the Elevations DB."""
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecast Elevations'])

    # WORKAROUND: Fetch all rows and filter in Python to avoid API filter errors
    response = client.query_database()
    results = response.get("results", []) if isinstance(response, dict) else response

    for page in results:
        props = page.get("properties", {})
        title_list = props.get("Weather Source + Elevation", {}).get("title", [])
        if title_list:
            title_text = title_list[0]["text"]["content"]
            if search_term in title_text:
                return page["id"]

    return None


# --- HELPER: Check Existing Forecasts ---
def fetch_existing_forecasts(elevation):
    """Gets currently active forecasts to prevent duplicate entries."""
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecasts'])

    print(f"Checking Notion for existing {elevation} reports...")

    # WORKAROUND: Fetch recent rows and filter in Python
    # (Notion API returns recent rows by default, so simple query usually suffices for recent checks)
    response = client.query_database()
    results = response.get("results", []) if isinstance(response, dict) else response

    if not results: return None

    # Python Filtering
    filtered_results = []
    for page in results:
        props = page.get("properties", {})

        # Check 1: Title starts with elevation
        title_list = props.get("Elevation + Update Time", {}).get("title", [])
        if not title_list or not title_list[0]["text"]["content"].startswith(elevation):
            continue

        # Check 2: Latest Report is Checked
        is_latest = props.get("Latest Report?", {}).get("checkbox", False)
        if is_latest:
            filtered_results.append(page)

    return client.results_to_dataframe(filtered_results)


# ==========================================
# MAIN CONTROLLER
# ==========================================
def update_forecast(elevation):
    print(f"--- ☁️ Processing {elevation} Forecast ---")
    url = config.URLS['Weather Forecast'][elevation]

    if elevation == "1800m":
        _process_rwdi_1800m(url, elevation)
    else:
        _process_snow_forecast(url, elevation)


# ==========================================
# LOGIC A: SNOW-FORECAST.COM (1480m, 2248m)
# ==========================================
def _process_snow_forecast(url, elevation):
    # 1. Scrape
    html = scraper.scrape_dynamic_content(url, '.forecast-table')
    if not html: return
    soup = bs4.BeautifulSoup(html, "html.parser")

    meta_dict = {}
    vancouver_tz = ZoneInfo("America/Vancouver")
    now_van = datetime.now(vancouver_tz)
    report_dt_obj = now_van

    # 2. Extract Metadata
    intro_div = soup.find("div", class_="weather-intro")
    if intro_div:
        text = intro_div.get_text(strip=True)
        match = re.search(r'Updated:\s*(\d+)\s*(min|hour)', text)
        if match:
            delta = timedelta(minutes=int(match.group(1))) if "min" in match.group(2) else timedelta(
                hours=int(match.group(1)))
            report_dt_obj = now_van - delta

    h = report_dt_obj.hour
    meta_dict["Edition"] = "AM" if h < 12 else ("PM" if h < 18 else "Night")

    meta_dict["Synopsis"] = "No synopsis"
    summ_div = soup.find('div', class_='about-weather-summary__content')
    if summ_div:
        txt = summ_div.get_text(" ", strip=True)
        if "Weather (Next 3 days):" in txt:
            meta_dict["Synopsis"] = txt.split('):', 1)[-1].strip()

    # 3. Extract Table Data
    table = soup.find('table', class_='forecast-table__table')
    if not table: return

    def get_row(name, sel=None, attr=None):
        data = []
        row = table.find('tr', attrs={'data-row': name})
        if row:
            for cell in row.find_all('td'):
                colspan = int(cell.get('colspan', 1))
                val = "-"
                if attr:
                    val = cell.get(attr)
                elif sel:
                    el = cell.select_one(sel)
                    if el: val = el.get_text(strip=True)
                else:
                    val = cell.get_text(strip=True)
                for _ in range(colspan): data.append(val)
        return data

    dates = get_row('days', attr='data-date')
    times = ["Night" if t == "night" else t for t in get_row('time')]
    summaries = get_row('phrases', '.forecast-table__phrase')
    snows = get_row('snow', '.snow-amount__value')
    rains = get_row('rain', '.rain-amount__value') or ["-"] * len(dates)
    highs = get_row('temperature-max', '.temp-value')
    lows = get_row('temperature-min', '.temp-value')
    freezing_levels = get_row('freezing-level', '.level-value')
    winds = get_row('wind')

    # 4. Upload
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecasts'])
    P = client.Props
    rel_id = get_forecast_relation_id(elevation)

    # Existing Check
    existing_df = fetch_existing_forecasts(elevation)
    existing_keys = set()
    if existing_df is not None and not existing_df.empty:
        for _, row in existing_df.iterrows():
            if row.get('Forecast Date') and row.get('Time of Day'):
                existing_keys.add((row['Forecast Date'], row['Time of Day']))

    for i in range(len(dates)):
        date_key = dates[i]
        period = times[i]

        if (date_key, period) in existing_keys: continue

        s_val = utils.clean_notion_number(snows[i])
        r_val = utils.clean_notion_number(rains[i])
        p_type = "Snow" if s_val > 0 else ("Rain" if r_val > 0 else "None")
        p_amount = s_val if s_val > 0 else r_val

        props = {
            "Elevation + Update Time": P.title(f"{elevation} - {report_dt_obj.strftime('%Y-%m-%d %H:%M')}"),
            "Report Date": P.date(report_dt_obj.isoformat()),
            "Forecast Date": P.date(date_key),
            "Time of Day": P.select(period),
            "Forecast Type": P.rich_text(meta_dict["Edition"]),
            "Synopsis": P.rich_text(meta_dict["Synopsis"][:2000]),
            "Daily Summary": P.rich_text(summaries[i]),
            "Precipitation Type": P.select(p_type),
            "Precipitation Amount": P.number(p_amount),
            "High": P.number(utils.clean_notion_number(highs[i])),
            "Low": P.number(utils.clean_notion_number(lows[i])),
            "Freezing Level (m)": P.number(utils.clean_notion_number(freezing_levels[i]))
        }

        if rel_id: props["Forecast Elevation"] = P.relation([rel_id])
        client.add_row(properties=props)
        print(f"✅ Uploaded {date_key} ({period})")


# ==========================================
# LOGIC B: RWDI / 1800m
# ==========================================
def _process_rwdi_1800m(url, elevation):
    html = scraper.scrape_dynamic_content(url, '.alpine__container')
    if not html: return
    soup = bs4.BeautifulSoup(html, "html.parser")

    meta_dict = {}
    time_blocks = soup.find_all("div", class_='alpine__time-container')
    if len(time_blocks) >= 1:
        txt = time_blocks[0].get_text(" ", strip=True)
        if "Report date:" in txt:
            clean_date = txt.split("Report date:")[1].split("Forecast")[0].strip().rstrip('.')
            meta_dict["Report Date"] = utils.parse_whistler_date(clean_date.split(' ', 1)[1])

    cards = soup.find_all("div", class_="alpine__card")
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecasts'])
    P = client.Props
    rel_id = get_forecast_relation_id(elevation)

    for card in cards:
        day_name = card.find("h3", class_="alpine__card-period").get_text(strip=True)
        summary = card.find("p", class_="alpine__card-summary").get_text(strip=True)
        temps = card.find("p", class_="alpine__card-temps").get_text(strip=True)
        high = re.search(r'High\s*([-\d]+)', temps)
        low = re.search(r'Low\s*([-\d]+)', temps)

        props = {
            "Elevation + Update Time": P.title(f"{elevation} - {datetime.now().strftime('%H:%M')}"),
            "Forecast Date": P.date(datetime.now().strftime("%Y-%m-%d")),
            "Time of Day": P.select(day_name),
            "Daily Summary": P.rich_text(summary),
            "High": P.number(int(high.group(1)) if high else 0),
            "Low": P.number(int(low.group(1)) if low else 0)
        }

        if rel_id: props["Forecast Elevation"] = P.relation([rel_id])
        client.add_row(properties=props)
        print(f"✅ Uploaded {day_name}")


# ==========================================
# SCHEDULER
# ==========================================
def get_time_until_update():
    print("--- ⏱️ Checking Schedule ---")
    url = config.URLS['Weather Forecast']['1480m']
    html = scraper.scrape_dynamic_content(url, '.forecast-table')
    if not html: return 60

    soup = bs4.BeautifulSoup(html, "html.parser")
    update_node = soup.find("span", class_="location-issued__update")

    if update_node:
        try:
            h = int(update_node.find("span", class_="hours").get_text(strip=True))
            m = int(update_node.find("span", class_="minutes").get_text(strip=True))
            return h * 60 + m
        except:
            pass
    return 60