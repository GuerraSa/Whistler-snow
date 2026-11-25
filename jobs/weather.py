import bs4
import re
from datetime import datetime
from internal_tools import NotionClient

# Internal Imports
import config
from cred import NOTION_TOKEN
from core import scraper, utils


# --- HELPER: Get Relation ID for Elevations ---
def get_forecast_relation_id(search_term):
    """Finds the Page ID for a specific elevation in the Elevations DB."""
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecast Elevations'])

    for page in client.query_datasource(strict=True):
        title_list = page.get("properties", {}).get("Weather Source + Elevation", {}).get("title", [])
        if title_list and search_term in title_list[0]["plain_text"]:
            return page["id"]

    return None


# --- HELPER: Check Existing Forecasts ---
def fetch_existing_forecasts(client, title):
    """(Forecast Date, Time of Day) pairs already uploaded for this report, so a re-run adds only what is missing."""
    print(f"Checking Notion for existing '{title}' rows...")
    rows = client.query_datasource(
        payload={"filter": {"property": "Elevation + Update Time", "title": {"equals": title}}}, strict=True)
    keys = set()
    for row in rows:
        props = row["properties"]
        keys.add(((props["Forecast Date"]["date"] or {}).get("start"),
                  (props["Time of Day"]["select"] or {}).get("name")))
    return keys


# ==========================================
# MAIN CONTROLLER
# ==========================================
def update_forecast(elevation):
    print(f"--- ☁️ Processing {elevation} Forecast ---")
    url = config.URLS['Weather Forecast'][elevation]

    if elevation == "1800m":
        return _process_rwdi_1800m(url, elevation)
    return _process_snow_forecast(url, elevation)


# ==========================================
# LOGIC A: SNOW-FORECAST.COM (1480m, 2248m)
# ==========================================
def _process_snow_forecast(url, elevation):
    """Uploads one snow-forecast.com report. Returns minutes until the next update, or None."""
    # 1. Scrape
    html = scraper.scrape_dynamic_content(url, '.forecast-table')
    if not html: return None
    soup = bs4.BeautifulSoup(html, "html.parser")
    minutes_to_update = _minutes_until_update(soup)

    meta_dict = {}

    # 2. Extract Metadata: the issue time ('9 am 25 Nov 2026') is the report's identity
    issued_el = soup.select_one(".location-subheader-issued__value b")
    try:
        report_dt_obj = datetime.strptime(issued_el.get_text(strip=True), "%I %p %d %b %Y").replace(tzinfo=utils.WHISTLER_TZ)
    except (AttributeError, ValueError):
        print("⚠️ No 'Issued' time found; skipping upload so a re-run can't duplicate rows.")
        return minutes_to_update

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
    if not table: return minutes_to_update

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

    # 4. Upload
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecasts'])
    P = client.Props
    rel_id = get_forecast_relation_id(elevation)
    title = f"{elevation} - {report_dt_obj.strftime('%Y-%m-%d %H:%M')}"
    existing_keys = fetch_existing_forecasts(client, title)

    for i in range(len(dates)):
        date_key = dates[i]
        period = times[i]

        if (date_key, period) in existing_keys: continue

        s_val = utils.clean_notion_number(snows[i])
        r_val = utils.clean_notion_number(rains[i])
        p_type = "Snow" if s_val > 0 else ("Rain" if r_val > 0 else "None")
        p_amount = s_val if s_val > 0 else r_val

        props = {
            "Elevation + Update Time": P.title(title),
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

    return minutes_to_update


# ==========================================
# LOGIC B: RWDI / 1800m
# ==========================================
def _process_rwdi_1800m(url, elevation):
    html = scraper.scrape_dynamic_content(url, '.alpine__container')
    if not html: return
    soup = bs4.BeautifulSoup(html, "html.parser")

    # 'Augmented forecast Issued Wed Nov 25 · 3:00pm' is the report's identity
    time_block = soup.find("div", class_='alpine__time-container')
    match = re.search(r'Issued\s+\w+\s+(\w+ \d+)\s*·\s*(\d+:\d+\s*[ap]m)', time_block.get_text(" ", strip=True) if time_block else "")
    if not match:
        print("⚠️ No 'Issued' time found; skipping upload so a re-run can't duplicate rows.")
        return
    issued = utils.parse_recent_date(f"{match[1]} {match[2].replace(' ', '')}", "%b %d %I:%M%p",
                                     datetime.now(utils.WHISTLER_TZ)).replace(tzinfo=utils.WHISTLER_TZ)

    cards = soup.find_all("div", class_="alpine__card")
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS['Weather Forecasts'])
    P = client.Props
    rel_id = get_forecast_relation_id(elevation)
    title = f"{elevation} - {issued.strftime('%Y-%m-%d %H:%M')}"
    existing_keys = fetch_existing_forecasts(client, title)

    for card in cards:
        day_name = card.find("h3", class_="alpine__card-period").get_text(strip=True)
        forecast_date = utils.card_date(day_name, issued.date()).isoformat()
        if (forecast_date, day_name) in existing_keys: continue

        summary = card.find("p", class_="alpine__card-summary").get_text(strip=True)
        temps = card.find("p", class_="alpine__card-temps").get_text(strip=True)
        high = re.search(r'High\s*([-\d]+)', temps)
        low = re.search(r'Low\s*([-\d]+)', temps)

        props = {
            "Elevation + Update Time": P.title(title),
            "Report Date": P.date(issued.isoformat()),
            "Forecast Date": P.date(forecast_date),
            "Time of Day": P.select(day_name),
            "Daily Summary": P.rich_text(summary),
            "High": P.number(int(high.group(1)) if high else None),
            "Low": P.number(int(low.group(1)) if low else None)
        }

        if rel_id: props["Forecast Elevation"] = P.relation([rel_id])
        client.add_row(properties=props)
        print(f"✅ Uploaded {day_name} ({forecast_date})")


# ==========================================
# SCHEDULER
# ==========================================
def _minutes_until_update(soup):
    """Reads the page's 'Forecast update in: X hr. Y min.' countdown. None if it is missing."""
    node = soup.select_one("[data-countdown]")
    try:
        return int(node.select_one(".hours").get_text(strip=True)) * 60 + int(node.select_one(".minutes").get_text(strip=True))
    except (AttributeError, ValueError):
        return None