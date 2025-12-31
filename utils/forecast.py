import httpx
import bs4
import re
import pprint
import pandas as pd
import requests
import time  # Added for retry delays
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from playwright.sync_api import sync_playwright

# Local Imports
from cred import NOTION_ENDPOINT, HEADERS
import cred
from notion_dbs import notion_db_id
from data_urls import whistler_data_url
from utils.func import whistler_peak_scrape, parse_whistler_date

# ==========================================
# 0. CONSTANTS & CONFIG
# ==========================================

SNOW_HISTORY_DB_ID = '145e268796a88032969be9ff33906b3e'
CURRENT_SEASON_PAGE_ID = "2b8e268796a88004b66bc101e8f04340"


# ==========================================
# 1. SHARED HELPER FUNCTIONS
# ==========================================

def send_to_notion_with_retry(payload, max_retries=3):
    """
    Sends a payload to Notion with automatic retries for timeouts and 5xx errors.
    """
    url = "https://api.notion.com/v1/pages"

    for attempt in range(max_retries):
        try:
            # Increased timeout to 30s to handle slow Notion responses
            response = httpx.post(url, headers=HEADERS, json=payload, timeout=30.0)

            # If successful, return immediately
            if response.status_code == 200:
                return response

            # If client error (400-499), do NOT retry
            if 400 <= response.status_code < 500:
                print(f"❌ Client Error ({response.status_code}): {response.text}")
                return response

            # If server error (500+), let it retry
            print(f"⚠️ Server Error ({response.status_code}), retrying in 2s...")

        except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.ConnectError) as e:
            print(f"⚠️ Network error ({type(e).__name__}) on attempt {attempt + 1}/{max_retries}, retrying in 2s...")
        except Exception as e:
            print(f"⚠️ Unexpected error: {e}, retrying...")

        # Wait before next attempt
        time.sleep(2)

    print("❌ Failed after max retries.")
    return None


def clean_notion_number(value_str):
    """
    Extracts integers and returns the average.
    Handles ranges ('10-15' -> 12.5) vs negatives ('-10' -> -10) correctly.
    """
    if not value_str or value_str == "-" or "nan" in str(value_str).lower():
        return 0
    # Regex uses (?<!\d) to ensure the hyphen is NOT preceded by a digit.
    numbers = re.findall(r'(?<!\d)-?\d+', str(value_str))
    if not numbers:
        return 0
    ints = list(map(int, numbers))
    return int(round(sum(ints) / len(ints)))


def get_elevation_relation_id(search_term):
    df_data = fetch_all_db_rows(notion_db_id['Weather Forecast Elevations'])
    if df_data is not None:
        match = df_data[df_data["Weather Source + Elevation"].str.contains(search_term, na=False)]
        if not match.empty:
            return match.iloc[0]["Page_ID"]
    print(f"⚠️ Warning: No Page ID found for '{search_term}'")
    return None


def parse_property(prop):
    ptype = prop.get("type")
    if ptype == "title":
        return prop["title"][0]["text"]["content"] if prop["title"] else ""
    elif ptype == "rich_text":
        return prop["rich_text"][0]["text"]["content"] if prop["rich_text"] else ""
    elif ptype == "number":
        return prop["number"]
    elif ptype == "select":
        return prop["select"]["name"] if prop["select"] else ""
    elif ptype == "date":
        return prop["date"]["start"] if prop["date"] else ""
    elif ptype == "checkbox":
        return prop["checkbox"]
    elif ptype == "formula":
        ftype = prop["formula"].get("type")
        return prop["formula"].get(ftype)
    return None


def fetch_all_db_rows(database_id, filter_payload=None):
    url = f"{cred.NOTION_ENDPOINT}/databases/{database_id}/query"

    if filter_payload:
        print("🔍 Applying Notion filter to query...")

    all_results = []
    has_more = True
    next_cursor = None

    try:
        while has_more:
            payload = {}
            if filter_payload:
                payload["filter"] = filter_payload
            if next_cursor:
                payload["start_cursor"] = next_cursor

            response = requests.post(url, json=payload, headers=cred.HEADERS)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)

            has_more = data.get("has_more")
            next_cursor = data.get("next_cursor")

        if not all_results:
            return None

        clean_rows = []
        for page in all_results:
            row_data = {}
            row_data["Page_ID"] = page["id"]
            props = page.get("properties")
            for col_name, col_data in props.items():
                row_data[col_name] = parse_property(col_data)
            clean_rows.append(row_data)

        df = pd.DataFrame(clean_rows)
        if not df.empty:
            cols = ["Page_ID"] + [c for c in df.columns if c != "Page_ID"]
            df = df[cols]
        return df

    except Exception as e:
        print(f"❌ Fetch Error: {e}")
        return None


def fetch_existing_forecasts(elevation):
    """
    Fetches ONLY rows matching the specific elevation AND 'Latest Report?' = True.
    """
    print(f"Checking Notion for existing {elevation} reports (Latest only)...")

    my_filter = {
        "and": [
            {
                "property": "Elevation + Update Time",
                "title": {
                    "starts_with": elevation
                }
            },
            {
                "property": "Latest Report?",
                "formula": {
                    "checkbox": {
                        "equals": True
                    }
                }
            }
        ]
    }

    return fetch_all_db_rows(notion_db_id['Weather Forecasts'], filter_payload=my_filter)


def parse_ski_date(date_str):
    try:
        date_obj = datetime.strptime(date_str, "%b %d")
    except ValueError:
        return None

    now = datetime.now()
    current_year = now.year
    current_month = now.month
    row_month = date_obj.month
    late_months = [10, 11, 12]

    if current_month in [11, 12]:
        year = current_year if row_month in late_months else (current_year + 1 if row_month < 6 else current_year)
    else:
        year = current_year - 1 if row_month in late_months else current_year

    return date_obj.replace(year=year)


# ==========================================
# 2. LOGIC FOR SNOW-FORECAST.COM (1480m & 2248m)
# ==========================================

def _process_snow_forecast_site(elevation):
    print(f"--- Processing {elevation} (Snow-Forecast.com) ---")

    url = whistler_data_url['Weather Forecast'][elevation]
    html = whistler_peak_scrape(url, '.forecast-table')
    soup = bs4.BeautifulSoup(html, "html.parser")
    get_time_until_update(soup, elevation)
    meta_dict = {}

    vancouver_tz = ZoneInfo("America/Vancouver")
    now_van = datetime.now(vancouver_tz)
    report_dt_obj = now_van
    found_date = False

    # A. Report Date
    intro_div = soup.find("div", class_="weather-intro")
    if intro_div:
        intro_text = intro_div.get_text(strip=True)
        match = re.search(r'Updated:\s*(\d+)\s*(min|hour)', intro_text)
        if match:
            amount = int(match.group(1))
            unit = match.group(2)
            delta = timedelta(minutes=amount) if "min" in unit else timedelta(hours=amount)
            report_dt_obj = now_van - delta
            found_date = True

    if not found_date:
        issued_node = soup.find(string=re.compile("Issued:"))
        if issued_node:
            container = issued_node.find_parent().parent
            if container:
                full_text = container.get_text(" ", strip=True).replace("\u2009", " ").replace(" ", " ")
                if "Issued:" in full_text:
                    try:
                        date_part = full_text.split("Issued:")[-1].replace("(local time)", "").strip()
                        parsed_dt = datetime.strptime(date_part, '%I %p %d %b %Y')
                        report_dt_obj = parsed_dt.replace(tzinfo=vancouver_tz)
                        found_date = True
                    except ValueError:
                        pass

    meta_dict["Report Date"] = report_dt_obj.strftime('%Y-%m-%d %H:%M:%S')

    # B. Next Update
    meta_dict["Next Update"] = None
    update_node = soup.find("span", class_="location-issued__update")
    if update_node:
        try:
            h_node = update_node.find("span", class_="hours")
            m_node = update_node.find("span", class_="minutes")
            hours = int(h_node.get_text(strip=True)) if h_node else 0
            minutes = int(m_node.get_text(strip=True)) if m_node else 0
            meta_dict["Next Update"] = now_van + timedelta(hours=hours, minutes=minutes)
        except ValueError:
            pass

    # C. Edition
    hour = report_dt_obj.hour
    if hour < 12:
        meta_dict["Forecast Edition"] = "AM"
    elif hour < 18:
        meta_dict["Forecast Edition"] = "PM"
    else:
        meta_dict["Forecast Edition"] = "Night"

    # D. Synopsis
    meta_dict["Synopsis"] = "Synopsis not found"
    summary_content = soup.find('div', class_='about-weather-summary__content')
    if summary_content:
        for p in summary_content.find_all('p', class_='about-weather-summary__text-block'):
            text = p.get_text(" ", strip=True)
            if "Weather (Next 3 days):" in text:
                prefix = "Whistler Blackcomb Weather (Next 3 days):"
                meta_dict["Synopsis"] = text.replace(prefix, "").split('):', 1)[-1].strip()
                break

    # 3. Extract Table Data
    table = soup.find('table', class_='forecast-table__table')
    if not table:
        print("❌ Error: Forecast table not found.")
        return

    def extract_row_data(row_name, css_selector=None, attribute=None):
        data_list = []
        row = table.find('tr', attrs={'data-row': row_name})
        if row:
            for cell in row.find_all('td'):
                colspan = int(cell.get('colspan', 1))
                value = "-"
                if attribute:
                    value = cell.get(attribute)
                elif css_selector:
                    item = cell.select_one(css_selector)
                    if item: value = item.get_text(strip=True)
                else:
                    value = cell.get_text(strip=True)
                for _ in range(colspan): data_list.append(value)
        return data_list

    dates_list = extract_row_data('days', attribute='data-date')
    times_list = extract_row_data('time')
    times_list = ["Night" if t == "night" else t for t in times_list]
    summary_list = extract_row_data('phrases', '.forecast-table__phrase')
    snow_list = extract_row_data('snow', '.snow-amount__value')
    max_temp_list = extract_row_data('temperature-max', '.temp-value')
    min_temp_list = extract_row_data('temperature-min', '.temp-value')
    fl_list = extract_row_data('freezing-level', '.level-value')

    rain_row = table.find('tr', attrs={'data-row': 'rain'})
    rain_list = extract_row_data('rain', '.rain-amount__value') if rain_row else ["-"] * len(dates_list)

    wind_row = table.find('tr', attrs={'data-row': 'wind'})
    wind_list, wind_dir_list = [], []
    if wind_row:
        for cell in wind_row.find_all('td'):
            colspan = int(cell.get('colspan', 1))
            val_node = cell.find(class_='wind-icon__val')
            speed = val_node.get_text(strip=True) if val_node else "0"
            tooltip_node = cell.find(class_='wind-icon__tooltip')
            direction = tooltip_node.get_text(strip=True) if tooltip_node else "Unknown"
            for _ in range(colspan):
                wind_list.append(speed)
                wind_dir_list.append(direction)
    else:
        wind_list, wind_dir_list = ["0"] * len(dates_list), ["Unknown"] * len(dates_list)

    meta_dict["Forecasts"] = {}
    for i in range(len(dates_list)):
        date_key = dates_list[i]
        if date_key not in meta_dict["Forecasts"]: meta_dict["Forecasts"][date_key] = []

        snow_val = clean_notion_number(snow_list[i])
        rain_val = clean_notion_number(rain_list[i])

        if snow_val > 0:
            precip_type, precip_amount = "Snow", snow_list[i]
        elif rain_val > 0:
            precip_type, precip_amount = "Rain", rain_list[i]
        else:
            precip_type, precip_amount = "None", "0cm"

        meta_dict["Forecasts"][date_key].append({
            "Period": times_list[i],
            "Summary": summary_list[i],
            "High": clean_notion_number(max_temp_list[i]),
            "Low": clean_notion_number(min_temp_list[i]),
            "Freezing Level": clean_notion_number(fl_list[i]),
            "Wind Speed": clean_notion_number(wind_list[i]),
            "Wind Direction": wind_dir_list[i],
            "Precipitation": precip_amount,
            "Precipitation Type": precip_type
        })

    # 5. Upload Logic
    existing_df = fetch_existing_forecasts(elevation)
    existing_combinations = set()

    if existing_df is not None and not existing_df.empty:
        existing_df['Report Date Norm'] = pd.to_datetime(existing_df['Report Date'], utc=True, errors='coerce')
        existing_df['Forecast Date Norm'] = pd.to_datetime(existing_df['Forecast Date']).dt.strftime('%Y-%m-%d')

        for _, row in existing_df.iterrows():
            if pd.notnull(row['Report Date Norm']):
                title_val = str(row.get('Elevation + Update Time', ''))
                existing_elev = title_val.split(' ')[0] if title_val else "Unknown"
                existing_combinations.add((row['Report Date Norm'], row['Forecast Date Norm'], existing_elev))

    current_report_dt_norm = pd.to_datetime(meta_dict["Report Date"]).tz_localize("America/Vancouver").tz_convert("UTC")
    report_date_iso = report_dt_obj.isoformat()
    report_time_str = report_dt_obj.strftime('%Y-%m-%dT%H:%M')
    next_update_str = meta_dict["Next Update"].isoformat() if meta_dict["Next Update"] else None

    forecast_rel_id = get_elevation_relation_id(elevation)

    for date_key, periods in meta_dict["Forecasts"].items():
        for period_data in periods:
            period_name = period_data.get("Period", "Unknown")

            if (current_report_dt_norm, date_key, elevation) in existing_combinations:
                print(f"⏭️ Skipping {date_key} ({period_name}) for {elevation}: Already exists.")
                continue

            page_title = f"{elevation} - {report_time_str}"

            props = {
                "Elevation + Update Time": {"title": [{"text": {"content": page_title}}]},
                "Report Date": {"date": {"start": report_date_iso}},
                "Forecast Date": {"date": {"start": date_key}},
                "Next Update": {"date": {"start": next_update_str}},
                "Time of Day": {"select": {"name": period_name}},
                "Forecast Type": {"rich_text": [{"text": {"content": meta_dict.get("Forecast Edition", "")}}]},
                "Synopsis": {"rich_text": [{"text": {"content": meta_dict.get("Synopsis", "")[:2000]}}]},
                "Daily Summary": {"rich_text": [{"text": {"content": period_data.get("Summary", "")}}]},
                "Precipitation Type": {"select": {"name": period_data.get("Precipitation Type", "None")}},
                "Ridge Wind Direction": {
                    "rich_text": [{"text": {"content": period_data.get("Wind Direction", "Unknown")}}]},
                "High": {"number": period_data.get("High")},
                "Low": {"number": period_data.get("Low")},
                "Freezing Level (m)": {"number": int(period_data.get("Freezing Level", 0))},
                "Precipitation Amount": {"number": clean_notion_number(period_data.get("Precipitation"))},
                "Ridge Wind Speed": {"number": clean_notion_number(period_data.get("Wind Speed"))}
            }

            if forecast_rel_id:
                props["Forecast Elevation"] = {"relation": [{"id": forecast_rel_id}]}

            # --- UPLOAD WITH RETRY ---
            payload = {"parent": {"database_id": notion_db_id['Weather Forecasts']}, "properties": props}
            response = send_to_notion_with_retry(payload)

            if response and response.status_code == 200:
                print(f"✅ Success: Uploaded {date_key} ({period_name})")
            else:
                print(f"❌ Error uploading {date_key} ({period_name})")


# ==========================================
# 3. LOGIC FOR 1800m (Alpine / RWDI)
# ==========================================

def _process_1800m(elevation):
    print(f"--- Processing {elevation} Forecast ---")

    html = whistler_peak_scrape(whistler_data_url['Weather Forecast'][elevation], '.alpine__container')
    soup = bs4.BeautifulSoup(html, "html.parser")
    get_time_until_update(soup, elevation)
    meta_dict = {}

    # 2. Extract Metadata
    time_containers = soup.find_all("div", class_='alpine__time-container')
    if len(time_containers) >= 2:
        first_block = time_containers[0]
        text_content = first_block.get_text(separator=" ", strip=True)
        if "Report date:" in text_content:
            date_part = text_content.split("Report date:")[1]
            if "Forecast type:" in date_part: date_part = date_part.split("Forecast type:")[0]
            clean_date = date_part.strip().rstrip('.').split(' ', 1)[1]
            meta_dict["Report Date"] = parse_whistler_date(clean_date)

        span = first_block.find("span")
        if span: meta_dict["Forecast Type"] = span.get_text(strip=True)

        second_block = time_containers[1]
        update_text = second_block.get_text(strip=True)
        if "Next update:" in update_text:
            next_str = update_text.split("Next update:")[1].strip().split(' ', 1)[1]
            meta_dict["Next Update"] = parse_whistler_date(next_str)

    summary_divs = soup.find_all("div", class_="summaryContent")
    if summary_divs:
        texts = [p.get_text(strip=True) for p in summary_divs[0].find_all("p") if
                 "synopsis-title" not in p.get("class", [])]
        meta_dict["Synopsis"] = "\n ".join(filter(None, texts))

    about_p = soup.find("p", class_="typeDetails")
    if about_p: meta_dict["About Forecast"] = about_p.get_text(strip=True)

    # 3. Prepare Report Date Object
    raw_report_obj = meta_dict.get("Report Date")
    if isinstance(raw_report_obj, str):
        try:
            report_date_obj = datetime.fromisoformat(raw_report_obj.replace("Z", "+00:00"))
        except ValueError:
            report_date_obj = datetime.now(ZoneInfo("UTC"))
    else:
        report_date_obj = raw_report_obj or datetime.now(ZoneInfo("UTC"))

    raw_report_str = report_date_obj.isoformat()

    # 4. Extract Cards
    meta_dict["Forecasts"] = {}
    forecast_cards = soup.find_all("div", class_="alpine__card")
    days_of_week = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    has_today = any(
        c.find("h3", class_="alpine__card-period").get_text(strip=True).lower() == "today" for c in forecast_cards)

    for card in forecast_cards:
        period_name = card.find("h3", class_="alpine__card-period").get_text(strip=True)
        period_lower = period_name.lower()

        if period_lower == "tonight" and has_today: continue

        if period_lower in ["tonight", "today"]:
            date_str = report_date_obj.strftime("%Y-%m-%d")
        else:
            current_day = report_date_obj.weekday()
            try:
                target_day = days_of_week.index(period_name)
                days_ahead = (target_day - current_day + 7) % 7
                if days_ahead == 0 and has_today: days_ahead = 7
                date_str = (report_date_obj + timedelta(days=days_ahead)).strftime("%Y-%m-%d")
            except ValueError:
                date_str = f"{period_name} (Unknown)"

        card_data = {
            "Day": period_name, "Precipitation Type": "None", "Precipitation Amount": "0cm",
            "Alpine High": None, "Alpine Low": None, "Ridge Wind Direction": "Unknown",
            "Ridge Wind Strength": "N/A", "Ridge Wind Speed": "0"
        }

        precip_node = card.find("p", class_=lambda x: x and "precip" in x)
        if precip_node:
            txt = precip_node.get_text(strip=True)
            if "Snow:" in txt:
                card_data["Precipitation Type"] = "Snow"
                # FIX: Replace "Trace" with "0" to ensure ranges like "Trace-2" parse as "0-2" (positive range)
                card_data["Precipitation Amount"] = txt.split("Snow:")[1].strip().replace("Trace", "0")
            elif "Rain:" in txt:
                card_data["Precipitation Type"] = "Rain"
                card_data["Precipitation Amount"] = txt.split("Rain:")[1].strip().replace("Trace", "0")

        fl_node = card.find(string=re.compile("Freezing Level"))
        if fl_node and fl_node.parent.find("span"):
            raw_fl = fl_node.parent.find("span").get_text(strip=True)
            nums = re.findall(r'\d+', raw_fl)
            if nums: card_data["Freezing Level (m)"] = nums[-1]

        temp_node = card.find("p", class_="alpine__card-temps")
        if temp_node:
            txt = temp_node.get_text(strip=True)
            l_match = re.search(r'Low\s*([-\d]+)', txt)
            if l_match: card_data["Alpine Low"] = int(l_match.group(1))
            h_match = re.search(r'High\s*([-\d]+)', txt)
            if h_match: card_data["Alpine High"] = int(h_match.group(1))

        wind_node = card.find("p", class_="alpine__card-wind")
        if wind_node:
            w_txt = wind_node.get_text(strip=True)
            dirs = ["north", "northeast", "east", "southeast", "south", "southwest", "west", "northwest"]
            card_data["Ridge Wind Direction"] = next((d for d in dirs if d in w_txt.lower()), "Unknown").capitalize()
            gust = re.search(r'gusting (?:to )?([\d-]+)', w_txt)
            if gust: card_data["Ridge Wind Strength"] = gust.group(1) + " km/h"
            nums = re.findall(r'\d+', w_txt)
            if "Light" in w_txt and not nums:
                card_data["Ridge Wind Speed"] = "Light"
            elif nums:
                card_data["Ridge Wind Speed"] = nums[0] + " km/h"

        summary_node = card.find("p", class_="alpine__card-summary")
        card_data["Daily Summary"] = summary_node.get_text(strip=True) if summary_node else "No summary"

        meta_dict["Forecasts"][date_str] = card_data

    # 5. Upload Preparation
    existing_df = fetch_existing_forecasts(elevation)
    existing_combinations = set()
    if existing_df is not None and not existing_df.empty:
        existing_df['Report Date Norm'] = pd.to_datetime(existing_df['Report Date'], utc=True, errors='coerce')
        existing_df['Forecast Date Norm'] = pd.to_datetime(existing_df['Forecast Date']).dt.strftime('%Y-%m-%d')
        for _, row in existing_df.iterrows():
            if pd.notnull(row['Report Date Norm']):
                title_val = str(row.get('Elevation + Update Time', ''))
                existing_elev = title_val.split(' ')[0] if title_val else "Unknown"
                existing_combinations.add((row['Report Date Norm'], row['Forecast Date Norm'], existing_elev))

    if isinstance(raw_report_str, str):
        current_report_dt_norm = pd.to_datetime(raw_report_str).tz_convert("UTC")
    else:
        current_report_dt_norm = pd.to_datetime(report_date_obj).tz_convert("UTC")

    forecast_rel_id = get_elevation_relation_id(elevation)

    next_update_raw = meta_dict.get("Next Update")
    next_update_str = None
    if next_update_raw:
        if isinstance(next_update_raw, str):
            next_update_str = next_update_raw
        elif hasattr(next_update_raw, 'isoformat'):
            next_update_str = next_update_raw.isoformat()

    vancouver_time = report_date_obj.astimezone(ZoneInfo("America/Vancouver")).strftime('%Y-%m-%dT%H:%M')

    # 6. Upload Loop
    for date_key, forecast in meta_dict["Forecasts"].items():
        if (current_report_dt_norm, date_key, elevation) in existing_combinations:
            print(f"⏭️ Skipping {date_key} for {elevation}: Already exists.")
            continue

        props = {
            "Elevation + Update Time": {"title": [{"text": {"content": f"{elevation} - {vancouver_time}"}}]},
            "Report Date": {"date": {"start": raw_report_str}},
            "Forecast Date": {"date": {"start": date_key}},
            "Next Update": {"date": {"start": next_update_str}},
            "Forecast Type": {"rich_text": [{"text": {"content": meta_dict.get("Forecast Type", "")}}]},
            "Synopsis": {"rich_text": [{"text": {"content": meta_dict.get("Synopsis", "")[:2000]}}]},
            "About Forecast": {"rich_text": [{"text": {"content": meta_dict.get("About Forecast", "")[:2000]}}]},
            "Daily Summary": {"rich_text": [{"text": {"content": forecast.get("Daily Summary", "")}}]},
            "Precipitation Type": {"select": {"name": forecast.get("Precipitation Type", "None")}},
            "Ridge Wind Direction": {
                "rich_text": [{"text": {"content": forecast.get("Ridge Wind Direction", "Unknown")}}]},
            "Ridge Wind Strength": {"rich_text": [{"text": {"content": forecast.get("Ridge Wind Strength", "N/A")}}]},
            # FIX: Added abs() here as a safety net
            "Precipitation Amount": {"number": abs(clean_notion_number(forecast.get("Precipitation Amount")))},
            "Alpine High": {"number": forecast.get("Alpine High")},
            "Alpine Low": {"number": forecast.get("Alpine Low")},
            "Freezing Level (m)": {"number": int(forecast.get("Freezing Level (m)", 0))},
            "Ridge Wind Speed": {"number": clean_notion_number(forecast.get("Ridge Wind Speed"))}
        }

        if forecast_rel_id:
            props["Forecast Elevation"] = {"relation": [{"id": forecast_rel_id}]}

        payload = {"parent": {"database_id": notion_db_id['Weather Forecasts']}, "properties": props}
        response = send_to_notion_with_retry(payload)

        if response and response.status_code == 200:
            print(f"✅ Success: Uploaded {date_key}")
        else:
            print(f"❌ Error uploading {date_key}")

# ==========================================
# 4. LOGIC FOR SNOWFALL HISTORY
# ==========================================

def update_snow_history(season_page_id=CURRENT_SEASON_PAGE_ID):
    print("--- Updating Snowfall History ---")

    html = whistler_peak_scrape(whistler_data_url['Snowfall History'], '.day-container')
    soup = bs4.BeautifulSoup(html, "html.parser")
    content = soup.find("div", id="content_history")

    if not content:
        print("❌ Error: Content history not found.")
        return

    days_str = content.text
    clean_lines = [line.strip() for line in days_str.split('\n') if line.strip()]

    try:
        start_index = clean_lines.index('Base') + 1
        data_lines = clean_lines[start_index:]
    except ValueError:
        data_lines = []

    rows = []
    for i in range(0, len(data_lines), 4):
        row = data_lines[i:i + 4]
        if len(row) == 4:
            rows.append(row)

    df = pd.DataFrame(rows, columns=["Date", "Snowfall", "Season", "Base"])

    cols_to_fix = ["Snowfall", "Season", "Base"]
    for col in cols_to_fix:
        df[col] = df[col].astype(str).str.replace('cm', '').astype(int)

    df['Date'] = df['Date'].apply(lambda x: parse_ski_date(x))
    df = df.dropna(subset=['Date'])

    existing_df = fetch_all_db_rows(SNOW_HISTORY_DB_ID)

    if existing_df is not None and not existing_df.empty:
        existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
        latest_notion_date = existing_df['Date'].max()
        rows_to_add = df[df['Date'] > latest_notion_date].copy()
    else:
        rows_to_add = df.copy()

    rows_to_add = rows_to_add.sort_values(by='Date')

    print(f"Found {len(rows_to_add)} new rows to add.")

    if not rows_to_add.empty:
        data_payload = rows_to_add.to_dict('records')

        for row in data_payload:
            date_iso = row["Date"].strftime("%Y-%m-%d")

            body = {
                "parent": {"database_id": SNOW_HISTORY_DB_ID},
                "properties": {
                    "Date": {
                        "title": [{"text": {"content": date_iso}}]
                    },
                    "Season": {
                        "relation": [{"id": season_page_id}]
                    },
                    "Snow (cm)": {"number": row["Snowfall"]},
                    "Season (cm)": {"number": row["Season"]},
                    "Base (cm)": {"number": row["Base"]},
                    "date": {"date": {"start": date_iso}}
                }
            }

            # --- UPLOAD WITH RETRY ---
            response = send_to_notion_with_retry(body)

            if response and response.status_code == 200:
                print(f"✅ Added History: {date_iso}")
            else:
                print(f"❌ Failed History: {date_iso}")
    else:
        print("No new history to upload.")

# ==========================================
# 5. SCHEDULE UPDATE FUNCTION
# ==========================================

def get_time_until_update(soup, elevation):
    """
    Scrapes the specific elevation page to find the 'Time Until Next Update'.
    Returns the time remaining in minutes (integer).
    Returns None if the data cannot be found.
    """
    print(f"--- Checking Update Timer for {elevation} ---")

    # 1. Setup & Scrape
    vancouver_tz = ZoneInfo("America/Vancouver")
    now_van = datetime.now(vancouver_tz)

    # -------------------------------------------
    # CASE A: 1480m & 2248m (Snow-Forecast.com)
    # Logic: These pages have a specific countdown timer in the HTML.
    # -------------------------------------------
    if elevation in ["1480m", "2248m"]:
        # Look for: <span class="location-issued__update">...<span class="hours">01</span>...</span>
        update_node = soup.find("span", class_="location-issued__update")

        if update_node:
            try:
                # Extract hours and minutes from the countdown spans
                h_node = update_node.find("span", class_="hours")
                m_node = update_node.find("span", class_="minutes")

                hours = int(h_node.get_text(strip=True)) if h_node else 0
                minutes = int(m_node.get_text(strip=True)) if m_node else 0

                total_minutes = (hours * 60) + minutes
                print(f"⏱️ {elevation}: Update in {total_minutes} mins ({hours}h {minutes}m)")
                return total_minutes
            except ValueError:
                print(f"⚠️ Could not parse countdown integers for {elevation}")
                return None
        else:
            print(f"⚠️ Countdown timer not found for {elevation}")
            return None

    # -------------------------------------------
    # CASE B: 1800m (Alpine / RWDI)
    # Logic: This page has a static "Next update:" timestamp string.
    # We must parse it and calculate the difference from NOW.
    # -------------------------------------------
    elif elevation == "1800m":
        # Look for the text "Next update:" in the time containers
        time_containers = soup.find_all("div", class_='alpine__time-container')
        next_update_str = None

        # Iterate containers to find the one with "Next update:"
        for container in time_containers:
            text = container.get_text(strip=True)
            if "Next update:" in text:
                # Text usually looks like: "Next update: Tuesday December 30, 2025 4pm"
                next_update_str = text.split("Next update:")[1].strip()
                break

        if next_update_str:
            try:
                # Clean up the string (remove potential extra periods or spaces)
                clean_str = next_update_str.rstrip(".")

                # Parse format: "Tuesday December 30, 2025 4pm" -> "%A %B %d, %Y %I%p"
                # Note: The site sometimes omits the comma after the day, handle both.
                try:
                    target_dt = datetime.strptime(clean_str, "%A %B %d, %Y %I%p")
                except ValueError:
                    # Retry without comma
                    target_dt = datetime.strptime(clean_str, "%A %B %d %Y %I%p")

                # Localize to Vancouver time
                target_dt = target_dt.replace(tzinfo=vancouver_tz)

                # Calculate difference
                diff = target_dt - now_van
                total_minutes = int(diff.total_seconds() / 60)

                # If time has passed (negative), return 0 or the negative value
                # (Negative implies the update is overdue)
                print(f"⏱️ {elevation}: Next update is {target_dt} ({total_minutes} mins from now)")
                return total_minutes

            except ValueError as ve:
                print(f"⚠️ Date parsing failed for '{next_update_str}': {ve}")
                return None
        else:
            print(f"⚠️ 'Next update' text not found for {elevation}")
            return None

    else:
        print(f"❌ Elevation '{elevation}' not supported.")
        return None

# ==========================================
# 6. MAIN DISPATCHER FUNCTION
# ==========================================

def get_forecast(elevation):
    """
    Main function to get forecast based on elevation.
    Inputs: "1480m", "1800m", "2248m"
    """
    if elevation in ["1480m", "2248m"]:
        _process_snow_forecast_site(elevation)

    elif elevation == "1800m":
        _process_1800m(elevation)

    else:
        print(f"❌ Error: Elevation '{elevation}' not recognized.")