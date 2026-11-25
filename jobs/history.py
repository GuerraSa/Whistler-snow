import bs4
import pandas as pd
from datetime import datetime
from internal_tools import NotionClient
import config
from cred import NOTION_TOKEN
from core import scraper, utils


def parse_ski_date(date_str):
    try:
        return utils.parse_recent_date(date_str, "%b %d", datetime.now(utils.WHISTLER_TZ))
    except ValueError:
        return None


def update_snow_history():
    print("--- ❄️ Updating Snowfall History ---")

    # 1. Scrape Website
    html = scraper.scrape_dynamic_content(config.URLS["Snowfall History"], '.day-container')
    if not html: return

    soup = bs4.BeautifulSoup(html, "html.parser")
    content = soup.find("div", id="content_history")
    if not content: return

    clean_lines = [line.strip() for line in content.text.split('\n') if line.strip()]
    try:
        idx = clean_lines.index('Base') + 1
        data_lines = clean_lines[idx:]
    except:
        return

    rows = []
    for i in range(0, len(data_lines), 4):
        row = data_lines[i:i + 4]
        if len(row) == 4: rows.append(row)

    # 2. Process Scraped Data
    df = pd.DataFrame(rows, columns=["Date", "Snowfall", "Season", "Base"])

    # Clean numbers
    for col in ["Snowfall", "Season", "Base"]:
        df[col] = pd.to_numeric(df[col].astype(str).str.replace('cm', ''), errors='coerce')
    df = df.dropna(subset=["Snowfall", "Base"])  # pre-season days show a bare 'cm'

    df['DateObj'] = df['Date'].apply(lambda x: parse_ski_date(x))
    df = df.dropna(subset=['DateObj'])

    # 3. Fetch Existing Notion Data (As DataFrame)
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS["Snowfall History"])

    print("   📊 Querying Notion Data Source...")
    existing_df = client.query_datasource(return_df=True, strict=True)  # a failed read must stop, not look empty

    existing_dates = set()

    # Extract existing dates directly from the DataFrame column
    if existing_df is not None and not existing_df.empty:
        # Check if the "Date" column exists (Title property)
        if "Date" in existing_df.columns:
            # Convert to string to ensure matching works ('2025-01-27')
            existing_dates = set(existing_df["Date"].astype(str))

    print(f"   📊 Found {len(existing_dates)} existing records in Notion.")

    # 4. Upload New Rows
    count = 0
    P = client.Props

    for _, row in df.iterrows():
        date_iso = row["DateObj"].strftime("%Y-%m-%d")

        # EXACT MATCH CHECK
        if date_iso in existing_dates:
            continue

        props = {
            "Date": P.title(date_iso),
            "Season": P.relation([config.SEASON_PAGE_ID]),
            "Snow (cm)": P.number(row["Snowfall"]),
            "Season (cm)": P.number(row["Season"]),
            "Base (cm)": P.number(row["Base"]),
            "date": P.date(date_iso)
        }
        client.add_row(properties=props)
        count += 1
        print(f"      + Added: {date_iso}")

    print(f"✅ Sync Complete. Added {count} new records.")