import bs4
import pandas as pd
from internal_tools import NotionClient
import config
from cred import NOTION_TOKEN
from core import scraper


def sync_lift_info():
    print("--- 🚠 Syncing Lift Information ---")

    # 1. Scrape Lift Data
    html = scraper.scrape_dynamic_content(config.URLS["Lifts"], ".row")
    if not html:
        return

    soup = bs4.BeautifulSoup(html, "html.parser")
    lift_data = []

    for row in soup.find_all('div', class_='row'):
        cells = row.find_all('div', class_='cell')
        if len(cells) >= 3:
            name = cells[0].get_text(strip=True)
            # Basic validation
            if not name or name.lower() == "lift name": continue

            lift_data.append({
                "Lift Name": name,
                "Bottom Elevation (m)": int(cells[1].get_text(strip=True).replace(',', '')),
                "Top Elevation (m)": int(cells[2].get_text(strip=True).replace(',', ''))
            })

    local_df = pd.DataFrame(lift_data)

    # 2. Fetch Existing Notion Data
    client = NotionClient(token=NOTION_TOKEN, database_id=config.DB_IDS["Lifts"])

    # Handle response (list or dict)
    response = client.query_database()
    results = response.get("results", []) if isinstance(response, dict) else response

    existing_names = set()

    # ROBUST MATCHING: Find the Title property dynamically
    for page in results:
        props = page.get("properties", {})
        title_content = ""

        # Loop through properties to find the one with type 'title'
        # This fixes issues if your column is named "Name" instead of "Lift Name"
        for prop_name, prop_data in props.items():
            if prop_data.get("type") == "title":
                title_list = prop_data.get("title", [])
                if title_list:
                    title_content = title_list[0]["text"]["content"]
                break

        if title_content:
            existing_names.add(title_content.strip().lower())

    print(f"   📊 Found {len(existing_names)} existing lifts in Notion.")

    # 3. Filter New (Case-Insensitive Check)
    # We compare the scraped name (lowercase) against the set of existing names (lowercase)
    rows_to_add = local_df[~local_df["Lift Name"].str.strip().str.lower().isin(existing_names)]

    if rows_to_add.empty:
        print("   ✅ No new lifts found. Database is up to date.")
        return

    print(f"   🚀 Found {len(rows_to_add)} NEW lifts to add.")

    # 4. Upload
    P = client.Props
    for _, row in rows_to_add.iterrows():
        props = {
            "Lift Name": P.title(row["Lift Name"]),
            "Bottom Elevation (m)": P.number(row["Bottom Elevation (m)"]),
            "Top Elevation (m)": P.number(row["Top Elevation (m)"])
        }
        client.add_row(properties=props)
        print(f"      + Added: {row['Lift Name']}")

    print("✅ Lift Sync Complete.")