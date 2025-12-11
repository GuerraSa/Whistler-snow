# pip install playwright
# also install playwright Chrome browser in terminal:
# playwright install chromium
import httpx
from playwright.sync_api import sync_playwright
import bs4
import pandas as pd
from datetime import datetime
from cred import NOTION_ENDPOINT, HEADERS
from data_urls import whistler_data_url
from utils.func import whistler_peak_scrape


html = whistler_peak_scrape(whistler_data_url['Snowfall History'], '.day-container')

soup = bs4.BeautifulSoup(html, "html.parser")
days = soup.find("div", id="content_history").text

days_str = days

# --- Parsing Logic (From previous step) ---
clean_lines = [line.strip() for line in days_str.split('\n') if line.strip()]

# Find start index
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

# --- NEW STEP 1: Remove 'cm' and convert to numbers ---
# We treat the columns as strings (.str), replace 'cm', and cast to integers
cols_to_fix = ["Snowfall", "Season", "Base"]
for col in cols_to_fix:
    df[col] = df[col].astype(str).str.replace('cm', '').astype(int)


# --- NEW STEP 2: Convert Date to datetime with custom logic ---
def parse_ski_date(date_str):
    # 1. Parse the Month and Day (e.g. "Dec 4")
    # This creates a date with the default year (usually 1900)
    date_obj = datetime.strptime(date_str, "%b %d")

    # 2. Get current timeframe info
    now = datetime.now()
    current_year = now.year
    current_month = now.month

    row_month = date_obj.month

    # We define the "Late Year" months.
    # Note: Your data includes Oct, so I added 10 to ensure Oct dates
    # are treated the same as Nov/Dec for season logic.
    late_months = [10, 11, 12]

    # 3. Apply User Logic
    # Case A: Current is Nov/Dec AND Table is Nov/Dec -> Current Year
    if current_month in [11, 12]:
        if row_month in late_months:
            year = current_year
        else:
            # Implicit else: If table is Jan-May, it's likely the same year
            # (or next year if forecasting, but usually history is same year)
            year = current_year

    # Case B: Current is NOT Nov/Dec (Jan-Oct) AND Table IS Nov/Dec -> Previous Year
    else:
        if row_month in late_months:
            year = current_year - 1
        else:
            # Implicit else: Both are Jan-Oct -> Current Year
            year = current_year

    # Return the new date with the correct year
    return date_obj.replace(year=year)


# Apply the function to the Date column
df['Date'] = df['Date'].apply(parse_ski_date)


# 1. SETUP: Mocking your Notion Table (Replace this with your actual import)
# Let's assume Notion has data up to Nov 28th, but our scraped 'df' goes up to Dec 4th.

table_block_id = '145e268796a88032969be9ff33906b3e'
DATABASE_ID = '5c44378e-068f-4074-ab7b-6f3b31fb7633'
SEASON_PAGE_ID = "2b8e268796a88004b66bc101e8f04340" # 25/26 Season

rows = []
has_more = True
next_cursor = None

print("Fetching data from Notion...")

while has_more:
    # 1. Prepare the payload with the cursor if we have one
    payload = {}
    if next_cursor:
        payload["start_cursor"] = next_cursor

    # 2. Send the request (Include json=payload)
    response = httpx.post(
        f'{NOTION_ENDPOINT}/databases/{table_block_id}/query',
        headers=HEADERS,
        json=payload
    )

    data = response.json()

    # 3. Process the current batch of results
    for result in data['results']:
        row = {}
        for key, value in result['properties'].items():
            # Your existing extraction logic
            if value['type'] == 'title':
                # Safety check in case title is empty
                if value['title']:
                    row[key] = value['title'][0]['text']['content']
                else:
                    row[key] = ""
            elif value['type'] == 'number':
                row[key] = value['number']
            elif value['type'] == 'rich_text':
                # Safety check in case text is empty
                if value['rich_text']:
                    row[key] = value['rich_text'][0]['text']['content']
                else:
                    row[key] = ""
            elif value['type'] == 'relation':
                # Extract relation IDs if needed
                row[key] = [r['id'] for r in value['relation']]

        rows.append(row)

    # 4. Update pagination variables for the next loop
    has_more = data.get('has_more', False)
    next_cursor = data.get('next_cursor')

    print(f"Fetched {len(data['results'])} rows. More to come? {has_more}")

# 5. Create DataFrame
notion_df = pd.DataFrame(rows)
print(f"Total rows retrieved: {len(notion_df)}")
print(notion_df.head())

# ---------------------------------------------------------

# 2. Ensure both Date columns are actual datetime objects
# Your 'df' is already converted from the previous step.
# We must ensure notion_df is too, or the comparison (>) will fail.
notion_df['Date'] = pd.to_datetime(notion_df['Date'])

# 3. Find the latest date currently in Notion
if not notion_df.empty:
    latest_notion_date = notion_df['Date'].max()
    print(f"Latest date in Notion: {latest_notion_date}")

    # 4. Filter the scraped 'df' for dates strictly greater than the Notion date
    rows_to_add = df[df['Date'] > latest_notion_date].copy()
    rows_to_add = rows_to_add[rows_to_add['Date'] < datetime.today().strftime('%Y-%m-%d')]
else:
    # If Notion is empty, everything is new
    rows_to_add = df.copy()

# 5. Sort them so they are in chronological order (optional, but usually better for uploading)
rows_to_add = rows_to_add.sort_values(by='Date')

print(f"\nFound {len(rows_to_add)} new rows to add:")
print(rows_to_add)

# 6. (Optional) Convert to a list of dictionaries
# This format is usually what you need when iterating to send API requests
data_payload = rows_to_add.to_dict('records')


# 7. --- SENDING TO NOTION ---
if 'data_payload' in locals() and data_payload:
    print(f"Uploading {len(data_payload)} new rows to Notion...")

    for row in data_payload:
        date_iso = row["Date"].strftime("%Y-%m-%d")

        body = {
            "parent": {"database_id": table_block_id},
            "properties": {
                # --- THE FIX IS HERE ---
                # Since 'Date' is your Title column, we must send it as a Title object (text)
                "Date": {
                    "title": [
                        {
                            "text": {"content": date_iso}
                        }
                    ]
                },
                # RELATION Column ("Season")
                # We link this row to the specific Page ID defined above
                "Season": {
                    "relation": [
                        {"id": SEASON_PAGE_ID}
                    ]
                },
                # The other columns remain as numbers
                "Snow (cm)": {"number": row["Snowfall"]},
                "Season (cm)": {"number": row["Season"]},
                "Base (cm)": {"number": row["Base"]},
                "date": {"date": {"start": date_iso}}
            }
        }

        try:
            response = httpx.post("https://api.notion.com/v1/pages", headers=HEADERS, json=body)
            if response.status_code == 200:
                print(f"✅ Added: {date_iso}")
            else:
                print(f"❌ Failed: {date_iso} - {response.text}")
        except Exception as e:
            print(f"⚠️ Error sending {date_iso}: {e}")

    print("Upload complete.")
else:
    print("⚠️ data_payload is missing.")