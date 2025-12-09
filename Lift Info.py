import httpx
from playwright.sync_api import sync_playwright
import bs4
import pandas as pd
from datetime import datetime
from cred import NOTION_ENDPOINT, HEADERS
from notion_dbs import notion_db_id
from data_urls import whistler_data_url

# Start Playwright
with sync_playwright() as p:
    # Launch a headless browser
    browser = p.chromium.launch(headless=True)
    # Open a new browser tab
    page = browser.new_page()
    page.set_viewport_size({"width": 1920, "height": 1080})  # Set window size

    # scrape the reviews page
    page.goto(whistler_data_url['Lifts'])
    # wait up to 5 seconds for element to appear
    page.wait_for_selector('.row', timeout=5000)

    # Retrieve the HTML content
    html = page.content()
    print(html)

    # Close the browser
    browser.close()


soup = bs4.BeautifulSoup(html, "html.parser")

# 1. Initialize a list to hold the extracted data
lift_data = []

# 2. Find all div elements with the class "row"
rows = soup.find_all('div', class_='row')

# 3. Loop through each row to extract cell data
for row in rows:
    # Find all 'cell' divs inside the current row
    cells = row.find_all('div', class_='cell')

    # Check to ensure the row has enough cells (we need at least 3)
    if len(cells) >= 3:
        # Extract text and strip whitespace
        lift_name = cells[0].get_text(strip=True)
        bottom_m = cells[1].get_text(strip=True)
        top_m = cells[2].get_text(strip=True)

        # Append to our list
        lift_data.append({
            "Lift Name": lift_name,
            "Bottom Elevation (m)": bottom_m,
            "Top Elevation (m)": top_m
        })

# 4. Convert the list of dictionaries to a DataFrame
df = pd.DataFrame(lift_data)

# --- Optional: Data Cleaning ---
# The numbers currently look like "2,134" (strings).
# Remove commas and convert to integers for analysis.
df['Bottom Elevation (m)'] = df['Bottom Elevation (m)'].str.replace(',', '').astype(int)
df['Top Elevation (m)'] = df['Top Elevation (m)'].str.replace(',', '').astype(int)

## Send to Notion
table_block_id = notion_db_id['Lifts']


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
# 1. Identify which Lift Names are already in Notion
# This creates a boolean mask (True/False) for rows that match
existing_lifts = df['Lift Name'].isin(notion_df['Lift Name'])

# 2. Filter the new dataframe using the tilde (~) to select rows that are NOT in the existing list
rows_to_add = df[~existing_lifts]


print(f"\nFound {len(rows_to_add)} new rows to add:")
print(rows_to_add)

# 6. (Optional) Convert to a list of dictionaries
# This format is usually what you need when iterating to send API requests
data_payload = rows_to_add.to_dict('records')


# 7. --- SENDING TO NOTION ---
if 'data_payload' in locals() and data_payload:
    print(f"Uploading {len(data_payload)} new rows to Notion...")

    for row in data_payload:

        body = {
            "parent": {"database_id": table_block_id},
            "properties": {
                # --- THE FIX IS HERE ---
                # Since 'Date' is your Title column, we must send it as a Title object (text)
                "Lift Name": {
                    "title": [
                        {
                            "text": {"content": row["Lift Name"]}
                        }
                    ]
                },
                # The other columns remain as numbers
                "Bottom Elevation (m)": {"number": row["Bottom Elevation (m)"]},
                "Top Elevation (m)": {"number": row["Top Elevation (m)"]}
            }
        }

        try:
            response = httpx.post("https://api.notion.com/v1/pages", headers=HEADERS, json=body)
            if response.status_code == 200:
                print(f"✅ Added: {row["Lift Name"]}")
            else:
                print(f"❌ Failed: {row["Lift Name"]} - {response.text}")
        except Exception as e:
            print(f"⚠️ Error sending {row["Lift Name"]}: {e}")

    print("Upload complete.")
else:
    print("⚠️ data_payload is missing.")