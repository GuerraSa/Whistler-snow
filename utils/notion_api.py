import requests
import pandas as pd
import cred

def fetch_db_metadata(database_id):
    """
    Fetches the database SCHEMA (Column Names and Types).
    Uses the 'Retrieve a database' endpoint (GET).
    """
    url = f"{cred.NOTION_ENDPOINT}/databases/{database_id}"

    print(f"Fetching Metadata for: {database_id}...")

    try:
        response = requests.get(url, headers=cred.HEADERS)
        response.raise_for_status()

        data = response.json()
        properties = data.get("properties", {})

        # Parse the schema into a list of dictionaries
        schema_list = []
        for name, details in properties.items():
            schema_info = {
                "Property_Name": name,
                "Property_Type": details.get("type"),
                "Property_ID": details.get("id")  # Useful for debugging or advanced updates
            }
            schema_list.append(schema_info)

        return pd.DataFrame(schema_list)

    except Exception as e:
        print(f"❌ Error fetching metadata: {e}")
        return None

