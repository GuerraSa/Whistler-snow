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

def fetch_all_db_rows(database_id):
    """
    Fetches ALL rows from a Notion database, handling pagination and formulas.
    """
    url = f"{cred.NOTION_ENDPOINT}/databases/{database_id}/query"

    print(f"Connecting to Notion Database: {database_id}...")

    all_results = []
    has_more = True
    next_cursor = None

    try:
        while has_more:
            payload = {}
            if next_cursor:
                payload["start_cursor"] = next_cursor

            response = requests.post(url, json=payload, headers=cred.HEADERS)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", [])
            all_results.extend(results)

            has_more = data.get("has_more")
            next_cursor = data.get("next_cursor")

            print(f"Fetched {len(results)} rows... (Total so far: {len(all_results)})")

        if not all_results:
            print("❌ Database is empty.")
            return None

        print(f"✅ Finished fetching {len(all_results)} total rows. Parsing data...")

        clean_rows = []
        for page in all_results:
            row_data = {}

            # --- ADDING PAGE ID HERE ---
            row_data["Page_ID"] = page["id"]

            props = page.get("properties")
            for col_name, col_data in props.items():
                row_data[col_name] = parse_property(col_data)

            clean_rows.append(row_data)

        # Move Page_ID to be the first column for better readability
        df = pd.DataFrame(clean_rows)

        # This reorders columns to put Page_ID first if it isn't already
        cols = ["Page_ID"] + [c for c in df.columns if c != "Page_ID"]
        df = df[cols]

        return df

    except requests.exceptions.HTTPError as err:
        print(f"❌ HTTP Error: {err}")
        return None
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None

def parse_property(prop):
    """
    Extracts value based on Notion property type.
    """
    prop_type = prop.get("type")

    if prop_type == "formula":
        formula_obj = prop.get("formula")
        result_type = formula_obj.get("type")
        return formula_obj.get(result_type)

    elif prop_type in ["title", "rich_text"]:
        content_list = prop.get(prop_type, [])
        return content_list[0].get("plain_text", "") if content_list else ""

    elif prop_type == "select":
        val = prop.get("select")
        return val.get("name") if val else None

    elif prop_type == "multi_select":
        return ", ".join([item.get("name") for item in prop.get("multi_select", [])])

    elif prop_type == "date":
        date_obj = prop.get("date")
        if date_obj:
            start = date_obj.get("start")
            end = date_obj.get("end")
            return f"{start} -> {end}" if end else start
        return None

    elif prop_type in ["checkbox", "url", "email", "phone_number", "number"]:
        return prop.get(prop_type)

    elif prop_type == "relation":
        return [r.get("id") for r in prop.get("relation", [])]

    elif prop_type == "rollup":
        rollup_obj = prop.get("rollup")
        result_type = rollup_obj.get("type")
        if result_type == "array":
            return str([x for x in rollup_obj.get("array")])
        return rollup_obj.get(result_type)

    return None
