import requests
import pandas as pd
import cred  # Imports your credentials file


def fetch_all_db_rows(database_id):
    """
    Fetches ALL rows from a Notion database, handling pagination automatically.
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
            # row_data["Page_ID"] = page["id"] # Optional: Keep ID if needed

            props = page.get("properties")
            for col_name, col_data in props.items():
                row_data[col_name] = parse_property(col_data)

            clean_rows.append(row_data)

        return pd.DataFrame(clean_rows)

    except requests.exceptions.HTTPError as err:
        print(f"❌ HTTP Error: {err}")
        return None
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return None


def parse_property(prop):
    """
    Extracts value based on Notion property type, including Formulas.
    """
    prop_type = prop.get("type")

    # --- FORMULA HANDLING ---
    if prop_type == "formula":
        formula_obj = prop.get("formula")
        result_type = formula_obj.get("type")
        # Return the value based on the formula's result type (string/number/boolean/date)
        return formula_obj.get(result_type)

    # --- OTHER PROPERTIES ---
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

    # Simple types: Checkbox, URL, Email, Phone, Number
    elif prop_type in ["checkbox", "url", "email", "phone_number", "number"]:
        return prop.get(prop_type)

    elif prop_type == "relation":
        return [r.get("id") for r in prop.get("relation", [])]

    # Rollups (Similar complexity to formulas)
    elif prop_type == "rollup":
        rollup_obj = prop.get("rollup")
        result_type = rollup_obj.get("type")  # array, number, or date
        if result_type == "array":
            # Arrays are usually lists of other types, simplify to string for now
            return str([x for x in rollup_obj.get("array")])
        return rollup_obj.get(result_type)

    return None


if __name__ == "__main__":
    target_db_id = input("Enter your Notion Database ID: ").strip()

    df = fetch_all_db_rows(target_db_id)

    if df is not None:
        print("\n--- Final Data ---")
        print(df.to_string())

        # Optional: Save to CSV to see full data easier
        # df.to_csv("notion_data.csv", index=False)