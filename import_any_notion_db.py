import requests
import pandas as pd
import cred  # Imports your credentials file
import utils.notion_api as notion_api

# --- MAIN EXECUTION ---
if __name__ == "__main__":
    target_db_id = input("Enter your Notion Database ID: ").strip()

    print("\n--- 1. Getting Metadata ---")
    df_meta = notion_api.fetch_db_metadata(target_db_id)
    if df_meta is not None:
        print(df_meta.to_string(index=False))

    print("\n--- 2. Getting Data Rows ---")
    df_data = notion_api.fetch_all_db_rows(target_db_id)
    if df_data is not None and not df_data.empty:
        print(df_data.head().to_string())
        print(f"\nTotal rows fetched: {len(df_data)}")
    else:
        print("No data found.")