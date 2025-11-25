
import os
import requests
import json
from datetime import datetime, timezone

def fetch_and_save_data():
    """
    Loads existing historical data, then fetches any new data from CoinGecko
    since the last entry and appends it.
    """
    api_key = os.getenv('COINGECKO_API_KEY')
    if not api_key:
        print("Error: COINGECKO_API_KEY secret is not set in the GitHub repository.")
        return
    else:
        print("Successfully loaded COINGECKO_API_KEY secret.")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_filename = os.path.join(script_dir, "price_data.json")

    existing_data = []
    last_timestamp_sec = 0

    # Step 1: Load the existing pre-filled data and find the last timestamp.
    try:
        with open(data_filename, 'r') as f:
            existing_data = json.load(f)
            if existing_data:
                # Get the timestamp from the very last entry in the file.
                last_timestamp_sec = existing_data[-1][0]
                print(f"Last entry in local data is from: {datetime.fromtimestamp(last_timestamp_sec, tz=timezone.utc).strftime('%Y-%m-%d')}")
            else:
                print("Local data file is empty.")
    except (FileNotFoundError, json.JSONDecodeError):
        print("Warning: 'price_data.json' not found or is invalid. A new file will be created with the last 365 days of data.")
        # If the file doesn't exist, we will fetch the maximum allowed data.
        last_timestamp_sec = 0


    # Step 2: Calculate the number of days to fetch from CoinGecko.
    if last_timestamp_sec > 0:
        # Calculate the number of days since the last update and add a 2-day buffer.
        days_to_fetch = (datetime.now(timezone.utc) - datetime.fromtimestamp(last_timestamp_sec, tz=timezone.utc)).days + 2
        # Ensure we don't exceed CoinGecko's 365-day limit for free keys.
        if days_to_fetch > 365:
            days_to_fetch = 365
        print(f"Fetching the last {days_to_fetch} day(s) of new data from CoinGecko...")
    else:
        # If there's no existing data, fetch the maximum allowed (365 days).
        days_to_fetch = 365
        print("No existing data found. Fetching the last 365 days from CoinGecko...")

    
    api_url = f"https://api.coingecko.com/api/v3/coins/bittensor/market_chart?vs_currency=usd&days={days_to_fetch}&interval=daily&x_cg_demo_api_key={api_key}"
    
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        
        new_data_raw = response.json().get('prices')

        if not isinstance(new_data_raw, list):
            print(f"Error: API returned an unexpected data format: {new_data_raw}")
            return
        
        # Step 3: Filter out duplicates and append only the new entries.
        new_entries_count = 0
        
        # Create a set of existing timestamps for fast lookups to avoid duplicates.
        existing_timestamps = {entry[0] for entry in existing_data}

        for entry in new_data_raw:
            timestamp_ms = entry[0]
            timestamp_sec = timestamp_ms // 1000
            price = entry[1]
            
            # CRITICAL: Only add data that is truly new.
            if timestamp_sec > last_timestamp_sec and timestamp_sec not in existing_timestamps:
                existing_data.append([timestamp_sec, price])
                new_entries_count += 1
        
        if new_entries_count > 0:
            # Sort the combined data by timestamp to ensure it's always in chronological order.
            existing_data.sort(key=lambda x: x[0])
            
            with open(data_filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
            print(f"Successfully added {new_entries_count} new data point(s). Total entries: {len(existing_data)}")
        else:
            print("No new data to add. The file is already up-to-date.")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error occurred while fetching from CoinGecko: {e}")
        print(f"Response body: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing data from the API response. Error: {e}")

if __name__ == "__main__":
    fetch_and_save_data()
