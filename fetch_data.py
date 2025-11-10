import os
import requests
import json
from datetime import datetime, timezone

def fetch_and_save_daily_close_data():
    """
    Loads existing daily data, then fetches the *entire* daily market chart
    history from CoinGecko. It efficiently appends only the new,
    missing data points to the local file.
    """
    # The CoinGecko Pro API key is typically passed via environment variable
    api_key = os.getenv('COINGECKO_PRO_API_KEY')
    if not api_key:
        print("Error: COINGECKO_PRO_API_KEY secret is not set.")
        return
    else:
        print("Successfully loaded COINGECKO_PRO_API_KEY secret.")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_filename = os.path.join(script_dir, "price_data.json")

    # Step 1: Load existing data from the local file.
    existing_data = []
    existing_timestamps_sec = set()
    try:
        with open(data_filename, 'r') as f:
            existing_data = json.load(f)
            # Create a set of timestamps (in seconds) for fast lookups.
            existing_timestamps_sec = {entry[0] for entry in existing_data}
            if existing_data:
                last_date = datetime.fromtimestamp(existing_data[-1][0], tz=timezone.utc).strftime('%Y-%m-%d')
                print(f"Loaded {len(existing_data)} existing data points. Last entry is from: {last_date}")
            else:
                print("Local data file is empty.")
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"Warning: '{data_filename}' not found or is invalid. Will create a new one.")


    # Step 2: Construct the API URL using the market_chart endpoint.
    # We request 'days=max' to get all available daily data.
    # When 'days' is > 90, CoinGecko returns daily data points.
    api_url = (
        f"https://pro-api.coingecko.com/api/v3/coins/bittensor/market_chart"
        f"?vs_currency=usd&days=max"
        f"&x_cg_pro_api_key={api_key}"
    )
    
    print(f"Fetching all daily data for bittensor from CoinGecko...")

    try:
        response = requests.get(api_url, timeout=60)
        response.raise_for_status()
        
        # The market_chart endpoint returns a JSON with a 'prices' key.
        # This 'prices' key holds a list of [timestamp, price]
        new_data_raw = response.json().get('prices')

        if not isinstance(new_data_raw, list):
            print(f"Error: API returned an unexpected data format or 'prices' key is missing: {response.json()}")
            return
        
        # Step 3: Process the data and find new entries
        new_entries = []
        new_entries_count = 0
        
        for entry in new_data_raw:
            timestamp_ms = entry[0]
            close_price = entry[1]
            timestamp_sec = timestamp_ms // 1000 # Convert to seconds
            
            # Check if this timestamp is already in our set of existing data
            if timestamp_sec not in existing_timestamps_sec:
                new_entries.append([timestamp_sec, close_price])
                # Add to set to prevent duplicates from this same API call
                existing_timestamps_sec.add(timestamp_sec)
                new_entries_count += 1

        if not new_data_raw:
            print("No data returned from the API.")
            return

        # Step 4: Append, sort, and save if new data was found
        if new_entries_count > 0:
            print(f"Found {new_entries_count} new data point(s).")
            
            # Add the new entries to the existing data
            combined_data = existing_data + new_entries
            
            # Sort the combined data by timestamp to ensure it's always in order
            combined_data.sort(key=lambda x: x[0])
            
            # Overwrite the existing file with the new, combined data
            with open(data_filename, 'w') as f:
                json.dump(combined_data, f, indent=2)
                
            print(f"Successfully added new data. Total entries: {len(combined_data)}")
        else:
            print("No new data to add. The file is already up-to-date.")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error occurred while fetching from CoinGecko: {e}")
        print(f"Response body: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except (json.JSONDecodeError, KeyError, TypeError, IndexError) as e:
        print(f"Error processing data from the API response. Error: {e}")

if __name__ == "__main__":
    fetch_and_save_daily_close_data()
