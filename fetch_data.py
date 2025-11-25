import os
import requests
import json
from datetime import datetime, timezone

def fetch_and_save_data():
    """
    Loads existing historical data, then fetches any new data from CoinGecko
    since the last entry and appends it.
    
    SAFETY: This script ensures no existing data is deleted. It aborts if the
    local file is corrupt rather than overwriting it.
    """
    api_key = os.getenv('COINGECKO')
    if not api_key:
        print("Error: COINGECKO secret is not set in the GitHub repository.")
        return
    else:
        print("Successfully loaded COINGECKO secret.")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_filename = os.path.join(script_dir, "price_data.json")

    existing_data = []
    last_timestamp_sec = 0

    # Step 1: Load the existing pre-filled data.
    # CRITICAL: If the file exists but is corrupt, we ABORT to avoid overwriting/deleting it.
    if os.path.exists(data_filename):
        try:
            with open(data_filename, 'r') as f:
                content = f.read()
                if not content.strip():
                    # Empty file is treated as no data
                    existing_data = []
                else:
                    existing_data = json.loads(content)
                    
            if existing_data:
                last_timestamp_sec = existing_data[-1][0]
                print(f"Last entry in local data is from: {datetime.fromtimestamp(last_timestamp_sec, tz=timezone.utc).strftime('%Y-%m-%d')}")
            else:
                print("Local data file is empty.")
                
        except json.JSONDecodeError:
            print(f"CRITICAL ERROR: '{data_filename}' exists but contains invalid JSON.")
            print("Aborting operation to prevent overwriting/deleting existing corrupted data.")
            return
        except Exception as e:
            print(f"Error reading file: {e}")
            return
    else:
        print(f"'{data_filename}' not found. A new file will be created.")

    # Capture the initial count to ensure we never shrink the dataset
    initial_count = len(existing_data)

    # Step 2: Calculate the number of days to fetch from CoinGecko.
    if last_timestamp_sec > 0:
        # Calculate the number of days since the last update and add a 2-day buffer.
        days_to_fetch = (datetime.now(timezone.utc) - datetime.fromtimestamp(last_timestamp_sec, tz=timezone.utc)).days + 2
        # Ensure we don't exceed CoinGecko's 365-day limit for free/demo keys.
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
        
        # Create a set of existing dates (YYYY-MM-DD) for fast lookups to avoid duplicates.
        existing_dates = set()
        for entry in existing_data:
            ts = entry[0]
            # Convert timestamp to UTC date string
            date_str = datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%Y-%m-%d')
            existing_dates.add(date_str)

        # Get 'today' in UTC to filter out unfinished daily candles
        today_utc_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')

        for entry in new_data_raw:
            timestamp_ms = entry[0]
            timestamp_sec = timestamp_ms // 1000
            price = entry[1]
            
            # Determine the date of this data point
            entry_date_str = datetime.fromtimestamp(timestamp_sec, tz=timezone.utc).strftime('%Y-%m-%d')

            # FILTER 1: Skip if this data point is for "Today" (unfinished day)
            if entry_date_str == today_utc_str:
                continue

            # FILTER 2: Skip if we already have data for this Date
            if entry_date_str in existing_dates:
                continue

            # If passed filters, append and mark date as seen
            existing_data.append([timestamp_sec, price])
            existing_dates.add(entry_date_str)
            new_entries_count += 1
        
        # Step 4: Write back to file ONLY if we have new data and the integrity check passes
        if new_entries_count > 0:
            # INTEGRITY CHECK: Ensure we haven't lost data
            if len(existing_data) < initial_count:
                print(f"CRITICAL ERROR: Data integrity check failed. New size ({len(existing_data)}) < Old size ({initial_count}). Aborting save.")
                return

            # Sort the combined data by timestamp to ensure it's always in chronological order.
            existing_data.sort(key=lambda x: x[0])
            
            with open(data_filename, 'w') as f:
                json.dump(existing_data, f, indent=2)
            print(f"Successfully added {new_entries_count} new data point(s). Total entries: {len(existing_data)}")
        else:
            print("No new closing price data found to append. The file is already up-to-date.")

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error occurred while fetching from CoinGecko: {e}")
        if hasattr(e, 'response') and e.response is not None:
             print(f"Response body: {e.response.text}")
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching data: {e}")
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing data from the API response. Error: {e}")

if __name__ == "__main__":
    fetch_and_save_data()
