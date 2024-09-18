import os
import pandas as pd
import requests
import time
from datetime import datetime, timedelta, timezone
import kaggle  

# Function to fetch data from Bitstamp API
def fetch_bitstamp_data(currency_pair, start_timestamp, end_timestamp, step=60, limit=1000):
    url = f'https://www.bitstamp.net/api/v2/ohlc/{currency_pair}/'
    params = {
        'step': step,  # 60 seconds (1-minute interval)
        'start': start_timestamp,
        'end': end_timestamp,
        'limit': limit  # Fetch 1000 data points max per request
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json().get('data', {}).get('ohlc', [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

# Download the latest dataset from Kaggle
def download_latest_dataset(dataset_slug):
    # Use Kaggle Python API to download the dataset directly to memory
    kaggle.api.dataset_download_files(dataset_slug, path='.', unzip=True)

# Check for missing days and return a list of dates to scrape
def check_missing_days(existing_data_filename):
    df = pd.read_csv(existing_data_filename)

    # Assuming your 'Timestamp' column is in Unix time
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')

    # Find the last available date in the dataset
    last_date = df['Timestamp'].max().date()

    # Get today's UTC date
    today = datetime.now(timezone.utc).date()

    # Identify missing days
    missing_days = pd.date_range(start=last_date + timedelta(days=1), end=today - timedelta(days=1))

    return missing_days

# Fetch data for missing days and append to the dataset
def fetch_and_append_missing_data(currency_pair, missing_days, existing_data_filename, output_filename):
    df_existing = pd.read_csv(existing_data_filename)
    all_new_data = []

    for day in missing_days:
        start_timestamp = int(time.mktime(day.timetuple()))
        end_timestamp = int(time.mktime((day + timedelta(days=1)).timetuple()))
        
        # Fetch data for the day
        new_data = fetch_bitstamp_data(currency_pair, start_timestamp, end_timestamp)
        
        if new_data:
            df_new = pd.DataFrame(new_data)
            df_new['timestamp'] = pd.to_numeric(df_new['timestamp'], errors='coerce')
            df_new['timestamp'] = pd.to_datetime(df_new['timestamp'], unit='s')
            df_new.columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
            all_new_data.append(df_new)

    # Combine new data with existing data
    if all_new_data:
        df_combined = pd.concat([df_existing] + all_new_data, ignore_index=True)
        df_combined.drop_duplicates(subset='Timestamp', inplace=True)
        df_combined.sort_values(by='Timestamp', ascending=True, inplace=True)

        # Save the updated dataset to the specified file
        df_combined.to_csv(output_filename, index=False)
        print(f"Updated dataset saved to {output_filename}")
    else:
        print("No new data found.")
        df_existing.to_csv(output_filename, index=False)

# Main execution
if __name__ == "__main__":
    dataset_slug = "mczielinski/bitcoin-historical-data"  # Kaggle dataset slug
    currency_pair = "btcusd"
    existing_data_filename = "btcusd_1-min_data.csv"  # The existing dataset filename
    output_filename = "btcusd_1-min_data.csv"  # The output filename (same as the dataset name on Kaggle)

    # Step 1: Download the latest dataset from Kaggle
    download_latest_dataset(dataset_slug)

    # Step 2: Check for missing days
    missing_days = check_missing_days(existing_data_filename)

    # Step 3: Fetch and append missing data
    if len(missing_days) > 0:
        print(f"Missing data for {len(missing_days)} days: {missing_days}")
        fetch_and_append_missing_data(currency_pair, missing_days, existing_data_filename, output_filename)
    else:
        print("No missing data to fetch.")

