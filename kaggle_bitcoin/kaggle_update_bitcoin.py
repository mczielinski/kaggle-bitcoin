import os
import time
from datetime import datetime, timedelta, timezone

import kaggle
import pandas as pd
import requests


# Function to fetch data from Bitstamp API
def fetch_bitstamp_data(
    currency_pair, start_timestamp, end_timestamp, step=60, limit=1000
):
    """
    Fetch OHLC data from Bitstamp API.
    
    Args:
        currency_pair: Trading pair to fetch (e.g. 'btcusd')
        start_timestamp: Start time as Unix timestamp
        end_timestamp: End time as Unix timestamp
        step: Time interval in seconds (60 = 1 minute)
        limit: Maximum number of data points per request
        
    Returns:
        List of OHLC data points
    """
    url = f"https://www.bitstamp.net/api/v2/ohlc/{currency_pair}/"
    params = {
        "step": step,  # 60 seconds (1-minute interval)
        "start": start_timestamp,
        "end": end_timestamp,
        "limit": limit,  # Fetch 1000 data points max per request
    }
    try:
        response = requests.get(url, params=params, timeout=60)
        response.raise_for_status()
        return response.json().get("data", {}).get("ohlc", [])
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []


# Download the latest dataset from Kaggle
def download_latest_dataset(dataset_slug):
    """Download the latest dataset from Kaggle."""
    # Use Kaggle Python API to download the dataset directly to memory
    kaggle.api.dataset_download_files(dataset_slug, path="upload", unzip=True)


def download_latest_metadata(dataset_slug):
    """Download the dataset metadata from Kaggle."""
    kaggle.api.dataset_metadata(dataset_slug, path="upload")


# Check for missing days and return a list of dates to scrape
def check_missing_days(existing_data_filename):
    """
    Check for missing days in the dataset.
    
    Args:
        existing_data_filename: Path to the existing CSV file
        
    Returns:
        List of dates that need to be fetched
    """
    df = pd.read_csv(existing_data_filename)

    # Ensure the Timestamp column is interpreted as Unix timestamp
    df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
    
    # Convert Unix timestamps to UTC datetime
    df["datetime"] = pd.to_datetime(df["Timestamp"], unit="s", utc=True)
    
    # Find the last available date in the dataset (in UTC)
    last_date = df["datetime"].max().date()
    
    # Get yesterday's UTC date (since we want to fetch complete days)
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    
    # Identify missing days (from the day after last date up to yesterday)
    # We only fetch complete days, so we don't fetch today's incomplete data
    missing_days = []
    if last_date < yesterday:
        missing_days = pd.date_range(
            start=last_date + timedelta(days=1), 
            end=yesterday,
            normalize=True,  # Set time to midnight
            freq="D"         # Daily frequency
        )
        print(f"Missing data from {last_date + timedelta(days=1)} to {yesterday}")
    else:
        print(f"Dataset is up to date as of {last_date}")
        
    return missing_days


# Fetch data for missing days and append to the dataset
def fetch_and_append_missing_data(
    currency_pair, missing_days, existing_data_filename, output_filename
):
    """
    Fetch data for missing days and append to the dataset.
    
    Args:
        currency_pair: Trading pair to fetch (e.g. 'btcusd')
        missing_days: List of dates to fetch
        existing_data_filename: Path to the existing CSV file
        output_filename: Path to save the updated CSV file
    """
    df_existing = pd.read_csv(existing_data_filename)
    df_existing["Timestamp"] = pd.to_numeric(df_existing["Timestamp"], errors="coerce")
    all_new_data = []

    for day in missing_days:
        # Convert the date to midnight UTC
        start_date = datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=timezone.utc)
        end_date = start_date + timedelta(days=1)
        
        # Convert to Unix timestamps
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        
        print(f"Fetching data for {day.date()} (UTC)")
        print(f"  - Start: {start_date} ({start_timestamp})")
        print(f"  - End: {end_date} ({end_timestamp})")

        # Bitstamp API has a limit of 1000 entries per request
        # For 1-minute data, we need to make multiple requests to cover a full day
        # A full day has 1440 minutes, so we'll need at least 2 requests
        
        # We'll use chunks of 12 hours (720 minutes) to be safe
        time_chunks = []
        current_start = start_timestamp
        chunk_size = 12 * 60 * 60  # 12 hours in seconds
        
        while current_start < end_timestamp:
            current_end = min(current_start + chunk_size, end_timestamp)
            time_chunks.append((current_start, current_end))
            current_start = current_end
        
        day_data = []
        for chunk_start, chunk_end in time_chunks:
            chunk_data = fetch_bitstamp_data(
                currency_pair, chunk_start, chunk_end, step=60
            )
            day_data.extend(chunk_data)
            # Be nice to the API and don't hammer it
            time.sleep(1)
        
        if day_data:
            df_day = pd.DataFrame(day_data)
            df_day["timestamp"] = pd.to_numeric(df_day["timestamp"], errors="coerce")
            
            # Rename columns to match existing dataset
            df_day.columns = [
                "Timestamp",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
            
            print(f"  - Retrieved {len(df_day)} data points")
            all_new_data.append(df_day)
        else:
            print(f"  - No data available for this day")

    # Combine new data with existing data
    if all_new_data:
        df_new = pd.concat(all_new_data, ignore_index=True)
        print(f"Total new data points: {len(df_new)}")
        
        df_combined = pd.concat([df_existing, df_new], ignore_index=True)
        print(f"Combined dataset size before deduplication: {len(df_combined)}")
        
        # Remove any duplicate timestamps
        df_combined = df_combined.drop_duplicates(subset="Timestamp", keep="first")
        print(f"Final dataset size after deduplication: {len(df_combined)}")
        
        # Sort by timestamp
        df_combined = df_combined.sort_values(by="Timestamp", ascending=True)

        # Save the updated dataset
        df_combined.to_csv(output_filename, index=False)
        print(f"Updated dataset saved to {output_filename}")
    else:
        print("No new data found.")
        df_existing.to_csv(output_filename, index=False)


# Main execution
if __name__ == "__main__":
    dataset_slug = "mczielinski/bitcoin-historical-data"  # Kaggle dataset slug
    currency_pair = "btcusd"
    upload_dir = "upload"

    # Ensure the 'upload/' directory exists
    if not os.path.exists(upload_dir):
        os.makedirs(upload_dir)

    existing_data_filename = os.path.join(
        upload_dir, "btcusd_1-min_data.csv"
    )  # The dataset file
    output_filename = existing_data_filename  # Output filename (same as the dataset name on Kaggle)

    print(f"Current time (UTC): {datetime.now(timezone.utc)}")
    
    # Step 1: Download the latest dataset and metadata from Kaggle
    print("Downloading dataset metadata from Kaggle...")
    download_latest_metadata(dataset_slug)  # Download metadata to 'upload/'
    
    print("Downloading dataset from Kaggle...")
    download_latest_dataset(dataset_slug)  # Download dataset to 'upload/'

    # Step 2: Check for missing days
    print("Checking for missing days...")
    missing_days = check_missing_days(existing_data_filename)

    # Step 3: Fetch and append missing data
    if len(missing_days) > 0:
        print(f"Found {len(missing_days)} missing days to fetch.")
        fetch_and_append_missing_data(
            currency_pair, missing_days, existing_data_filename, output_filename
        )
    else:
        print("No missing data to fetch.")