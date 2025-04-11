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
        "start": int(start_timestamp),
        "end": int(end_timestamp),
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


# Check for missing data
def check_missing_data(existing_data_filename):
    """
    Check for missing data in the dataset.
    
    Args:
        existing_data_filename: Path to the existing CSV file
        
    Returns:
        Tuple containing the last timestamp in the dataset and the current timestamp
    """
    df = pd.read_csv(existing_data_filename)

    # Ensure the Timestamp column is interpreted as Unix timestamp
    df["Timestamp"] = pd.to_numeric(df["Timestamp"], errors="coerce")
    
    # Get the last timestamp in the dataset
    last_timestamp = df["Timestamp"].max()
    
    # Get the current time as a Unix timestamp (minus a buffer of 10 minutes to ensure data is available)
    current_time = datetime.now(timezone.utc) - timedelta(minutes=10)
    current_timestamp = int(current_time.timestamp())
    
    last_datetime = datetime.fromtimestamp(last_timestamp, tz=timezone.utc)
    
    print(f"Last data point in dataset: {last_datetime} (Unix: {last_timestamp})")
    print(f"Current time (minus buffer): {current_time} (Unix: {current_timestamp})")
    
    if current_timestamp > last_timestamp:
        print(f"Gap of {current_timestamp - last_timestamp} seconds detected.")
        return last_timestamp, current_timestamp
    else:
        print("Dataset is up to date.")
        return None, None


# Fetch missing data and append to the dataset
def fetch_and_append_missing_data(
    currency_pair, last_timestamp, current_timestamp, existing_data_filename, output_filename
):
    """
    Fetch missing data and append to the dataset.
    
    Args:
        currency_pair: Trading pair to fetch (e.g. 'btcusd')
        last_timestamp: The last timestamp in the dataset
        current_timestamp: The current timestamp
        existing_data_filename: Path to the existing CSV file
        output_filename: Path to save the updated CSV file
    """
    df_existing = pd.read_csv(existing_data_filename)
    df_existing["Timestamp"] = pd.to_numeric(df_existing["Timestamp"], errors="coerce")
    
    start_timestamp = last_timestamp 
    end_timestamp = current_timestamp
    
    print(f"Fetching data from {datetime.fromtimestamp(start_timestamp, tz=timezone.utc)} to {datetime.fromtimestamp(end_timestamp, tz=timezone.utc)}")
    
    # Break the time period into manageable chunks to respect API limits
    time_chunks = []
    current_start = start_timestamp
    # Each chunk should be at most 1000 minutes (Bitstamp API limit)
    chunk_size = 1000 * 60  # 1000 minutes in seconds
    
    while current_start < end_timestamp:
        current_end = min(current_start + chunk_size, end_timestamp)
        time_chunks.append((current_start, current_end))
        current_start = current_end
    
    all_new_data = []
    
    for i, (chunk_start, chunk_end) in enumerate(time_chunks):
        print(f"Fetching chunk {i+1}/{len(time_chunks)}: {datetime.fromtimestamp(chunk_start, tz=timezone.utc)} to {datetime.fromtimestamp(chunk_end, tz=timezone.utc)}")
        
        chunk_data = fetch_bitstamp_data(
            currency_pair, chunk_start, chunk_end, step=60
        )
        
        if chunk_data:
            df_chunk = pd.DataFrame(chunk_data)
            df_chunk["timestamp"] = pd.to_numeric(df_chunk["timestamp"], errors="coerce")
            
            # Rename columns to match existing dataset
            df_chunk.columns = [
                "Timestamp",
                "Open",
                "High",
                "Low",
                "Close",
                "Volume",
            ]
            
            print(f"  - Retrieved {len(df_chunk)} data points")
            all_new_data.append(df_chunk)
        else:
            print(f"  - No data available for this chunk")
        
        # Be nice to the API and don't hammer it
        time.sleep(1)
    
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

    # Step 2: Check for missing data
    print("Checking for missing data...")
    last_timestamp, current_timestamp = check_missing_data(existing_data_filename)

    # Step 3: Fetch and append missing data
    if last_timestamp is not None and current_timestamp is not None:
        print("Missing data detected.")
        fetch_and_append_missing_data(
            currency_pair, last_timestamp, current_timestamp, existing_data_filename, output_filename
        )
    else:
        print("No missing data to fetch.")