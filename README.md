
# Bitcoin Historical Data Automation

This repository automates the process of fetching, merging, and updating Bitcoin (BTC/USD) historical data from Bitstamp. The updated dataset is then uploaded to [Kaggle](https://www.kaggle.com/mczielinski/bitcoin-historical-data) every day using GitHub Actions.

## What It Does and Why

- **Daily Updates**: The repository ensures that the `btcusd_1-min_data.csv` dataset is always up to date with the latest data by fetching missing days of Bitcoin trading data from the Bitstamp API.
- **Merges Existing Data**: It first downloads the existing dataset from Kaggle, identifies any missing data, fetches it from the Bitstamp API, and then merges it with the existing data.
- **Automated Upload**: Once the missing data is merged, the updated dataset is automatically uploaded back to Kaggle, ensuring that the dataset remains accurate and complete without manual intervention.

## Repository Structure

```
.
├── kaggle_bitcoin
│   └── kaggle_update_bitcoin.py      # Main Python script for data fetching and merging
├── pyproject.toml                    # uv dependency manager file
├── uv.lock                           # uv dependency manager file
└── .github
    └── workflows
        └── kaggle-automation.yml     # GitHub Actions workflow for automation
```

## Workflow Overview

1. **GitHub Actions** automatically triggers every day at midnight (UTC).
2. **The dataset is downloaded** from Kaggle (`btcusd_1-min_data.csv`).
3. **Missing data** is identified by comparing the last available date in the dataset with today’s date.
4. **Data is fetched** from the Bitstamp API to fill in the missing days.
5. **The updated dataset is uploaded** back to Kaggle, keeping the dataset current.

## Kaggle API Setup

Ensure that your Kaggle API credentials are set as GitHub Secrets:
- `KAGGLE_USERNAME`: Your Kaggle username.
- `KAGGLE_KEY`: Your Kaggle API key.

These credentials are used by the script to download and upload the dataset on Kaggle.

## Running Locally

To run the script locally, follow these steps:

1. Install dependencies and run main script using uv:
   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install .        
   python kaggle_bitcoin/kaggle_update_bitcoin.py
   ```

---

This setup automates the process of keeping the Kaggle dataset up-to-date with the latest Bitcoin trading data, ensuring the dataset remains comprehensive without any manual effort.
