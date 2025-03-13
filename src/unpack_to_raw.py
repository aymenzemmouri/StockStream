import argparse
import yfinance as yf
import pandas as pd
import concurrent.futures
import time
import boto3
import io
from boto3.s3.transfer import TransferConfig
from pathlib import Path
import pyarrow as pa
import pyarrow.feather as feather
import json
import pyarrow.compute as pc

S3_ENDPOINT_URL = "http://localhost:4566"
RAW_BUCKET_NAME = "raw"
CACHE_BUCKET_NAME = "cache"  # Separate bucket for storing the cache
CACHE_FILE_KEY = "download_cache_raw.json"


def unpack_to_raw(ticker, start_date, end_date, interval, endpoint_url=S3_ENDPOINT_URL):
    """
    Downloads stock data for a given ticker and stores it in a specified partitioned raw format (year/month/day).
    :param ticker: Stock ticker symbol (e.g., "AAPL")
    :param start_date: Start date (YYYY-MM-DD)
    :param end_date: End date (YYYY-MM-DD)
    :param interval: Data interval (e.g., "1d", "1h")
    :param output_path: Base path where raw data should be stored.
    """
    # print(f"Downloading data for {ticker} from {start_date} to {end_date} with interval {interval}")

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
    )
    
    try:
        data = yf.download(ticker, start=start_date, end=end_date, interval=interval)

        if data.empty:
            print(f"⚠️ Unpack :  No data found for {ticker} in the given date range.")
            return pd.DataFrame()
        
        data.columns = data.columns.get_level_values('Price')
        data.reset_index(inplace=True)

        # Convert Datetime to Date

        # Store Raw Data in S3 (Partitioned by Year/Month/Day)
        date_column = 'Datetime' if 'Datetime' in data.columns else 'Date'
        for (year, month), month_data in data.groupby([data[date_column].dt.year, data[date_column].dt.month]):

            s3_key = f"{ticker}/{year}/{month:02d}.csv"

            # Convert DataFrame to Parquet in-memory
            buffer = io.BytesIO()
            month_data.to_csv(buffer, index=False)
            buffer.seek(0)

            # Upload to S3
            s3_client.put_object(Bucket=RAW_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
            # print(f Raw data saved to S3: s3://raw/{s3_key}")

        
        # print("⌛ Duration: ", time.time() - start_time)
        return data
    except Exception as e:
        print(f"❌ Unpack : Error downloading data for {ticker}: {e}")
        return pd.DataFrame()
    


####################################################################################################
#  FAST 

# Define TransferConfig for faster S3 uploads
transfer_config = TransferConfig(multipart_threshold=5 * 1024 * 1024, max_concurrency=10)
def get_arrow_schema(df):
    schema_dict = {}
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            schema_dict[col] = pa.timestamp("ns")  # Correct datetime type
        elif df[col].dtype == "object":
            schema_dict[col] = pa.string()
        else:
            schema_dict[col] = pa.float64()
    return pa.schema(schema_dict)

def upload_to_s3(ticker, year, month, table, s3_client):
    try:
        s3_key = f"{ticker}/{year}/{month:02d}.feather"
        buffer = io.BytesIO()
        feather.write_feather(table, buffer)
        buffer.seek(0)

        # Parallelized S3 Upload
        s3_client.upload_fileobj(buffer, RAW_BUCKET_NAME, s3_key, Config=transfer_config)
        # print(f Uploaded: {s3_key}")

    except Exception as e:
        print(f"❌ Error uploading {s3_key}: {e}")

def load_cache_from_s3(s3_client):
    """
    Load the cache file from S3.
    If the file doesn't exist, return an empty dictionary.
    """
    try:
        obj = s3_client.get_object(Bucket=CACHE_BUCKET_NAME, Key=CACHE_FILE_KEY)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        print("⚠️ Cache file not found in S3. Starting fresh.")
        return {}
    except Exception as e:
        print(f"⚠️ Error loading cache from S3: {e}")
        return {}

def save_cache_to_s3(cache, s3_client):
    """
    Save the updated cache file to S3.
    """
    try:
        cache_json = json.dumps(cache, indent=4)
        s3_client.put_object(Bucket=CACHE_BUCKET_NAME, Key=CACHE_FILE_KEY, Body=cache_json.encode("utf-8"))
        print("✅ Cache updated and saved to S3.")
    except Exception as e:
        print(f"⚠️ Error saving cache to S3: {e}")

def get_missing_date_ranges(start_date, end_date, existing_data):
    """
    Determines which date ranges are missing by comparing with cached data.
    """
    start = pd.to_datetime(start_date)
    end = pd.to_datetime(end_date)

    missing_dates = []
    for date in pd.date_range(start=start, end=end, freq="MS"):  # Monthly start dates
        year, month = date.year, date.month
        if (year, month) not in existing_data:
            missing_dates.append((year, month))
    
    return missing_dates

# Optimized unpack function with PyArrow Compute & multi-threading for multiple tickers at a time
def unpack_to_raw_fast(tickers, start_date, end_date, interval, endpoint_url=S3_ENDPOINT_URL):
    """
    Fetches stock data from Yahoo Finance **only for missing date ranges** using a JSON cache stored in S3.
    """
    
    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
    )

    cache = load_cache_from_s3(s3_client)
    missing_ticker_ranges = {}

    for ticker in tickers:
        # Step 1: Check cache from S3 first
        existing_data = set(tuple(x) for x in cache.get(ticker, []))

        # Step 2: Find missing months (without checking S3)
        missing_dates = get_missing_date_ranges(start_date, end_date, existing_data)

        if not missing_dates:
            print(f"⏭️ Skipping {ticker} - All data exists in cache.")
            continue

        missing_ticker_ranges[ticker] = missing_dates

    # Step 3: If all data is cached, exit early
    if not missing_ticker_ranges:
        print("✅ All requested data is already cached. No need to fetch from Yahoo Finance.")
        return

    print(f"📡 Fetching missing data for tickers: {missing_ticker_ranges}")

    # Step 4: Download missing data from Yahoo Finance
    yf_start_date = min([f"{year}-{month:02d}-01" for ticker in missing_ticker_ranges for year, month in missing_ticker_ranges[ticker]])
    yf_end_date = end_date  # Download up to requested end date

    data = yf.download(
        tickers=list(missing_ticker_ranges.keys()),
        start=yf_start_date,
        end=yf_end_date,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,
        prepost=False,
        threads=True,
        proxy=None
    )

    if data.empty:
        print(f"⚠️ No data found for requested tickers.")
        return None

    futures = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        for ticker, missing_dates in missing_ticker_ranges.items():
            if ticker not in data.columns.get_level_values(0).unique():
                print(f"⚠️ {ticker} data not found in Yahoo Finance response. Skipping.")
                continue

            ticker_data = data[ticker].reset_index()

            date_column = "Datetime" if "Datetime" in ticker_data.columns else "Date"
            if date_column not in ticker_data.columns:
                print(f"⚠️ Date column '{date_column}' not found for {ticker}. Skipping...")
                continue

            # Convert to PyArrow Table
            arrow_table = pa.Table.from_pandas(ticker_data, preserve_index=False)

            date_array = arrow_table.column(date_column)
            year_array = pc.year(date_array)
            month_array = pc.month(date_array)

            arrow_table = arrow_table.append_column("Year", year_array)
            arrow_table = arrow_table.append_column("Month", month_array)

            # Step 5: Upload only missing months & update cache
            for year, month in missing_dates:
                year_filter = pc.equal(arrow_table["Year"], year)
                month_filter = pc.equal(arrow_table["Month"], month)
                mask = pc.and_(year_filter, month_filter)
                month_data = arrow_table.filter(mask)

                if month_data.num_rows > 0:
                    futures.append(executor.submit(upload_to_s3, ticker, year, month, month_data, s3_client))
                    cache.setdefault(ticker, []).append([year, month])  # Update cache

    # Step 6: Save updated cache to S3
    save_cache_to_s3(cache, s3_client)

    # Wait for all uploads to complete
    concurrent.futures.wait(futures)






if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download stock data and store in S3.")
    parser.add_argument("tickers", type=str, nargs="+", help="Stock ticker symbols (e.g., AAPL MSFT GOOGL)")
    parser.add_argument("start_date", type=str, help="Start date in YYYY-MM-DD format")
    parser.add_argument("end_date", type=str, help="End date in YYYY-MM-DD format")
    parser.add_argument("interval", type=str, help="Data interval (e.g., 1d, 1h)")
    parser.add_argument("--fast", action="store_true", help="Use optimized fast processing for multiple tickers")
    
    parser.add_argument("--endpoint_url", type=str, default="http://localhost:4566", help="Custom S3 endpoint URL (default: LocalStack)")
    
    args = parser.parse_args()

    start_time = time.time()

    # Check if multiple tickers were provided
    if args.fast:
        unpack_to_raw_fast(args.tickers, args.start_date, args.end_date, args.interval, args.endpoint_url)
    elif len(args.tickers) == 1:
        unpack_to_raw(args.tickers[0], args.start_date, args.end_date, args.interval, args.endpoint_url)
    else:
        print("❌ Error: Multiple tickers are only supported with the --fast flag.")
        exit(1)

    print(f"⌛ Processing completed in {time.time() - start_time:.4f} seconds")
