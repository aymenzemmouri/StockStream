import concurrent
import pandas as pd
import time
import concurrent.futures
import boto3
from io import BytesIO
import os
import time
import numpy as np
import concurrent.futures
import pyarrow.feather as feather 
from numba import jit, prange
from pathlib import Path

# Configure LocalStack S3 client
S3_ENDPOINT_URL = "http://localhost:4566"
STAGING_BUCKET_NAME = "staging"
RAW_BUCKET_NAME = "raw"

def list_s3_files(bucket_name, s3_client, prefix=""):
    """
    List all files in an S3 bucket under a given prefix.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]

def preprocess_to_staging(ticker, endpoint_url=S3_ENDPOINT_URL):
    """
    Cleans and preprocesses raw data from LocalStack S3 raw bucket, then stores it in the staging bucket.
    :param ticker: Stock ticker symbol (e.g., "AAPL").
    """
    start_time = time.time()
    # print(f"🔄 Cleaning and preprocessing data for {ticker}...")
    s3_client = boto3.client("s3", endpoint_url=endpoint_url)
    try:
        # List all csv files in the raw bucket for the given ticker
        raw_files = list_s3_files(RAW_BUCKET_NAME, s3_client, f"{ticker}")
        
        if not raw_files:
            print(f"⚠️ No raw data found for {ticker}. Skipping.")
            return

        all_files = []
        
        # Read csv files from S3
        for file_key in raw_files:
            if file_key.endswith(".csv"):
                
                # Read file from S3
                file_obj = s3_client.get_object(Bucket=RAW_BUCKET_NAME, Key=file_key)
                data = pd.read_csv(BytesIO(file_obj["Body"].read()))
                
                all_files.append(data)

        # Concatenate data
        if not all_files:
            print(f"⚠️ No valid data found for {ticker}.")
            return
        
        data = pd.concat(all_files, ignore_index=True)

        # Handle missing values
        data.ffill(inplace=True)
        data.bfill(inplace=True)
        data.dropna(inplace=True)

        # Normalize numerical columns
        for column in ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']:
            if column in data.columns:
                data[column] = (data[column] - data[column].mean()) / data[column].std()

        # Convert DataFrame to csv format in memory
        buffer = BytesIO()
        data.to_csv(buffer, index=False)
        buffer.seek(0)

        # Upload to LocalStack S3 (Staging Bucket)
        s3_client.upload_fileobj(buffer, STAGING_BUCKET_NAME, f"{ticker}.csv")



    except Exception as e:
        print(f"❌ Error processing data for {ticker}: {e}")






####################################################################################################
# FAST 


@jit(nopython=True, parallel=True)
def normalize_array(arr):
    arr = arr.astype(np.float64)  # Ensure consistent float64 type
    mean = np.mean(arr)
    std = np.std(arr)
    return (arr - mean) / std if std > 0 else arr

def normalize_data(data):
    """
    Applies fast normalization based on dataset size:
    - Uses Pandas for small datasets.
    - Uses Numba for large datasets.
    """
    numeric_columns = list(set(data.columns) & {'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume'})

    for column in numeric_columns:
        data[column] = data[column].astype(np.float64)  # Ensure float64
        normalized_column = f"{column}_normalized"
        if len(data) < 500000: 
            data[normalized_column] = (data[column] - data[column].mean()) / data[column].std()
        else:  # Use Numba for large datasets
            data[normalized_column] = normalize_array(data[column].values)  # Assign back the normalized values
    return data

def preprocess_to_staging_fast(ticker, endpoint_url=S3_ENDPOINT_URL):
    """
    Cleans and preprocesses raw data from LocalStack S3 raw bucket, then stores it in the staging bucket.
    :param ticker: Stock ticker symbol (e.g., "AAPL").
    """
    total_start_time = time.time()
    print(f"🔄 Cleaning and preprocessing data for {ticker}...")
    s3_client = boto3.client("s3", endpoint_url=endpoint_url)
    try:
        start_time = time.time()

        # List all Feather files in S3 raw bucket for this ticker
        raw_prefix = f"{ticker}/"
        raw_files = list_s3_files(RAW_BUCKET_NAME, s3_client, raw_prefix)

        if not raw_files:
            print(f"⚠️ Preprocess : No raw data found for {ticker}. Skipping.")
            return pd.DataFrame()

        all_files = []

        # Read Feather files from S3 in parallel
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future_to_file = {
                executor.submit(
                    lambda key: feather.read_feather(
                        BytesIO(s3_client.get_object(Bucket=RAW_BUCKET_NAME, Key=key)["Body"].read())
                    ),
                    file_key,
                ): file_key
                for file_key in raw_files
                if file_key.endswith(".feather")
            }

            for future in concurrent.futures.as_completed(future_to_file):
                try:
                    all_files.append(future.result())
                except Exception as e:
                    print(f"⚠️ Preprocess : Error reading file {future_to_file[future]}: {e}")

        # print(f"⌛ File reading duration: {time.time() - start_time:.4f} seconds")

        if not all_files:
            print(f"⚠️ Preprocess : No valid data found for {ticker}.")
            return pd.DataFrame()

        # Concatenate all data
        data = pd.concat(all_files, ignore_index=True)
        data.sort_values("Date", inplace=True)
        # print(f"⌛ Data concatenation duration: {time.time() - start_time:.4f} seconds")

        # Handle missing values
        data.ffill(inplace=True)
        data.bfill(inplace=True)
        data.dropna(inplace=True)
        # print(f"⌛ Missing value handling duration: {time.time() - start_time:.4f} seconds")

        # Normalize numerical columns
        start_time = time.time()
        for column in ["Open", "High", "Low", "Close", "Adj Close", "Volume"]:
            if column in data.columns:
                normalized_column = f"{column}_normalized"
                data[normalized_column] = (data[column] - data[column].mean()) / data[column].std()

                print("normalized_column", normalized_column)
        # print(f"⌛ Normalization duration: {time.time() - start_time:.4f} seconds")

        # Convert DataFrame to Feather format in memory
        buffer = BytesIO()
        feather.write_feather(data, buffer)
        buffer.seek(0)

        # Upload preprocessed file to S3 (staging bucket)
        s3_client.upload_fileobj(buffer, STAGING_BUCKET_NAME, f"{ticker}.feather")

        # print(f Preprocessed data saved to S3: s3://{STAGING_BUCKET_NAME}/{ticker}.feather")
        # print(f"🚀 Total processing duration: {time.time() - total_start_time:.4f} seconds")

        return data

    except Exception as e:
        print(f"❌ Preprocess : Error preprocessing data for {ticker}: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Preprocess raw stock data and store in staging area.")
    parser.add_argument("ticker", type=str, help="Stock ticker symbol (e.g., AAPL).")
    parser.add_argument("--fast", action="store_true", help="Use optimized fast processing")

    parser.add_argument("--endpoint_url", type=str, default="http://localhost:4566", help="Custom S3 endpoint URL (default: LocalStack)")
    
    args = parser.parse_args()

    start_time = time.time()
    
    if args.fast:
        preprocess_to_staging_fast(args.ticker, args.endpoint_url)
    else:   
        preprocess_to_staging(args.ticker, args.endpoint_url)

    print(f"🚀 Total duration: {time.time() - start_time:.4f} seconds")