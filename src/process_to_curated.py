import pandas as pd

import os
import pandas as pd
import numpy as np
from numba import jit, prange
import time

import time
import pandas as pd
import boto3
import pyarrow.feather as feather
from io import BytesIO

# S3 Configuration for LocalStack
S3_ENDPOINT_URL = "http://localhost:4566"
STAGING_BUCKET_NAME = "staging"
CURATED_BUCKET_NAME = "curated"


def read_s3_csv(bucket_name, key, s3_client):
    """Reads a csv file from S3 and returns a DataFrame."""
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(BytesIO(file_obj["Body"].read()))


def read_s3_feather(bucket_name, key, s3_client):
    """Reads a Feather file from S3 and returns a DataFrame."""
    file_obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    return feather.read_feather(BytesIO(file_obj["Body"].read()))


def write_s3_csv(data, bucket_name, key, s3_client):
    """Writes a DataFrame as a csv file to S3."""
    buffer = BytesIO()
    data.to_csv(buffer, index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket_name, key)


def write_s3_feather(data, bucket_name, key, s3_client):
    """Writes a DataFrame as a Feather file to S3."""
    buffer = BytesIO()
    feather.write_feather(data, buffer)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket_name, key)

def write_s3_parquet(data, bucket_name, key, s3_client):
    """Writes a DataFrame as a Parquet file to S3."""
    buffer = BytesIO()
    data.to_parquet(buffer, index=False)
    buffer.seek(0)
    s3_client.upload_fileobj(buffer, bucket_name, key)


def compute_technical_indicators(data):
    """
    Adds technical indicators and time-based features to stock data.
    
    Parameters:
        data (pd.DataFrame): DataFrame containing stock market data with at least 'Date' and 'Close' columns.
    
    Returns:
        pd.DataFrame: DataFrame with added technical indicators.
    """
    # Convert Date column to datetime
    data['Date'] = pd.to_datetime(data['Date'])

    # Moving Averages
    data['SMA_10'] = data['Close'].rolling(window=10, min_periods=1).mean()
    data['SMA_50'] = data['Close'].rolling(window=50, min_periods=1).mean()
    data['SMA_200'] = data['Close'].rolling(window=200, min_periods=1).mean()

    # Exponential Moving Averages
    data['EMA_10'] = data['Close'].ewm(span=10, adjust=False).mean()
    data['EMA_50'] = data['Close'].ewm(span=50, adjust=False).mean()
    data['EMA_200'] = data['Close'].ewm(span=200, adjust=False).mean()

    # Bollinger Bands
    rolling_std = data['Close'].rolling(window=10, min_periods=1).std()
    data['Upper_Band'] = data['SMA_10'] + (rolling_std * 2)
    data['Lower_Band'] = data['SMA_10'] - (rolling_std * 2)

    # MACD Indicator
    data['EMA_12'] = data['Close'].ewm(span=12, adjust=False).mean()
    data['EMA_26'] = data['Close'].ewm(span=26, adjust=False).mean()
    data['MACD'] = data['EMA_12'] - data['EMA_26']
    data['MACD_Signal'] = data['MACD'].ewm(span=9, adjust=False).mean()
    data['MACD_Histogram'] = data['MACD'] - data['MACD_Signal']

    # RSI (Relative Strength Index)
    delta = data['Close'].diff()
    gain = np.where(delta > 0, delta, 0)
    loss = np.where(delta < 0, -delta, 0)
    avg_gain = pd.Series(gain).rolling(window=14, min_periods=1).mean()
    avg_loss = pd.Series(loss).rolling(window=14, min_periods=1).mean()
    rs = avg_gain / avg_loss
    data['RSI_14'] = 100 - (100 / (1 + rs))

    # Average True Range (ATR) - Measures volatility
    high_low = data['High'] - data['Low']
    high_close = np.abs(data['High'] - data['Close'].shift())
    low_close = np.abs(data['Low'] - data['Close'].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    data['ATR'] = tr.rolling(window=14, min_periods=1).mean()

    # Standard Deviation (Measures volatility)
    data['Std_Dev'] = data['Close'].rolling(window=10, min_periods=1).std()

    # Time-based Features
    data["DayOfWeek"] = data["Date"].dt.dayofweek
    data["Month"] = data["Date"].dt.month
    data["Year"] = data["Date"].dt.year
    data["Is_Weekend"] = data["DayOfWeek"].apply(lambda x: 1 if x >= 5 else 0)

    return data


def process_to_curated(ticker, endpoint_url=S3_ENDPOINT_URL):
    """
    Enhances stock data from LocalStack S3 (Staging Bucket) and saves to Curated (CSV).
    """
    s3_client = boto3.client("s3", endpoint_url=endpoint_url)

    try:
        staging_key = f"{ticker}.csv"
        data = read_s3_csv(STAGING_BUCKET_NAME, staging_key, s3_client)

        data = compute_technical_indicators(data)

        curated_key = f"{ticker}.csv"
        write_s3_csv(data, CURATED_BUCKET_NAME, curated_key, s3_client)

        return data

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        return pd.DataFrame()


####################################################################################################
#  FAST


def process_to_curated_fast(ticker, endpoint_url=S3_ENDPOINT_URL):
    """
    Enhances stock data from LocalStack S3 (Staging Bucket) and saves to Curated (Feather).
    """
    print(f"📊 Processing {ticker} to curated (Feather)...")

    s3_client = boto3.client("s3", endpoint_url=endpoint_url)
    try:
        # Read from S3 (Staging)
        staging_key = f"{ticker}.feather"
        data = read_s3_feather(STAGING_BUCKET_NAME, staging_key, s3_client)

        data = compute_technical_indicators(data)
        
        curated_key = f"{ticker}.parquet"
        write_s3_parquet(data, CURATED_BUCKET_NAME, curated_key, s3_client)

        return data

    except Exception as e:
        print(f"❌ Error processing {ticker}: {e}")
        return pd.DataFrame()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Process stock data to curated layer with feature engineering.")
    parser.add_argument("ticker", type=str, help="Stock ticker symbol (e.g., AAPL).")
    parser.add_argument("--fast", action="store_true", help="Use optimized fast processing")

    parser.add_argument("--endpoint_url", type=str, default="http://localhost:4566", help="Custom S3 endpoint URL (default: LocalStack)")
    
    args = parser.parse_args()

    start_time = time.time()
    
    if args.fast:
        process_to_curated_fast(args.ticker, endpoint_url = args.endpoint_url)
    else:
        process_to_curated(args.ticker, endpoint_url = args.endpoint_url)

    print(f"⌛ Processing completed in {time.time() - start_time:.4f} seconds")