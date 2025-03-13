from sanic import Sanic, response


import time 
import os
import pandas as pd
import concurrent.futures
import asyncio
import boto3

import sys
from pathlib import Path

# Get the absolute path to the 'src' directory
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(src_path))

# Now import the files
from unpack_to_raw import unpack_to_raw, unpack_to_raw_fast
from preprocess_to_staging import preprocess_to_staging, preprocess_to_staging_fast
from process_to_curated import process_to_curated, process_to_curated_fast


app = Sanic("StockDataAPI")

S3_ENDPOINT_URL = "http://localstack:4566"

# Initialize S3 Client
s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT_URL,  # LocalStack S3
    aws_access_key_id="root",
    aws_secret_access_key="root",
    region_name="us-east-1"
)


@app.get("/health")
async def health_check(request):
    return response.json({"status": "API is running"})


@app.get("/stats")
async def get_data_lake_stats(request):
    """
    Returns the number of files and estimated total size per bucket.
    """
    try:
        buckets = ["raw", "staging", "curated"]
        stats = {}

        for bucket in buckets:
            s3_response = s3_client.list_objects_v2(Bucket=bucket)  # ✅ Use a different variable name for S3 response

            if "Contents" in s3_response:
                total_files = len(s3_response["Contents"])
                total_size = sum(obj["Size"] for obj in s3_response["Contents"]) / (1024**2)  # Convert bytes to MB
                stats[bucket] = {"files": total_files, "total_size_mb": round(total_size, 2)}
            else:
                stats[bucket] = {"files": 0, "total_size_mb": 0.0}

        return response.json({"buckets": stats})  # ✅ Ensure correct JSON response
    except Exception as e:
        return response.json({"error": str(e)}, status=500)









@app.post("/ingest")
async def ingest_stock_data(request):
    
    try:
        data = request.json
        tickers = data.get("tickers", [])
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        # interval = data.get("interval", "1h")

        print("tickers", tickers)

        if not tickers or not start_date or not end_date:
            return response.json({"error": "Missing required parameters: tickers, start_date, end_date"}, status=400)
        
        results = []
        
        unpack_duration = 0
        preprocess_duration = 0
        process_duration = 0

        start_time = time.time()

        for ticker in tickers:

            start_time_unpack = time.time()
            unpack_to_raw(ticker, start_date, end_date, "1d")
            unpack_duration += time.time() - start_time_unpack

            start_time_preprocess = time.time()
            preprocess_to_staging(ticker, S3_ENDPOINT_URL)
            print("preprocess completed")
            preprocess_duration += time.time() - start_time_preprocess

            start_time_process = time.time()
            process_to_curated(ticker, S3_ENDPOINT_URL)
            print("process completed")
            process_duration += time.time() - start_time_process
            
            results.append({"ticker": ticker, "status": "success"})
        
        duration = time.time() - start_time
        return response.json({"message": "Data ingestion completed successfully", "tickers": results, "duration": duration, "unpack_duration": unpack_duration, "preprocess_duration": preprocess_duration, "process_duration": process_duration})
    except Exception as e:
        return response.json({"error": str(e)}, status=500)


@app.post("/ingest_fast")
async def ingest_stock_data_fast(request):
    try:
        data = request.json
        tickers = data.get("tickers", [])
        start_date = data.get("start_date")
        end_date = data.get("end_date")

        if not tickers or not start_date or not end_date:
            return response.json({"error": "Missing required parameters: tickers, start_date, end_date"}, status=400)

        start_time_total = time.time()

        
        # Step 1: Sequentially Unpack Raw Data for All Tickers
        start_time_unpack = time.time()
        unpack_to_raw_fast(tickers, start_date, end_date, "1d", S3_ENDPOINT_URL)
        unpack_duration = time.time() - start_time_unpack

        # Step 2 & 3: Preprocessing and Processing in Parallel
        async def process_ticker(ticker):
            try:
                # Step 2: Preprocess Staging
                await asyncio.to_thread(preprocess_to_staging_fast, ticker, S3_ENDPOINT_URL)

                # Step 3: Process to Curated
                await asyncio.to_thread(process_to_curated_fast, ticker, S3_ENDPOINT_URL)

                return {"ticker": ticker, "status": "success"}

            except Exception as e:
                return {"ticker": ticker, "status": f"failed - {str(e)}"}

        # Run Preprocessing & Processing in Parallel
        print("🚀 Starting Preprocessing & Processing Phase (Parallel)...")
        tasks = [process_ticker(ticker) for ticker in tickers]  # No need for `create_task`
        results = await asyncio.gather(*tasks)  # Properly waits for completion

        total_duration = time.time() - start_time_total
        return response.json({
            "message": "Data ingestion completed successfully",
            "tickers": results,
            "unpack_duration": unpack_duration,
            "total_duration": total_duration,
        })
    except Exception as e:
        return response.json({"error": str(e)}, status=500)








# Function to fetch a file from LocalStack S3
def get_s3_file(ticker, bucket):
    try:
        # List all objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket)
        if "Contents" in response:
            # Filter files that start with the ticker name (any extension)
            matching_files = [obj["Key"] for obj in response["Contents"] if obj["Key"].startswith(ticker)]

            if not matching_files:
                return None  # No matching files found

            # Fetch the first matching file
            file_key = matching_files[0]  # You can modify this to return all matching files
            obj = s3_client.get_object(Bucket=bucket, Key=file_key)
            return obj["Body"].read()
    except s3_client.exceptions.ClientError:
        return None

# Function to list all files in a bucket (when no ticker is provided)
def list_s3_files(bucket):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket)
        files = [obj["Key"] for obj in response.get("Contents", [])]
        return files if files else None
    except s3_client.exceptions.ClientError:
        return None


# Route: Fetch Raw Data or List Files
@app.get("/raw/", name="raw_list")
@app.get("/raw/<ticker>", name="raw_ticker")
async def get_raw_data(request, ticker=None):
    return await handle_s3_request(request, "raw", ticker)

# Route: Fetch Staging Data or List Files
@app.get("/staging/", name="staging_list")
@app.get("/staging/<ticker>", name="staging_ticker")
async def get_staging_data(request, ticker=None):
    return await handle_s3_request(request, "staging", ticker)

# Route: Fetch Curated Data or List Files
@app.get("/curated/", name="curated_list")
@app.get("/curated/<ticker>", name="curated_ticker")
async def get_curated_data(request, ticker=None):
    return await handle_s3_request(request, "curated", ticker)

# Generic Function to Handle Both File Retrieval & Listing
async def handle_s3_request(request, bucket, ticker=None):
    if ticker:
        file_data = get_s3_file(ticker, bucket)
        if file_data:
            return response.raw(file_data, content_type="application/octet-stream")
        return response.json({"error": f"{bucket.capitalize()} data not found for {ticker}"}, status=404)
    else:
        files = list_s3_files(bucket)
        if files:
            return response.json({"files": files})
        return response.json({"error": f"No files found in {bucket}"}, status=404)

# Run the API on Port 8082
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)