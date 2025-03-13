from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# ✅ Configuration
TICKERS = [
    # 🔹 Tech Giants
    "AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "META", "NVDA", "AMD", "INTC", "ORCL",
] 
START_DATE = "1990-01-01"
END_DATE = "2025-01-31"
INTERVAL = "1d"

# ✅ Default arguments for Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def create_dag():
    with DAG(
        dag_id="stock_data_pipeline_fast",
        default_args=default_args,
        description="DAG to ingest, preprocess, and curate stock data",
        schedule_interval="@daily",
        start_date=datetime(2024, 3, 13),
        catchup=False,
        concurrency=len(TICKERS)
    ) as dag:

        # # ✅ Task 1: Download all tickers to raw storage (Runs first)
        # task_unpack = BashOperator(
        #     task_id="download_to_raw",
        #     bash_command=f"python /opt/airflow/scripts/unpack_to_raw.py {' '.join(TICKERS)} {START_DATE} {END_DATE} {INTERVAL} --fast --endpoint_url http://localstack:4566",
        # )

        # ✅ Create task dictionaries for dependencies
        unpack_tasks = {}
        preprocess_tasks = {}
        process_tasks = {}


        # ✅ Task 1: Download each ticker to raw storage (Runs in parallel)
        for ticker in TICKERS:
            unpack_tasks[ticker] = BashOperator(
                task_id=f"download_{ticker}",
                bash_command=f"python /opt/airflow/scripts/unpack_to_raw.py --fast {ticker} {START_DATE} {END_DATE} {INTERVAL} --endpoint_url http://localstack:4566",
            )

        # ✅ Task 2: Preprocess Staging (Runs in parallel for each ticker)
        for ticker in TICKERS:
            preprocess_tasks[ticker] = BashOperator(
                task_id=f"preprocess_{ticker}",
                bash_command=f"python /opt/airflow/scripts/preprocess_to_staging.py {ticker} --fast --endpoint_url http://localstack:4566",
            )
            unpack_tasks[ticker] >> preprocess_tasks[ticker]  # ✅ Ensure unpack runs first

        # ✅ Task 3: Process to Curated (Runs in parallel after preprocessing)
        for ticker in TICKERS:
            process_tasks[ticker] = BashOperator(
                task_id=f"process_{ticker}",
                bash_command=f"python /opt/airflow/scripts/process_to_curated.py {ticker} --fast --endpoint_url http://localstack:4566",
            )
            preprocess_tasks[ticker] >> process_tasks[ticker]  # ✅ Ensure preprocess runs before processing

    return dag

# ✅ Assign the DAG
globals()["stock_data_pipeline_fast"] = create_dag()