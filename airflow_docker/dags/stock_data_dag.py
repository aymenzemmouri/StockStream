from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Configuration
TICKERS = [
    # 🔹 Tech Giants
    "AAPL", "MSFT", "GOOGL", "TSLA", "AMZN", "META", "NVDA", "AMD", "INTC", "ORCL",
] 
START_DATE = "1990-01-01"
END_DATE = "2025-01-31"
INTERVAL = "1d"

# Default arguments for Airflow
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def create_dag():
    with DAG(
        dag_id="stock_data_pipeline",
        default_args=default_args,
        description="DAG to ingest, preprocess, and curate stock data",
        schedule_interval="@daily",
        start_date=datetime(2024, 3, 13),
        catchup=False,
        concurrency=len(TICKERS)
    ) as dag:

        # Create task dictionaries for dependencies
        unpack_tasks = {}
        preprocess_tasks = {}
        process_tasks = {}

        # # Task 1: Download each ticker to raw storage (Runs in parallel)
        # for ticker in TICKERS:
        #     unpack_tasks[ticker] = BashOperator(
        #         task_id=f"download_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/unpack_to_raw.py {ticker} {START_DATE} {END_DATE} {INTERVAL} --endpoint_url http://localstack:4566",
        #     )

        # # Task 2: Preprocess Staging (Runs in parallel after downloading)
        # for ticker in TICKERS:
        #     preprocess_tasks[ticker] = BashOperator(
        #         task_id=f"preprocess_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/preprocess_to_staging.py {ticker} --endpoint_url http://localstack:4566",
        #     )
        #     unpack_tasks[ticker] >> preprocess_tasks[ticker]  # Ensure unpack runs first

        # # Task 3: Process to Curated (Runs in parallel after preprocessing)
        # for ticker in TICKERS:
        #     process_tasks[ticker] = BashOperator(
        #         task_id=f"process_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/process_to_curated.py {ticker} --endpoint_url http://localstack:4566",
        #     )
        #     preprocess_tasks[ticker] >> process_tasks[ticker]  # Ensure preprocess runs before processing


        # # Task 1: Download each ticker to raw storage (Runs in parallel)
        # for ticker in TICKERS:
        #     unpack_tasks[ticker] = BashOperator(
        #         task_id=f"download_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/unpack_to_raw.py {ticker} {START_DATE} {END_DATE} {INTERVAL} --endpoint_url http://localstack:4566",
        #     )

        # # Task 2: Preprocess Staging (Runs in parallel after downloading)
        # for ticker in TICKERS:
        #     preprocess_tasks[ticker] = BashOperator(
        #         task_id=f"preprocess_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/preprocess_to_staging.py {ticker} --endpoint_url http://localstack:4566",
        #     )
        #     unpack_tasks[ticker] >> preprocess_tasks[ticker]  # Ensure unpack runs first

        # # Task 3: Process to Curated (Runs in parallel after preprocessing)
        # for ticker in TICKERS:
        #     process_tasks[ticker] = BashOperator(
        #         task_id=f"process_{ticker}",
        #         bash_command=f"python /opt/airflow/scripts/process_to_curated.py {ticker} --endpoint_url http://localstack:4566",
        #     )
        #     preprocess_tasks[ticker] >> process_tasks[ticker]  # Ensure preprocess runs before processing

        previous_task = None  # To track the last task in the sequence

        for ticker in TICKERS:
            # Step 1: Download Task
            download_task = BashOperator(
                task_id=f"download_{ticker}",
                bash_command=f"python /opt/airflow/scripts/unpack_to_raw.py {ticker} {START_DATE} {END_DATE} {INTERVAL} --endpoint_url http://localstack:4566"
            )

            # Step 2: Preprocessing Task
            preprocess_task = BashOperator(
                task_id=f"preprocess_{ticker}",
                bash_command=f"python /opt/airflow/scripts/preprocess_to_staging.py {ticker} --endpoint_url http://localstack:4566",
            )

            # Step 3: Processing Task
            process_task = BashOperator(
                task_id=f"process_{ticker}",
                bash_command=f"python /opt/airflow/scripts/process_to_curated.py {ticker} --endpoint_url http://localstack:4566",
            )

            # Set up dependencies
            download_task >> preprocess_task >> process_task

            # Ensure sequential execution across tickers
            if previous_task:
                previous_task >> download_task
            
        previous_task = process_task  # Update the last task in the sequence

    return dag

# Assign the DAG
globals()["stock_data_pipeline"] = create_dag()