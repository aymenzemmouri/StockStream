# StockStream - Automated Stock Data Processing and Dashboard

## 📌 Overview

This project provides a fully automated stock data pipeline, enabling efficient data ingestion, transformation, and visualization. It is designed to collect stock market data from Yahoo Finance, process it in a structured data lake, and expose it via an API and a dashboard.

The entire infrastructure is containerized using Docker Compose, and deployment is automated with a single script. LocalStack simulates AWS S3 storage for seamless local development.

## 🌟 Features
- **End-to-End Automation:** Fetch, process, and store stock data with minimal manual intervention.
Local AWS Simulation: Uses LocalStack to emulate AWS S3 services locally.
- **Optimized Storage:**
  - Feather & Parquet formats for efficient storage and retrieval
  - DuckDB for fast data querying.
- **Parallel Processing:** Ingests multiple tickers simultaneously using multi-threading.
- **API Access:** Retrieve raw, cleaned, and processed data via a REST API.
- **Interactive Dashboard:** Built with Streamlit and Plotly to visualize stock trends and insights.

---
## 🚀 Technologies Used  

| Technology         | Purpose |
|-------------------|---------|
| **Sanic**         | Fast, asynchronous web framework for handling API requests efficiently. |
| **Apache Airflow** | Automates and schedules the data pipeline execution using DAGs. DAGs can be modified inside `airflow_docker/dags`. |
| **Feather & PyArrow** | Optimized data storage for fast reading/writing (Feather for raw/staging, PyArrow for interoperability). |
| **Parquet & DuckDB** | Parquet provides efficient data compression, while DuckDB enables fast in-memory querying of Parquet files. |
| **Numba**         | Accelerates numerical computations using Just-In-Time (JIT) compilation, improving performance. |
| **Docker Compose** | Manages containerized services, including the API, Airflow, and LocalStack, ensuring an isolated and reproducible environment. |
| **LocalStack**     | Simulates AWS services locally, allowing S3-like storage without an actual AWS account. |
| **Streamlit & Plotly** | Streamlit builds an interactive dashboard, while Plotly generates dynamic visualizations for stock data trends. |

---

## 📂 Architecture

The project is built around a three-layer data lake:
1. **Raw Layer:**
- Stores raw data from Yahoo Finance in Feather format.
- Structured as : 

```  
s3://raw/{ticker}/{year}/{month}.feather
```

- No transformations are applied.
	
2.	**Staging Layer:**
- Cleans and normalizes data (handling missing values, standardizing formats).
- Uses Feather for optimized storage.
- Structured as :

```
s3://staging/{ticker}.feather
```

3.	**Curated Layer:**
- Adds financial indicators (RSI, moving averages, Bollinger Bands, etc.).
- Converts data to Parquet for better compression and query performance.
- Queried efficiently with DuckDB.
- Structured as : 
  
```
s3://curated/{ticker}.parquet
```

---

## ⚡ Execution
**1. Deploy the Infrastructure**

```bash
chmod +x deploy-containers.sh
./deploy-containers.sh
```

🔹 What This Script Does:
   - Start all Docker containers.
   - Set up LocalStack to simulate AWS S3.
   - Create the necessary S3 buckets (raw, staging, curated, cache).

⚠️ Note:
After running the script, check the status of the containers using:

```bash
docker ps -a
```

If you notice that the Airflow webserver container has stopped, restart it manually with:
```bash
docker-compose restart airflow-webserver
```
Once restarted, you should be able to access Airflow at:

```bash
http://localhost:8080
```

This ensures that all services are running correctly and ready for use. 


**2. Test the API**

Once deployed, the API exposes multiple endpoints to interact with the data pipeline.

📥 Ingest Stock Data
- Standard Ingestion (sequential processing):
  
```bash
curl -X POST http://localhost:8000/ingest \
     -H "Content-Type: application/json" \
     -d '{
           "tickers": ["AAPL", "MSFT"],
           "start_date": "2023-01-01",
           "end_date": "2023-12-31"
         }'
```
- Optimized Ingestion (parallel processing, cached downloads):
```bash
curl -X POST http://localhost:8000/ingest_fast \
     -H "Content-Type: application/json" \
     -d '{
           "tickers": ["AAPL", "MSFT"],
           "start_date": "2023-01-01",
           "end_date": "2023-12-31"
         }'
```
📊 Retrieve Data

- Raw Data (unprocessed stock prices):
```bash
curl -X GET http://localhost:8000/raw/AAPL
```

- Staging Data (cleaned & normalized):
```bash
curl -X GET http://localhost:8000/staging/AAPL
```

- Curated Data (enriched with financial indicators):
```bash
curl -X GET http://localhost:8000/curated/AAPL.parquet --output AAPL.parquet
```

📈 Get Data Lake Statistics
```bash
curl -X GET http://localhost:8000/stats
```

🔍 Check API Status
```bash
curl -X GET http://localhost:8000/health
```

**3. Launch the Dashboard**

🔹 The interactive dashboard is built with Streamlit and allows visualization of stock data. To access it, open a browser and go to:

```link
http://localhost:8501
```

**4. Accessing Airflow & Modifying DAGs**

Apache Airflow is used to schedule and automate the data ingestion pipeline. Once the infrastructure is running, you can access the Airflow web UI to monitor and manage DAG executions.

🔹 Start by opening the Airflow UI:

```bash
http://localhost:8080
```
🔹 Login Credentials:
```
Username: airflow
Password: airflow
```

🔹 Inside the Airflow UI, you can:
- View scheduled DAG runs.
- Manually trigger data ingestion pipelines.
- Monitor logs and execution history.


🔹 Modifying DAGs : The Airflow DAGs (Directed Acyclic Graphs) that define the data pipeline execution logic are stored in the following directory:

```
airflow_docker/dags/
```
To modify a DAG, simply edit the Python script inside this folder

---

## 🚀 Next Steps
- Add real-time stock data streaming.
- Improve ML-based stock trend predictions.
- Enhance API security and authentication.
