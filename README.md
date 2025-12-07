# Dockerized Stock Market Data Pipeline (Airflow + PostgreSQL)

## Overview
This project implements a fully Dockerized, automated data pipeline that fetches daily stock
market data from the **Alpha Vantage API**, processes it, and stores it in a **PostgreSQL database**.
The workflow is orchestrated using **Apache Airflow**, running inside Docker containers.

The pipeline includes:
- Scheduled API ingestion (daily by default)
- Data parsing & validation
- UPSERT insertion into PostgreSQL
- Automatic table creation
- Robust error handling
- Environment variable–based credential management
- Complete container orchestration via Docker Compose

This repository contains all required source files to run the pipeline end-to-end.

---

## Project Structure

```
stock-pipeline/
│
├── docker-compose.yml              # Defines Airflow + Postgres services
├── .env                            # Stores API keys and credentials
├── README.md                       # Documentation
│
├── airflow/
│   ├── dags/
│   │   └── stock_pipeline_dag.py   # Airflow DAG definition
│   └── requirements.txt            # Python dependencies for Airflow
│
└── scripts/
    └── fetch_and_store.py          # Data fetching + database update logic
```

---

## Technologies Used
- **Docker & Docker Compose**
- **Apache Airflow**
- **PostgreSQL 15**
- **Python 3.11**
- **Alpha Vantage Stock Market API**
- **psycopg2**, **requests**

---

## Prerequisites
Before running the pipeline, ensure you have:

- Docker installed  
- Docker Compose installed  
- An Alpha Vantage API Key (free)

Get an API key here:  
https://www.alphavantage.co/support/#api-key

---

## Environment Setup ('.env' file)

Create a '.env' file in the project root with:

```
AIRFLOW_UID=50000
AIRFLOW_GID=0

POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=stocks

ALPHAVANTAGE_API_KEY=YOUR_API_KEY_HERE
STOCK_SYMBOL=MSFT
```

Update `STOCK_SYMBOL` if you want to ingest a different stock.

---

## How to Build & Run the Pipeline

### **1️⃣ Build and Start the Services**

In your project directory:

```bash
docker compose up --build -d
```

This command starts:

- PostgreSQL  
- Airflow Webserver  
- Airflow Scheduler  
- Script volume mounts  
- Airflow initialization scripts  

Allow containers to fully start (10–20 seconds).

---

### **2️⃣ Access Airflow UI**

Open your browser:

- http://localhost:8080

Use the login credentials created during initialization:

```
Username: admin
Password: admin123   (or whatever password you set)
```

---

### **3️⃣ Enable and Trigger the DAG**

Inside Airflow:

1. Locate **stock_market_pipeline**
2. Toggle the DAG to **ON**
3. Click ▶️ **Trigger DAG**

You will see the task `fetch_and_store_stock_data` execute.

---

## Verifying Data in PostgreSQL

Enter the Postgres container:

```bash
docker compose exec postgres bash
```

Open `psql`:

```bash
psql -U airflow -d stocks
```

Check the table:

```sql
SELECT * FROM daily_stock_prices LIMIT 10;
```

Count rows:

```sql
SELECT COUNT(*) FROM daily_stock_prices;
```

You should see stock price entries for each day returned by the API.

---

## ♻️ Stopping the Pipeline

```bash
docker compose down
```

To remove database data:

```bash
docker volume rm stock-pipeline_postgres_data
```

---

## Error Handling & Resilience

The pipeline includes:

- Retry logic (Airflow's retry mechanism)
- JSON parsing validation
- Skipping malformed rows
- UPSERT logic preventing duplication
- Logging at each stage
- Containerized isolation

If API rate limits are hit, Airflow will retry automatically.

---

## Cleaning Up

To clean all containers, images, and volumes:

```bash
docker compose down --volumes --rmi all
```
