# RealTimeFinHub — Real-Time Stock Market Data Pipeline

![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake\&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-FF694B?logo=dbt\&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apacheairflow\&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?logo=python\&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker\&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-C72E29?logo=minio\&logoColor=white)

---

## Project Overview

**RealTimeFinHub** is an end-to-end real-time data engineering project designed to collect, stream, store, orchestrate, and transform live stock market data.

The pipeline retrieves stock quotes from the Finnhub API, publishes them to Apache Kafka, stores raw JSON records in MinIO, loads them into Snowflake through Apache Airflow, and transforms the data using dbt.

The project follows a **Medallion Architecture** with Bronze, Silver, and Gold layers.

---

## Architecture

<!-- Insert your architecture image here -->

![RealTimeFinHub Architecture](architecture.png)

### Data Flow

```text
Finnhub API
    ↓
Python Kafka Producer
    ↓
Apache Kafka
    ↓
Python Kafka Consumer
    ↓
MinIO Object Storage
    ↓
Apache Airflow
    ↓
Snowflake Bronze Layer
    ↓
dbt Transformations
    ↓
Snowflake Silver and Gold Layers
```

---

## Tech Stack

* **Python** — API integration, Kafka producer, and Kafka consumer
* **Finnhub API** — Live stock market data source
* **Apache Kafka** — Real-time event streaming
* **MinIO** — S3-compatible object storage for raw JSON files
* **Apache Airflow** — Pipeline orchestration and scheduling
* **Snowflake** — Cloud data warehouse
* **dbt** — SQL-based data transformation and modeling
* **Docker Compose** — Local infrastructure and service containerization
* **PostgreSQL** — Airflow metadata database

---

## Key Features

* Fetches live stock market quotes from the Finnhub API
* Publishes stock events to Kafka in JSON format
* Consumes Kafka events and stores them in MinIO
* Organizes raw data in a Bronze storage layer
* Uses Airflow to automate MinIO-to-Snowflake ingestion
* Loads raw JSON data into Snowflake
* Applies dbt transformations using Bronze, Silver, and Gold layers
* Uses environment variables to protect API keys, Snowflake credentials, and MinIO credentials
* Runs infrastructure services with Docker Compose

---

## Repository Structure

```text
RealTimeFinHub/
│
├── .env.example                  # Example environment variables (safe to share)
├── .gitignore                    # Files excluded from GitHub
├── requirements.txt              # Python dependencies
│
├── infra/
│   ├── docker-compose.yml        # Kafka, Zookeeper, MinIO, Airflow, PostgreSQL
│   │
│   ├── producer/
│   │   └── producer.py           # Fetches Finnhub data and publishes to Kafka
│   │
│   ├── consumer/
│   │   └── consumer.py           # Consumes Kafka messages and saves JSON to MinIO
│   │
│   ├── dags/
│   │   └── minio_to_snowflake.py # Airflow DAG: MinIO to Snowflake ingestion
│   │
│   ├── logs/                     # Airflow logs generated locally
│   ├── plugins/                  # Reserved for future Airflow custom plugins
│   └── docs/
│       └── minio_to_snowflake/   # Documentation or screenshots for the pipeline
│
├── dbt_stocks/
│   ├── dbt_project.yml           # dbt project configuration
│   ├── README.md                 # dbt project documentation
│   ├── models/
│   │   ├── bronze/
│   │   │   ├── bronze_stg_stock_quotes.sql
│   │   │   └── sources.yml
│   │   │
│   │   ├── silver/
│   │   │   └── silver_clean_stock_quotes.sql
│   │   │
│   │   └── gold/
│   │       ├── gold_kpi.sql
│   │       ├── gold_candlestick.sql
│   │       └── gold_treechart.sql
│   │
│   ├── analyses/                 # Optional dbt analyses
│   ├── macros/                   # Reusable dbt SQL macros
│   ├── seeds/                    # Optional static CSV data
│   ├── snapshots/                # Optional dbt snapshots
│   ├── tests/                    # dbt data quality tests
│   ├── logs/                     # Generated dbt logs
│   └── target/                   # Generated dbt compiled files
│
└── venv/                         # Local Python virtual environment
```



---

## Pipeline Steps

### 1. Data Extraction — Finnhub API

A Python producer retrieves live stock market quotes from the Finnhub API.

The API key is stored in the `.env` file and loaded through environment variables.

---

### 2. Real-Time Streaming — Apache Kafka

The producer publishes stock quote events to a Kafka topic.

Each event is sent in JSON format and can include fields such as:

```json
{
  "symbol": "AAPL",
  "c": 210.45,
  "d": 1.20,
  "dp": 0.57,
  "h": 211.10,
  "l": 208.30,
  "o": 209.00,
  "pc": 209.25,
  "t": 1710000000,
  "fetched_at": 1710000010
}
```

---

### 3. Raw Storage — MinIO

A Kafka consumer reads the messages from the Kafka topic and stores each event as a JSON file in a MinIO bucket.

Example storage structure:

```text
bronze-transactions/
├── AAPL/
│   ├── 1710000010.json
│   └── 1710000070.json
├── MSFT/
│   └── 1710000020.json
└── TSLA/
    └── 1710000030.json
```

---

### 4. Orchestration — Apache Airflow

Apache Airflow runs the `minio_to_snowflake` DAG.

The DAG performs two main tasks:

1. Downloads JSON files from the MinIO bucket.
2. Uploads the files to Snowflake and loads them into the Bronze table.

The workflow is scheduled to run automatically.

---

### 5. Data Warehouse — Snowflake

Snowflake stores the raw stock quote data in the Bronze layer.

Example Bronze table:

```sql
CREATE OR REPLACE TABLE BRONZE_STOCKS_RAW (
    RAW_DATA VARIANT,
    LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

The `RAW_DATA` column stores the original JSON event as a Snowflake `VARIANT`.

---

### 6. Data Transformations — dbt

dbt transforms the data across three layers.

#### Bronze Layer

The Bronze model extracts fields from the raw JSON data stored in Snowflake.

Examples of extracted fields:

* Symbol
* Current price
* Price change
* Percentage change
* Daily high and low
* Opening price
* Previous close
* Market timestamp
* Ingestion timestamp

#### Silver Layer

The Silver model cleans and validates the stock quote data.

Typical operations include:

* Filtering invalid or null prices
* Rounding financial values
* Standardizing column names
* Preparing structured records for analysis

#### Gold Layer

The Gold layer provides business-ready analytical models.

Available models include:

* Latest stock KPI by symbol
* Price change and percentage change
* Candlestick-style daily price metrics
* Stock trend and summary datasets

---

## Environment Variables

Create a `.env` file at the root of the project.

```env
# Finnhub API
API_KEYS=your_finnhub_api_key

# MinIO
MINIO_ACCESS_KEY=your_minio_access_key
MINIO_SECRET_KEY=your_minio_secret_key

# Snowflake
SNOWFLAKE_USER=your_snowflake_username
SNOWFLAKE_PASSWORD=your_snowflake_password
SNOWFLAKE_ACCOUNT=your_snowflake_account
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=STOCKS_MDS
SNOWFLAKE_SCHEMA=COMMON
```

Do not publish the real `.env` file on GitHub.

Instead, publish only `.env.example`.

---

## Getting Started

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd RealTimeFinHub
```

### 2. Create your environment file

```bash
copy .env.example .env
```

Then update `.env` with your own credentials.

### 3. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 4. Start the infrastructure

```bash
cd infra
docker compose up -d
```

### 5. Run the Kafka producer

```bash
cd producer
python producer.py
```

### 6. Run the Kafka consumer

```bash
cd consumer
python consumer.py
```

### 7. Access Airflow

Open:

```text
http://localhost:8080
```

Run the `minio_to_snowflake` DAG from the Airflow interface.

### 8. Run dbt transformations

```bash
cd dbt_stocks
dbt run
```

---

## Security Notes

The following files must remain private and should be ignored by Git:

```text
.env
profiles.yml
venv/
logs/
target/
dbt_packages/
__pycache__/
```

Use `.env.example` and `profiles.yml.example` to provide safe configuration templates for other users.

---

## Future Improvements

* Add dbt data quality tests
* Add incremental dbt models
* Add Airflow alerts and monitoring
* Add Kafka schema validation
* Add Snowflake loading history and deduplication
* Add a BI dashboard in a future version
* Deploy the pipeline to a cloud environment

---

## Author

**Chaimaa Amar**

Data Engineering Student
Big Data and Information Systems
#   R e a l - T i m e - S t o c k - M a r k e t - D a t a - P i p e l i n e - p r o j e c t 
 
 