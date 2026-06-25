import os
import boto3
import snowflake.connector

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


# ============================================================
# Environment variables passed by Docker Compose
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET = "bronze-transactions"
LOCAL_DIR = "/tmp/minio_downloads"

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")

SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DATABASE", "STOCKS_MDS")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "COMMON")


# ============================================================
# Validation: makes configuration errors easy to understand
# ============================================================

required_variables = {
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "SNOWFLAKE_USER": SNOWFLAKE_USER,
    "SNOWFLAKE_PASSWORD": SNOWFLAKE_PASSWORD,
    "SNOWFLAKE_ACCOUNT": SNOWFLAKE_ACCOUNT,
}

missing_variables = [
    name for name, value in required_variables.items()
    if not value
]

if missing_variables:
    raise ValueError(
        "Missing environment variables in Airflow container: "
        + ", ".join(missing_variables)
    )


# ============================================================
# Task 1: Download JSON files from MinIO
# ============================================================

def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
    )

    objects = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])

    if not objects:
        print(f"No files found in MinIO bucket: {BUCKET}")
        return []

    local_files = []

    for obj in objects:
        key = obj["Key"]

        # Keeps the folder structure, e.g. AAPL/123.json
        local_file = os.path.join(LOCAL_DIR, key)

        os.makedirs(os.path.dirname(local_file), exist_ok=True)

        s3.download_file(BUCKET, key, local_file)

        print(f"Downloaded: {key} -> {local_file}")

        local_files.append(local_file)

    return local_files


# ============================================================
# Task 2: Load downloaded files into Snowflake
# ============================================================

def load_to_snowflake(**context):
    local_files = context["ti"].xcom_pull(task_ids="download_minio")

    if not local_files:
        print("No files to load into Snowflake.")
        return

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )

    cur = conn.cursor()

    try:
        for file_path in local_files:
            cur.execute(
                f"PUT file://{file_path} @%BRONZE_STOCKS_RAW AUTO_COMPRESS=TRUE"
            )

            print(f"Uploaded to Snowflake stage: {file_path}")

        cur.execute("""
            COPY INTO BRONZE_STOCKS_RAW (RAW_DATA)
            FROM (
                SELECT $1
                FROM @%BRONZE_STOCKS_RAW
            )
            FILE_FORMAT = (TYPE = JSON)
        """)

        print("Snowflake COPY INTO completed successfully.")

    finally:
        cur.close()
        conn.close()


# ============================================================
# Airflow DAG
# ============================================================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 6, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="minio_to_snowflake",
    default_args=default_args,
    schedule="*/1 * * * *",
    catchup=False,
    tags=["minio", "snowflake", "stocks"],
) as dag:

    download_minio = PythonOperator(
        task_id="download_minio",
        python_callable=download_from_minio,
    )

    load_snowflake = PythonOperator(
        task_id="load_snowflake",
        python_callable=load_to_snowflake,
    )

    download_minio >> load_snowflake