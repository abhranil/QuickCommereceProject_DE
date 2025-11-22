from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator

# ---- Configuration ----
PROJECT_ID = "flawless-agency-474210-p4"
REGION = "asia-south1"
CLUSTER_NAME = "quick-commerce"  # existing Dataproc cluster name
PYSPARK_URI = "gs://dataproc-staging-asia-south1-925894589695-qxkvzrhv/pyspark/orders_transformation_bq.py"  # your PySpark script
#TEMP_BUCKET = "quickcommerce-temp-bucket"  # temp bucket for BQ writes (optional)

# ---- Default DAG Arguments ----
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email": ["abhra.nil007@gmail.com"],
    "retries": None,
    "retry_delay": timedelta(minutes=5),
}

# ---- DAG Definition ----
with DAG(
    "order_transformation_dataproc_dag",
    default_args=default_args,
    description="Run PySpark order transformation on Dataproc and load to BigQuery",
    schedule_interval=None,  # or set cron like "0 2 * * *"
    start_date=datetime(2025, 10, 1),
    catchup=False,
    tags=["dataproc", "pyspark", "etl", "bigquery"],
) as dag:

    # ---- Dataproc Job Configuration ----
    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": PYSPARK_URI,
            # Optional: Add arguments for your script if needed
            # "args": ["--some_param", "value"],
            "jar_file_uris": [
                "gs://hadoop-lib/bigquery/bigquery-connector-hadoop3-latest.jar"
            ],
        },
    }

    # ---- Task to Submit PySpark Job ----
    run_pyspark_job = DataprocSubmitJobOperator(
        task_id="run_order_transformation_job",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    run_pyspark_job
