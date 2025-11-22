from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# === Import functions from your Python files ===
from newExtractOrderData import export_orders_to_gcs         # MySQL extraction
from extractFirestoreData import export_inventory_to_gcs  # Firestore extraction

# === Toggle which tasks to run (for testing) ===
RUN_MYSQL = False         # Set True if you want to run MySQL export
RUN_FIRESTORE = True      # Set True if you want to run Firestore export

# === Default arguments for DAG ===
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=1),
}

# === Define the DAG ===
with DAG(
    dag_id='run_data_exports_dag',
    default_args=default_args,
    description='Export data from MySQL and Firestore to GCS',
    schedule_interval=None,          # Manual trigger
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['mysql', 'firestore', 'export'],
) as dag:

    # ====== Task 1: Export Orders from MySQL ======
    if RUN_MYSQL:
        export_orders_task = PythonOperator(
            task_id='export_orders_to_gcs',
            python_callable=export_orders_to_gcs,
        )

    # ====== Task 2: Export Inventory from Firestore ======
    if RUN_FIRESTORE:
        export_inventory_task = PythonOperator(
            task_id='export_inventory_to_gcs',
            python_callable=export_inventory_to_gcs,
        )

    # ====== Set Task Dependencies ======
    if RUN_MYSQL and RUN_FIRESTORE:
        export_orders_task >> export_inventory_task
    elif RUN_MYSQL:
        export_orders_task
    elif RUN_FIRESTORE:
        export_inventory_task