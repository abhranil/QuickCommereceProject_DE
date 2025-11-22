from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import subprocess
import os

# ==========================
# DAG CONFIGURATION
# ==========================

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": None,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    dag_id="full_event_pipeline",
    default_args=default_args,
    description="Run Status + GPS publisher/subscriber pipeline sequentially",
    schedule_interval=None,   # Trigger manually
    start_date=datetime(2025, 10, 25),
    catchup=False,
    tags=["pubsub", "gps", "status", "gcs", "pipeline"],
)
#start = EmptyOperator(task_id="start")
# ==========================
# HELPER FUNCTION
# ==========================

def run_script(script_name):
    """
    Run a Python script located in the same DAG directory.
    """
    script_path = os.path.join(os.path.dirname(__file__), script_name)
    print(f"🚀 Running script: {script_path}")

    result = subprocess.run(["python", script_path], capture_output=True, text=True)
    print(result.stdout)

    if result.returncode != 0:
        print(result.stderr)
        raise RuntimeError(f"❌ Script failed: {script_name}")

    print(f"✅ Successfully completed: {script_name}")

# ==========================
# TASK DEFINITIONS
# ==========================

# 1️⃣ Status Event Publisher
publish_status_events = PythonOperator(
    task_id="publish_status_events",
    python_callable=lambda: run_script("statusEvent_simulatorlimited.py"),
    dag=dag,
)

# 2️⃣ Status Event Subscriber
consume_status_events = PythonOperator(
    task_id="consume_status_events",
    python_callable=lambda: run_script("statusEventDataExtractiontoGCSLimited.py"),
    dag=dag,
)

# 3️⃣ GPS Event Publisher
publish_gps_events = PythonOperator(
    task_id="publish_gps_events",
    python_callable=lambda: run_script("gps_event_publisher.py"),
    dag=dag,
)

# 4️⃣ GPS Event Subscriber
consume_gps_events = PythonOperator(
    task_id="consume_gps_events",
    python_callable=lambda: run_script("gps_event_subscriber.py"),
    dag=dag,
)

# ==========================
# TASK DEPENDENCIES
# ==========================

#[publish_gps_events,consume_gps_events]
#start >> [publish_status_events, consume_status_events,publish_gps_events,consume_gps_events]

publish_gps_events >> consume_gps_events
publish_status_events >> consume_status_events 
