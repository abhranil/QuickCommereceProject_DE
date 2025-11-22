from google.cloud import pubsub_v1, storage
import json
import pandas as pd
import os
from datetime import datetime
import time
import threading
import logging
import sys

# ===== CONFIGURATION =====
PROJECT_ID = "flawless-agency-474210-p4"
SUBSCRIPTION_ID = "GPSEvent-sub"
BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"
LOCAL_DIR = "/tmp/gps_data"
UPLOAD_INTERVAL = 10          # seconds between flushes
RUN_DURATION = 30             # total runtime in seconds
GCS_PREFIX = "GPSEventData/"

# ===== Logging Setup =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

def consume_and_upload_gps_events():
    """
    Subscribes to Pub/Sub GPS messages for a limited time window and uploads data batches to GCS.
    """
    logger.info(f"🚀 Starting GPS Pub/Sub consumer for {RUN_DURATION} seconds...")

    # Prepare local directory & clients
    os.makedirs(LOCAL_DIR, exist_ok=True)
    subscriber = pubsub_v1.SubscriberClient()
    storage_client = storage.Client()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    buffer = []
    lock = threading.Lock()

    def flush_to_gcs():
        """Flush buffered GPS messages to CSV and upload to GCS."""
        nonlocal buffer
        with lock:
            if not buffer:
                return

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            csv_file = f"{LOCAL_DIR}/gps_{timestamp}.csv"
            df = pd.DataFrame(buffer)
            df.to_csv(csv_file, index=False)
            logger.info(f"💾 Saved {len(buffer)} records → {csv_file}")

            # Upload to GCS
            bucket = storage_client.bucket(BUCKET_NAME)
            blob_name = f"{GCS_PREFIX}gps_{timestamp}.csv"
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(csv_file)
            logger.info(f"☁️ Uploaded to: gs://{BUCKET_NAME}/{blob_name}")

            # Cleanup
            buffer = []
            os.remove(csv_file)

    def callback(message):
        """Handle incoming Pub/Sub messages."""
        nonlocal buffer
        try:
            data = json.loads(message.data.decode("utf-8"))
            with lock:
                buffer.append(data)
            message.ack()
            logger.info(f"✅ Received message: {data}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to process message: {e}")
            message.nack()

    # Subscribe to Pub/Sub topic
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"📡 Listening to subscription: {subscription_path}")

    # Background thread for periodic GCS flushes
    stop_event = threading.Event()

    def periodic_flush():
        while not stop_event.is_set():
            time.sleep(UPLOAD_INTERVAL)
            flush_to_gcs()

    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    # Run for limited duration
    try:
        time.sleep(RUN_DURATION)
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted manually.")
    finally:
        stop_event.set()
        flush_thread.join(timeout=5)
        streaming_pull_future.cancel()
        flush_to_gcs()
        logger.info("✅ GPS Consumer stopped and remaining data uploaded.")

if __name__ == "__main__":
    consume_and_upload_gps_events()
