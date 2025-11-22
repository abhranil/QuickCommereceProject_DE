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
SUBSCRIPTION_ID = "StatusEvent-sub"
BUCKET_NAME = "dataproc-staging-asia-south1-925894589695-qxkvzrhv"
LOCAL_DIR = "/tmp/status_data"
BATCH_SIZE = 50              # messages per CSV
UPLOAD_INTERVAL = 30         # seconds between GCS uploads
GCS_PREFIX = "StatusEventData/"


# ===== Logging Setup =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def consume_and_upload_status_events(run_duration=120):
    """
    Subscribes to Pub/Sub messages for a limited time window and uploads batches to GCS.
    """
    logger.info(f"🚀 Starting Pub/Sub consumer for {run_duration} seconds...")

    # Create required dirs & clients
    os.makedirs(LOCAL_DIR, exist_ok=True)
    subscriber = pubsub_v1.SubscriberClient()
    storage_client = storage.Client()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    # Buffer for storing messages
    buffer = []
    lock = threading.Lock()

    def flush_to_gcs():
        """Flush buffered messages to CSV and upload to GCS."""
        nonlocal buffer
        with lock:
            if not buffer:
                return

            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            csv_file = f"{LOCAL_DIR}/status_{timestamp}.csv"
            df = pd.DataFrame(buffer)

            # Keep consistent column order
            columns = ["order_id", "courier_id", "status", "ts"]
            df = df[[c for c in columns if c in df.columns]]
            df.to_csv(csv_file, index=False)
            logger.info(f"💾 Saved {len(buffer)} records → {csv_file}")

            # Upload to GCS
            bucket = storage_client.bucket(BUCKET_NAME)
            blob_name = f"{GCS_PREFIX}status_{timestamp}.csv"
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
            logger.info(f"✅ Received: {data}")
        except Exception as e:
            logger.warning(f"⚠️ Failed to process message: {e}")
            message.nack()

    # Subscribe
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    logger.info(f"📡 Listening to: {subscription_path}")

    # Periodic flushing thread
    def periodic_flush():
        while not stop_event.is_set():
            time.sleep(UPLOAD_INTERVAL)
            flush_to_gcs()

    stop_event = threading.Event()
    flush_thread = threading.Thread(target=periodic_flush, daemon=True)
    flush_thread.start()

    # Run for a limited duration
    try:
        time.sleep(run_duration)
    except KeyboardInterrupt:
        logger.info("🛑 Interrupted manually.")
    finally:
        stop_event.set()
        flush_thread.join(timeout=5)
        streaming_pull_future.cancel()
        flush_to_gcs()
        logger.info("✅ Consumer stopped and remaining data uploaded.")


# Standalone testing entrypoint
if __name__ == "__main__":
    consume_and_upload_status_events(run_duration=120)
