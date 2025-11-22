from google.cloud import pubsub_v1
import json
import time
import random
import logging
from datetime import datetime, timezone
import sys

# ===== GCP SETTINGS =====
PROJECT_ID = "flawless-agency-474210-p4"
TOPIC_ID = "GPSEvent"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# ===== Function to simulate random GPS data =====
def generate_gps_event(courier_id):
    lat_base, lon_base = 12.9716, 77.5946  # Bengaluru coordinates
    lat = lat_base + random.uniform(-0.01, 0.01)
    lon = lon_base + random.uniform(-0.01, 0.01)
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "courier_id": courier_id,
        "lat": round(lat, 6),
        "lon": round(lon, 6),
        "ts": ts
    }

# ===== Main Function =====
def run_gps_publisher():
    """Publish GPS events for 10 seconds."""

    logger.info(f"🚀 Starting Pub/Sub publishing for 10 seconds...")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    
    courier_ids = [
        "C100", "C101", "C102", "C103", "C104", "C105", "C106",
        "C107", "C108", "C109", "C110", "C111", "C112", "C113",
        "C114", "C115", "C116", "C117", "C118"
    ]

    start_time = time.time()
    published_count = 0
    try:
        
         while time.time() - start_time < 10:  # run for 10 seconds
            for courier_id in courier_ids:
                event = generate_gps_event(courier_id)
                data_str = json.dumps(event)
                data_bytes = data_str.encode("utf-8")
                publisher.publish(topic_path, data=data_bytes)
                published_count += 1
                logger.info(f"✅ Published: {json.dumps(event)}")
                sys.stdout.flush()
                time.sleep(0.5)

    except Exception as e:
        logger.error(f"❌ Error during publishing: {e}", exc_info=True)
    finally:
        logger.info(f"🛑 Finished publishing {published_count} messages after 10 seconds.")
        sys.stdout.flush()

if __name__ == "__main__":
    run_gps_publisher()
