from google.cloud import pubsub_v1
import json
import time
import random
from datetime import datetime, timezone
import logging
import sys

# ===== GCP SETTINGS =====
PROJECT_ID = "flawless-agency-474210-p4"
TOPIC_ID = "StatusEvent"

# ===== Logging Setup =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

# ===== Possible order statuses =====
ORDER_STATUSES = ["picked_up", "in_transit", "delivered", "cancelled"]

def generate_status_event(order_id, courier_id):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    status = random.choice(ORDER_STATUSES)
    return {
        "order_id": order_id,
        "courier_id": courier_id,
        "status": status,
        "ts": ts,
    }

def publish_status_events(duration_seconds=10):
    """Publish random order status events to Pub/Sub for a limited time."""
    logger.info(f"🚀 Starting Pub/Sub publishing for {duration_seconds} seconds...")

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    courier_ids = [
        "C100", "C101", "C102", "C103", "C104", "C105",
        "C106", "C107", "C108", "C109", "C110", "C111",
        "C112", "C113", "C114", "C115", "C116", "C117", "C118"
    ]

    start_time = time.time()
    published_count = 0

    try:
        while time.time() - start_time < duration_seconds:
            for i, courier_id in enumerate(courier_ids):
                if time.time() - start_time >= duration_seconds:
                    break

                event = generate_status_event(1000 + i, courier_id)
                data_bytes = json.dumps(event).encode("utf-8")

                publisher.publish(topic_path, data=data_bytes)
                published_count += 1
                logger.info(f"✅ Published: {json.dumps(event)}")
                sys.stdout.flush()
                time.sleep(0.5)
    except Exception as e:
        logger.error(f"❌ Error during publishing: {e}", exc_info=True)
    finally:
        logger.info(f"🛑 Finished publishing {published_count} messages after {duration_seconds} seconds.")
        sys.stdout.flush()

if __name__ == "__main__":
    publish_status_events(10)
