import os
import time
import json

from confluent_kafka import Consumer
from api import SkyPortal
from utils import read_avro, log
from dotenv import load_dotenv

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")

topic = os.getenv("TOPIC")
filter_to_group_map = json.loads(os.getenv("FILTER_TO_GROUP_MAP"))

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'umn_boom_kafka_consumer_group_{int(time.time())}',
    'auto.offset.reset': 'earliest',
    "enable.auto.commit": False,  # Disable auto-commit of offsets
    "session.timeout.ms": 6000,  # Session timeout for the consumer
    "max.poll.interval.ms": 300000,  # Maximum time between polls
    "security.protocol": "PLAINTEXT",  # Use PLAINTEXT if no authentication
})
consumer.subscribe([topic])

RED = "\033[31m"
GREEN = "\033[32m"
YELLOW = "\033[33m"
ENDC = "\033[0m"

def consume():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)

    log("Listening for messages...")
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                time.sleep(5)
                continue
            if msg.error():
                log(f"Consumer error: {msg.error()}")
                continue
            record = read_avro(msg)

            # Check if the alert passed any of the filters in the map
            for filter in record.get("filters", []):
                if filter.get("filter_id") in filter_to_group_map:
                    response = skyportal.save_to_groups(
                        record.get("objectId"),
                        filter_to_group_map[filter.get("filter_id")]
                    )
                    if response.get("status") == "success":
                        log(f"{GREEN}Object {record.get('objectId')} saved.{ENDC}")
                    elif response.get("message").startswith("Source already saved"):
                        log(f"{YELLOW}Object {record.get('objectId')} already saved.{ENDC}")
                    else:
                        log(f"{RED}Error saving object {record.get('objectId')}: {response.get('message')}{ENDC}")

                time.sleep(0.5)  # To avoid hitting rate limits

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume()
