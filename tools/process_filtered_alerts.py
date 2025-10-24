import os
import time

from confluent_kafka import Consumer

from tools.api import SkyPortal
from utils import read_avro, log
from dotenv import load_dotenv

load_dotenv()

skyportal_url = os.getenv("SKYPORTAL_URL")
skyportal_api_key = os.getenv("SKYPORTAL_API_KEY")
filter_id = os.getenv("FILTER_ID")
topic = os.getenv("TOPIC")
group_ids_to_save_source_to =[int(x) for x in os.getenv("GROUP_IDS_TO_SAVE_SOURCE_TO").split(',')]

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

            # Check if the alert passed the specified filter and save to groups if so
            for filter in record.get("filters", []):
                if filter.get("filter_id") == filter_id:
                    skyportal.save_to_groups(
                        record.get("objectId"),
                        group_ids_to_save_source_to,
                    )
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume()
