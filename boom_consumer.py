import os
import time
import json
from datetime import datetime, timezone

from dotenv import load_dotenv
from utils.avro import read_avro
from confluent_kafka import Consumer
from utils.logger import log

load_dotenv()

thumbnail_types = [
    ("cutoutScience", "new"),
    ("cutoutTemplate", "ref"),
    ("cutoutDifference", "sub"),
]

# To determine the filter passed, check the filter_name in the alert's filters field
# Example filter names used in BOOM:
# - fast_transient_ztf
# - fast_transient_lsst
# - galactic_fast_transient_ztf
# - crossmatch_ztf_lsst

config = {
    'bootstrap.servers': os.getenv("BOOM_KAFKA_SERVERS"),
    'group.id': f'umn_boom_kafka_consumer_group_{int(time.time())}',
    'auto.offset.reset': 'earliest',
    "enable.auto.commit": False,
}
if os.getenv("BOOM_KAFKA_USERNAME") and os.getenv("BOOM_KAFKA_PASSWORD"):
    config.update({
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": os.getenv("BOOM_KAFKA_USERNAME"),
        "sasl.password": os.getenv("BOOM_KAFKA_PASSWORD"),
    })
else:
    config["security.protocol"] = "PLAINTEXT"
consumer = Consumer(config)

topic = "ZTF_alerts_results"
consumer.subscribe([topic])
log(f"Subscribed to topic: {topic}")


def consume():
    log("Listening for messages...")
    last_kafka_date = None
    count = 0
    count_by_filter = {}
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                log(f"Processed {count} messages.{f' Last message timestamp: {last_kafka_date.isoformat()}' if last_kafka_date else ''}")
                log(f"No {'more ' if count_by_filter else ''}messages available")
                break
            if msg.error():
                log(f"Consumer error: {msg.error()}")
                continue

            _, ts_ms = msg.timestamp()
            kafka_date = datetime.fromtimestamp(ts_ms // 1000, tz=timezone.utc)
            if last_kafka_date is None or kafka_date > last_kafka_date:
                last_kafka_date = kafka_date

            if count and count % 1000 == 0:
                log(f"Processed {count} messages. Message timestamp: {kafka_date.isoformat()}")
            count += 1

            record = read_avro(msg)

            # Remove cutouts to improve readability, you can remove this block to keep them
            for cutout_type, _ in thumbnail_types:
                del record[cutout_type]

            # Save the first alert to a JSON file for inspection of its structure
            if count == 1:
                with open("first_alert.json", "w") as f:
                    json.dump(record, f, indent=2)

            # Extract filter name for counting
            for boom_filter in record.get("filters", []):
                count_by_filter[boom_filter["filter_name"]] = count_by_filter.get(boom_filter["filter_name"], 0) + 1

    except KeyboardInterrupt:
        pass
    finally:
        log(count_by_filter)
        consumer.close()

if __name__ == "__main__":
    consume()