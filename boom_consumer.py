import os
import time
import json

from dotenv import load_dotenv
from utils.avro import read_avro
from confluent_kafka import Consumer

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
print(f"Subscribed to topic: {topic}")


def consume():
    print("Listening for messages...")
    alerts = []
    count = 0
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                print(f"No {'more ' if alerts else ''}messages available, exiting")
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue

            if count % 1000 == 0:
                print(f"Processed {count} messages")
            count += 1

            record = read_avro(msg)

            # Remove cutouts to improve readability, you can remove this block to keep them
            for cutout_type, _ in thumbnail_types:
                del record[cutout_type]

            # Save the first alert to a JSON file for inspection of its structure
            if count == 1:
                with open("first_alert.json", "w") as f:
                    json.dump(record, f, indent=2)

            alerts.append(record)

    except KeyboardInterrupt:
        pass
    finally:
        print(f"Processed {count} messages")
        consumer.close()

if __name__ == "__main__":
    consume()