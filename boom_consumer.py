import time
import json

from confluent_kafka import Consumer
from utils.avro import read_avro

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
# - galactic_fast_transient_lsst
# - crossmatch_ztf_ned
# - crossmatch_lsst_ned
# - crossmatch_ztf_lsst
# - crossmatch_lsst_ztf

consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': f'umn_boom_kafka_consumer_group_{int(time.time())}',
    'auto.offset.reset': 'earliest',
    "enable.auto.commit": False,  # Disable auto-commit of offsets
    "session.timeout.ms": 6000,  # Session timeout for the consumer
    "max.poll.interval.ms": 300000,  # Maximum time between polls
    "security.protocol": "PLAINTEXT",  # Use PLAINTEXT if no authentication
})
topic = 'LSST_alerts_results'
topic = 'ZTF_alerts_results'
consumer.subscribe([topic])
print(f"Subscribed to topic: {topic}")


def consume():
    print("Listening for messages...")
    alerts = []
    try:
        while True:
            msg = consumer.poll(timeout=10.0)
            if msg is None:
                print(f"No {'more ' if alerts else ''}messages available, exiting")
                break
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            record = read_avro(msg)

            # Remove cutouts to improve readability, you can remove this block to keep them
            for cutout_type, _ in thumbnail_types:
                del record[cutout_type]

            # Save the first alert to a JSON file for inspection of its structure
            if len(alerts) == 0:
                with open("first_alert.json", "w") as f:
                    json.dump(record, f, indent=2)

            alerts.append(record)

    except KeyboardInterrupt:
        pass
    finally:
        print(f"Processed {len(alerts)} messages")
        consumer.close()

if __name__ == "__main__":
    consume()