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
annotation_origin = os.getenv("ANNOTATION_ORIGIN")
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

def consume():
    skyportal = SkyPortal(instance=skyportal_url, token=skyportal_api_key)

    log("Listening for messages...")
    while True:
        try:
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
                    skyportal.save_to_groups(
                        record.get("objectId"),
                        filter_to_group_map[filter.get("filter_id")]
                    )

                    annotations = json.loads(filter.get("annotations"))
                    data={}
                    for band, photstat in annotations.get("photstats", {}).items():
                        data[f"{band}_band_peak_jd"] = photstat.get("peak_jd")
                        data[f"{band}_band_peak_mag"] = photstat.get("peak_mag")
                        if photstat.get("fading") is not None:
                            data[f"{band}_band_fading_rate"] = photstat.get("fading").get("rate")
                        if photstat.get("rising") is not None:
                            data[f"{band}_band_rising_rate"] = photstat.get("rising").get("rate")

                    skyportal.add_annotation(
                        record.get("objectId"),
                        [filter_to_group_map[filter.get("filter_id")]],
                        annotation_origin,
                        data
                    )

                    time.sleep(0.2)  # To avoid hitting rate limits

        except Exception as e:
            log(e)
        except KeyboardInterrupt:
            break

    consumer.close()

if __name__ == "__main__":
    consume()
