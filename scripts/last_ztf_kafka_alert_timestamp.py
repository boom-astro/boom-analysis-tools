#!/usr/bin/env python3
"""Print the timestamp of the last alert produced to the ZTF Kafka topics
that boom consumes for a given night.

Connects to the ZTF brokers given by `--brokers` (default: BOOM_KAFKA_SERVERS,
see .env), discovers every partition of
`ztf_<YYYYMMDD>_programid<N>` for the requested night and program ids,
reads the last message of each partition by seeking to (high_watermark - 1)
and reports the most recent Kafka message timestamp.

Example:
    python3 scripts/last_ztf_kafka_alert_timestamp.py --night 2026-05-21
    python3 scripts/last_ztf_kafka_alert_timestamp.py --night 20260521 \\
        --program-ids 1,2,3
"""

import argparse
import datetime as dt
import os
import sys

from confluent_kafka import Consumer, TopicPartition

DEFAULT_BROKERS = os.environ.get("BOOM_KAFKA_SERVERS", "localhost:9092")
DEFAULT_PROGRAM_IDS = "1,2,3"


def parse_night(value: str) -> dt.date:
    for fmt in ("%Y-%m-%d", "%Y%m%d"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(
        f"Invalid night '{value}': expected YYYY-MM-DD or YYYYMMDD"
    )


def parse_program_ids(value: str) -> list[int]:
    try:
        ids = [int(p.strip()) for p in value.split(",") if p.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid program ids '{value}'") from exc
    if not ids:
        raise argparse.ArgumentTypeError("At least one program id is required")
    return ids


def last_timestamp_for_topic(consumer: Consumer, topic: str, poll_timeout: float):
    """Return (datetime, partition, offset) for the latest message in `topic`,
    or None if the topic does not exist or has no messages."""
    metadata = consumer.list_topics(topic=topic, timeout=10.0)
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None or topic_meta.error is not None or not topic_meta.partitions:
        return None

    latest = None  # (timestamp_ms, partition_id, offset)
    for partition_id in topic_meta.partitions:
        tp = TopicPartition(topic, partition_id)
        low, high = consumer.get_watermark_offsets(tp, timeout=10.0, cached=False)
        if high <= low:
            continue
        tp.offset = high - 1
        consumer.assign([tp])
        msg = consumer.poll(timeout=poll_timeout)
        if msg is None or msg.error() is not None:
            continue
        ts_type, ts_ms = msg.timestamp()
        if ts_type == 0:  # TIMESTAMP_NOT_AVAILABLE
            continue
        if latest is None or ts_ms > latest[0]:
            latest = (ts_ms, partition_id, msg.offset())

    if latest is None:
        return None
    ts_ms, partition_id, offset = latest
    return (
        dt.datetime.fromtimestamp(ts_ms / 1000, tz=dt.timezone.utc),
        partition_id,
        offset,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--night",
        type=parse_night,
        required=True,
        help="Observing night (YYYY-MM-DD or YYYYMMDD)",
    )
    parser.add_argument(
        "--brokers",
        default=DEFAULT_BROKERS,
        help=f"Kafka bootstrap servers (default: {DEFAULT_BROKERS})",
    )
    parser.add_argument(
        "--program-ids",
        type=parse_program_ids,
        default=parse_program_ids(DEFAULT_PROGRAM_IDS),
        help="Comma-separated ZTF program ids to inspect (default: 1,2,3)",
    )
    parser.add_argument(
        "--poll-timeout",
        type=float,
        default=10.0,
        help="Seconds to wait for the last message of each partition (default: 10)",
    )
    args = parser.parse_args()

    consumer = Consumer(
        {
            "bootstrap.servers": args.brokers,
            "group.id": "boom-last-alert-timestamp-inspector",
            "enable.auto.commit": False,
            "auto.offset.reset": "latest",
        }
    )

    night_str = args.night.strftime("%Y%m%d")
    overall_latest = None
    try:
        for program_id in args.program_ids:
            topic = f"ztf_{night_str}_programid{program_id}"
            result = last_timestamp_for_topic(consumer, topic, args.poll_timeout)
            if result is None:
                print(f"{topic}: no messages (topic missing or empty)")
                continue
            ts, partition_id, offset = result
            print(
                f"{topic}: last alert at {ts.isoformat()} "
                f"(partition={partition_id}, offset={offset})"
            )
            if overall_latest is None or ts > overall_latest:
                overall_latest = ts
    finally:
        consumer.close()

    if overall_latest is None:
        print("No alerts found for this night.")
        return 1
    print(f"\nLatest alert across all topics: {overall_latest.isoformat()}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
