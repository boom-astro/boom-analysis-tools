import argparse
import redis

from astropy.time import Time
from utils.mongo import fetch_mongo
from utils.logger import log


def get_redis_queue(survey_name, process_type):
    """
    Get the name of a Boom Redis queue based on the survey name and process type.

    Args:
        survey_name (str): The survey name, e.g., "ZTF", "LSST", or "DECAM".
        process_type (str): The type of process, e.g., "packet", "enrichment+filter", or "filter".

    Returns:
        The name of the Redis queue.
    """
    if process_type== "enrichment+filter":
        process_type = "enrichment" # After enrichment, alerts go to the filter queue
    return f"{survey_name}_alerts_{process_type}_queue"


if __name__ == "__main__":
    # --- CLI arguments ---
    parser = argparse.ArgumentParser(
        description=(
            "Reprocess alerts in Boom by pushing them back to a Redis queue. "
            "You can choose to reprocess only the filtering step, "
            "or the enrichment and filtering steps."
        )
    )
    parser.add_argument(
        "--survey-name",
        type=str,
        choices=["ZTF", "LSST", "DECAM"],
        default="ZTF",
        help="The survey name (default: ZTF)."
    )
    parser.add_argument(
        "--created-after",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts created after the given Julian Date (default: all alerts)."
    )
    parser.add_argument(
        "--observed-after",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts observed after the given Julian Date (default: all alerts)."
    )
    parser.add_argument(
        "--reprocess-type",
        type=str,
        choices=["enrichment+filter", "filter"],
        default="filter",
        help="The type of reprocessing to perform (default: filter)."
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="The number of alerts to push to Redis in each batch (default: 10000)."
    )
    parser.add_argument(
        "--mongo-uri",
        type=str,
        default="mongodb://localhost:27017",
        help="The MongoDB connection URI (default: mongodb://localhost:27017)."
    )
    parser.add_argument(
        "--redis-host",
        type=str,
        default="localhost",
        help="The Redis server host (default: localhost)."
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="The Redis server port (default: 6379)."
    )
    args = parser.parse_args()
    survey_name = args.survey_name
    created_after = args.created_after
    observed_after = args.observed_after
    reprocess_type = args.reprocess_type
    batch_size = args.batch_size
    mongo_uri = args.mongo_uri
    redis_host = args.redis_host
    redis_port = args.redis_port

    alerts_collection = fetch_mongo(f"{survey_name}_alerts", url=mongo_uri)
    # Build query filter based on period
    query_filter = {}
    if created_after or observed_after:
        if created_after:
            query_filter["created_at"] = {"$gte": created_after}
            log(f"Filtering alerts created after JD {created_after} ({Time(created_after, format='jd').iso[:19]})")
        if observed_after:
            query_filter["candidate.jd"] = {"$gte": observed_after}
            log(f"Filtering alerts observed after JD {observed_after} ({Time(observed_after, format='jd').iso[:19]})")
            nb_alerts = alerts_collection.count_documents(query_filter)
        else:
            log(
                "Since filtering is performed only on created_at (and not on candidate.jd, which is indexed), "
                "the count and find queries may be very slow to run. Therefore, the count step is skipped."
            )
            nb_alerts = "unknown"
    else:
        nb_alerts = alerts_collection.estimated_document_count()
    if nb_alerts == 0:
        log("No alerts found matching the criteria. Exiting.")
        exit(0)

    log(
        f"Reprocessing {nb_alerts} alerts from survey '{survey_name}' using '{reprocess_type}' pipeline."
    )

    log(f"Starting to push alerts to Redis queue in batches of {batch_size}...")

    batch = []
    count = 0
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    redis_queue = get_redis_queue(survey_name, reprocess_type)
    for alert in alerts_collection.find(query_filter):
        # For ZTF filter reprocessing, we need both programid and alert id
        if reprocess_type == "filter" and survey_name == "ZTF":
            batch.append(f"{alert['candidate']['programid']},{alert['_id']}")
        else:
            batch.append(alert["_id"])

        count += 1

        if len(batch) >= batch_size or count == nb_alerts:
            redis_client.lpush(redis_queue, *batch)
            log(f"{count}/{nb_alerts} alerts processed.")
            batch = []

    log(f"Finished reprocessing {nb_alerts} alerts.")