import os
import argparse
import redis

from dotenv import load_dotenv
from astropy.time import Time
from utils.mongo import fetch_mongo
from utils.logger import log, YELLOW, ENDC

load_dotenv()

def get_redis_queue(survey, process_type):
    """
    Get the name of a Boom Redis queue based on the survey name and process type.

    Args:
        survey (str): The survey name, e.g., "ZTF", "LSST", or "DECAM".
        process_type (str): The type of process, e.g., "packet", "enrichment+filter", or "filter".

    Returns:
        The name of the Redis queue.
    """
    if process_type== "enrichment+filter":
        process_type = "enrichment" # After enrichment, alerts go to the filter queue
    return f"{survey}_alerts_{process_type}_queue"


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
        "--survey",
        type=str,
        choices=["ZTF", "LSST", "DECAM"],
        default="ZTF",
        help="The survey name (default: ZTF)."
    )
    parser.add_argument(
        "--candid",
        type=int,
        default=None,
        help="Reprocess a single alert by its candid."
    )
    parser.add_argument(
        "--created-after",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts created after the given Julian Date."
    )
    parser.add_argument(
        "--created-before",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts between created-after and created-before."
    )
    parser.add_argument(
        "--observed-after",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts observed after the given Julian Date."
    )
    parser.add_argument(
        "--observed-before",
        type=float,
        metavar="JD",
        default=None,
        help="Reprocess alerts between observed-after and observed-before."
    )
    parser.add_argument(
        "--reprocess-type",
        type=str,
        choices=["enrichment+filter", "filter"],
        default="enrichment+filter",
        help="The type of reprocessing to perform (default: enrichment+filter)."
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
        default=os.getenv("BOOM_MONGO_URI", "mongodb://localhost:27017"),
        help="The MongoDB connection URI can be set via the BOOM_MONGO_URI environment variable (default: mongodb://localhost:27017)."
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
    survey = args.survey
    created_after = args.created_after
    created_before = args.created_before
    observed_after = args.observed_after
    observed_before = args.observed_before
    reprocess_type = args.reprocess_type
    batch_size = args.batch_size
    mongo_uri = args.mongo_uri
    redis_host = args.redis_host
    redis_port = args.redis_port

    alerts_collection = fetch_mongo(f"{survey}_alerts", url=mongo_uri)
    redis_client = redis.Redis(host=redis_host, port=redis_port, db=0)
    redis_queue = get_redis_queue(survey, reprocess_type)

    # --- Single alert reprocessing by candid ---
    if args.candid:
        alert = alerts_collection.find_one({"_id": args.candid})
        if not alert:
            log(f"{YELLOW}No alert found with candid {args.candid}.{ENDC}")
            exit(1)
        if reprocess_type == "filter" and survey == "ZTF":
            value = f"{alert['candidate']['programid']},{alert['_id']}"
        else:
            value = alert["_id"]
        redis_client.lpush(redis_queue, value)
        log(f"Pushed alert {alert['_id']} (candid {args.candid}) to {redis_queue}.")
        exit(0)

    # --- Bulk reprocessing ---
    # Build query filter based on period
    query_filter = {}
    if created_before and not created_after:
        log(f"{YELLOW}Warning: created-before can be set only if created-after is also set.{ENDC}")
        exit(1)
    if observed_before and not observed_after:
        log(f"{YELLOW}Warning: observed-before can be set only if observed-after is also set.{ENDC}")
        exit(1)
    if created_after or observed_after:
        if created_after:
            query_filter["created_at"] = {"$gte": created_after}
            if created_before:
                query_filter["created_at"]["$lte"] = created_before
                log(f"Filtering alerts created between JD {created_after} ({Time(created_after, format='jd').iso[:19]}) and JD {created_before} ({Time(created_before, format='jd').iso[:19]})")
            else:
                log(f"Filtering alerts created after JD {created_after} ({Time(created_after, format='jd').iso[:19]})")

        if observed_after:
            query_filter["candidate.jd"] = {"$gte": observed_after}
            if observed_before:
                query_filter["candidate.jd"]["$lte"] = observed_before
                log(f"Filtering alerts observed between JD {observed_after} ({Time(observed_after, format='jd').iso[:19]}) and JD {observed_before} ({Time(observed_before, format='jd').iso[:19]})")
            else:
                log(f"Filtering alerts observed after JD {observed_after} ({Time(observed_after, format='jd').iso[:19]})")
            nb_alerts = alerts_collection.count_documents(query_filter)
        else:
            try:
                nb_alerts = alerts_collection.count_documents(
                    query_filter,
                    maxTimeMS=6000  # 6 secondes timeout
                )
            except Exception as e:
                log(
                    f"{YELLOW}Since filtering is performed only on created_at (and not on candidate.jd, which is indexed), "
                    f"the count and find queries may be very slow to run. Therefore, the count step was aborted after 6 seconds."
                )
                nb_alerts = "X"
    else:
        nb_alerts = alerts_collection.estimated_document_count()
    if nb_alerts == 0:
        log("No alerts found matching the criteria. Exiting.")
        exit(0)

    log(
        f"Reprocessing {nb_alerts} alerts from survey '{survey}' using '{reprocess_type}' pipeline."
    )

    log(f"Starting to push alerts to Redis queue in batches of {batch_size}...")

    batch = []
    count = 0
    for alert in alerts_collection.find(query_filter):
        # For ZTF filter reprocessing, we need both programid and alert id
        if reprocess_type == "filter" and survey == "ZTF":
            batch.append(f"{alert['candidate']['programid']},{alert['_id']}")
        else:
            batch.append(alert["_id"])

        count += 1

        if len(batch) >= batch_size or count == nb_alerts:
            redis_client.lpush(redis_queue, *batch)
            log(f"{count}/{nb_alerts} alerts processed.")
            batch = []

    if batch:
        redis_client.lpush(redis_queue, *batch)
        log(f"{count}/{nb_alerts} alerts processed.")

    log(f"Finished reprocessing {nb_alerts} alerts.")