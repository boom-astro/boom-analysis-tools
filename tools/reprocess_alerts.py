import argparse
import redis

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
    args = parser.parse_args()
    survey_name = args.survey_name
    reprocess_type = args.reprocess_type
    batch_size = args.batch_size

    redis = redis.Redis(host="localhost", port=6379, db=0)
    alerts_collection=fetch_mongo(f"{survey_name}_alerts")

    nb_alerts = alerts_collection.count_documents({})
    log(
        f"Reprocessing {nb_alerts} alerts from survey '{survey_name}' using '{reprocess_type}' pipeline."
    )

    log(f"Starting to push alerts to Redis queue in batches of {batch_size}...")

    batch = []
    count = 0
    redis_queue = get_redis_queue(survey_name, reprocess_type)
    for alert in alerts_collection.find():
        # For ZTF filter reprocessing, we need both programid and alert id
        if reprocess_type == "filter" and survey_name == "ZTF":
            batch.append(f"{alert['candidate']['programid']},{alert['_id']}")
        else:
            batch.append(alert["_id"])

        count += 1

        if len(batch) >= batch_size or count == nb_alerts:
            redis.lpush(redis_queue, *batch)
            log(f"{count}/{nb_alerts} alerts processed.")
            batch = []

    log(f"Finished reprocessing {nb_alerts} alerts.")