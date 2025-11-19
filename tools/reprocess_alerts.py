import argparse
import redis

from tools.utils import fetch_mongo, log


def get_redis_queue(survey_name, queue_type):
    """
    Get the name of a Boom Redis queue based on the survey name and queue type.

    Args:
        survey_name (str): The survey name, e.g., "ZTF", "LSST", or "DECAM".
        queue_type (str): The type of queue, e.g., "packet", "enrichment", or "filter".

    Returns:
        The name of the Redis queue.
    """
    return f"{survey_name}_alerts_{queue_type}_queue"


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
        default="enrichment+filter",
        help="The type of reprocessing to perform (default: enrichment+filter)."
    )
    args = parser.parse_args()
    survey_name = args.survey_name
    reprocess_type = args.reprocess_type

    redis = redis.Redis(host="localhost", port=6379, db=0)
    alerts_collection=fetch_mongo(f"{survey_name}_alerts")

    log(
        f"Reprocessing {alerts_collection.count_documents({})} alerts from survey '{survey_name}' using '{reprocess_type}' pipeline."
    )
    for alert in alerts_collection.find():
        redis.lpush(get_redis_queue(survey_name, reprocess_type), alert["_id"])