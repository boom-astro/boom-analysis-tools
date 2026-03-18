import json
import os
import argparse

from dotenv import load_dotenv
from utils.mongo import fetch_mongo

load_dotenv()


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
        "--mongo-uri",
        type=str,
        default=os.getenv("BOOM_MONGO_URI", "mongodb://localhost:27017"),
        help="The MongoDB connection URI can be set via the BOOM_MONGO_URI environment variable (default: mongodb://localhost:27017)."
    )
    args = parser.parse_args()
    mongo_uri = args.mongo_uri

    # Get the ztf lyon filter json from filters folder
    with open("../filters/lyon_ztf.json", 'r') as file:
        lyon_ztf = json.load(file)

    alerts_aux_collection = fetch_mongo("ZTF_alerts_aux", url=mongo_uri)
    alerts_collection = fetch_mongo(f"ZTF_alerts", url=mongo_uri)

    for alert in alerts_collection.aggregate(lyon_ztf):
        object_id = alert["objectId"]
        obj = alerts_aux_collection.find_one({"_id": str(object_id)})
        print(obj)
        break