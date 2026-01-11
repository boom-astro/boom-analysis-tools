import argparse
import sys
import uuid

from datetime import datetime, timezone
from utils.mongo import fetch_mongo
from utils.logger import log

def now_jd() -> float:
    """Return the current date as Julian Date."""
    return datetime.now(timezone.utc).timestamp() / 86400.0 + 2440587.5


def add_filter(survey: str, name: str, filter_file: str):
    with open(filter_file, "r") as f:
        filter_pipeline = f.read()

    filter_id = str(uuid.uuid4())
    now = now_jd()

    filter_doc = {
        "_id": filter_id,
        "name": name,
        "active": True,
        "user_id": "cli",
        "survey": survey,
        "permissions": {
            "ZTF": [1, 2, 3],
        },
        "fv": [
            {
                "fid": "v2e0fs",
                "pipeline": filter_pipeline,
                "created_at": now,
            }
        ],
        "active_fid": "v2e0fs",
        "created_at": now,
        "updated_at": now,
    }

    collection = fetch_mongo("filters")
    try:
        collection.insert_one(filter_doc)
        log(f"Successfully inserted filter {name} from file {filter_file}")
    except Exception as e:
        log(f"Error inserting filter: {e}")
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add a filter to the Boom database")
    parser.add_argument("survey", help="Survey to add a filter for (ZTF, LSST or DECAM).")
    parser.add_argument("name", help="Name of the filter to be added.")
    parser.add_argument("filter_file", help="Path to the JSON file containing the filter")
    args = parser.parse_args()

    add_filter(args.survey, args.name, args.filter_file)