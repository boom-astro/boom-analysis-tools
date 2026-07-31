#!/usr/bin/env python3
"""Recover alerts by objectId from a remote boom API and insert them into the
local MongoDB.

Reads a text file containing one objectId per line (for example
`orphan_aux_object_ids.txt`: aux objects that have no alert on the local
instance), queries the remote boom `/queries/find` endpoint in batches using
`{"objectId": {"$in": [...]}}`, and upserts every recovered alert document
into the local `<survey>_alerts` collection (keyed on `_id`, so re-running is
idempotent and never duplicates).

One object can have many alerts, so each batch is paginated with a stable
`_id` sort and `skip` until the remote returns fewer docs than the page limit.

ObjectIds that no alert was found for are written to `<input>.missing.txt`.
An optional `--output` also dumps the recovered docs as JSONL.

Remote authentication: pass `--username`/`--password` (or set REMOTE_BOOM_USERNAME /
REMOTE_BOOM_PASSWORD), or pass an existing bearer token via `--token` / REMOTE_BOOM_TOKEN.

Local MongoDB: defaults mirror config.yaml (localhost:27017, db "boom", user
"mongoadmin", authSource=admin); password from BOOM_DATABASE__PASSWORD. Pass a
full `--mongo-uri` to override everything.

Example:
    REMOTE_BOOM_USERNAME=me REMOTE_BOOM_PASSWORD=secret BOOM_DATABASE__PASSWORD=dbpw \\
    python3 scripts/recover_alerts_by_object.py \\
        --input orphan_aux_object_ids.txt --survey ztf
"""

import argparse
import json
import os
import sys
import time
from urllib.parse import quote_plus

import requests
from bson.int64 import Int64
from pymongo import MongoClient, ReplaceOne

DEFAULT_BASE_URL = "https://api.kaboom.caltech.edu"
# STREAM_NAME in src/alert/<survey>.rs: collections are named "<STREAM>_alerts".
SURVEY_TO_COLLECTION = {"ztf": "ZTF_alerts", "lsst": "LSST_alerts"}


def get_token(session, base_url, username, password):
    """Authenticate with the boom API and return a bearer token."""
    response = session.post(
        f"{base_url}/auth",
        data={"username": username, "password": password},
        timeout=30,
    )
    if response.status_code != 200:
        raise SystemExit(
            f"authentication failed ({response.status_code}): {response.text}"
        )
    return response.json()["access_token"]


def post_find(session, base_url, token, catalog_name, filter_doc, limit, skip):
    """Run a single paginated /queries/find request with retry."""
    body = {
        "catalog_name": catalog_name,
        "filter": filter_doc,
        "limit": limit,
        "skip": skip,
        "sort": {"_id": 1},
        "max_time_ms": 120000,
    }
    headers = {"Authorization": f"Bearer {token}"}
    last_error = None
    for attempt in range(5):
        try:
            response = session.post(
                f"{base_url}/queries/find",
                json=body,
                headers=headers,
                timeout=180,
            )
        except requests.RequestException as error:
            last_error = error
            time.sleep(2 ** attempt)
            continue
        if response.status_code == 200:
            return response.json().get("data") or []
        if response.status_code in (429, 502, 503, 504):
            last_error = f"{response.status_code}: {response.text}"
            time.sleep(2 ** attempt)
            continue
        raise SystemExit(
            f"query failed ({response.status_code}): {response.text}"
        )
    raise SystemExit(f"query failed after retries: {last_error}")


def build_mongo_uri(args):
    """Build a MongoDB URI mirroring src/conf.rs build_db()."""
    if args.mongo_uri:
        return args.mongo_uri
    using_auth = bool(args.mongo_username) and bool(args.mongo_password)
    credentials = ""
    if using_auth:
        credentials = (
            f"{quote_plus(args.mongo_username)}:"
            f"{quote_plus(args.mongo_password)}@"
        )
    uri = (
        f"mongodb://{credentials}{args.mongo_host}:{args.mongo_port}/"
        f"{args.mongo_db}?directConnection=true"
    )
    if using_auth:
        uri += "&authSource=admin"
    return uri


def read_object_ids(path):
    """Read objectIds (one per line), ignoring blanks and duplicates."""
    object_ids = []
    seen = set()
    with open(path, "r") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            if line not in seen:
                seen.add(line)
                object_ids.append(line)
    return object_ids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="file with one objectId per line"
    )
    parser.add_argument("--survey", choices=sorted(SURVEY_TO_COLLECTION), default="ztf")
    parser.add_argument(
        "--remote-collection",
        help="remote MongoDB collection name (defaults from --survey)",
    )
    parser.add_argument(
        "--local-collection",
        help="local MongoDB collection name (defaults from --survey)",
    )
    parser.add_argument(
        "--output", help="optional JSONL dump of recovered alerts"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="fetch and report, but do not write to MongoDB",
    )
    # Remote API.
    parser.add_argument("--base-url", default=os.environ.get("BOOM_BASE_URL", DEFAULT_BASE_URL))
    parser.add_argument("--username", default=os.environ.get("REMOTE_BOOM_USERNAME"))
    parser.add_argument("--password", default=os.environ.get("REMOTE_BOOM_PASSWORD"))
    parser.add_argument("--token", default=os.environ.get("REMOTE_BOOM_TOKEN"))
    parser.add_argument(
        "--batch-size", type=int, default=200, help="objectIds per $in query"
    )
    parser.add_argument(
        "--page-limit",
        type=int,
        default=5000,
        help="max alerts fetched per paginated request",
    )
    # Local MongoDB (defaults mirror config.yaml).
    parser.add_argument("--mongo-uri", default=os.environ.get("BOOM_MONGO_URI"))
    parser.add_argument("--mongo-host", default="localhost")
    parser.add_argument("--mongo-port", type=int, default=27017)
    parser.add_argument("--mongo-db", default="boom")
    parser.add_argument("--mongo-username", default="mongoadmin")
    parser.add_argument(
        "--mongo-password", default=os.environ.get("BOOM_DATABASE__PASSWORD", "")
    )
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    remote_collection_name = args.remote_collection or SURVEY_TO_COLLECTION[args.survey]
    local_collection_name = args.local_collection or SURVEY_TO_COLLECTION[args.survey]

    object_ids = read_object_ids(args.input)
    print(f"read {len(object_ids)} unique objectIds from {args.input}")
    if not object_ids:
        return

    session = requests.Session()
    token = args.token
    if not token:
        if not args.username or not args.password:
            raise SystemExit(
                "provide --token, or --username/--password "
                "(or REMOTE_BOOM_TOKEN / REMOTE_BOOM_USERNAME / REMOTE_BOOM_PASSWORD)"
            )
        token = get_token(session, base_url, args.username, args.password)

    mongo_collection = None
    if not args.dry_run:
        mongo_client = MongoClient(build_mongo_uri(args))
        mongo_collection = mongo_client[args.mongo_db][local_collection_name]

    output_handle = open(args.output, "w") if args.output else None
    found_objects = set()
    recovered = 0
    inserted = 0

    try:
        for start in range(0, len(object_ids), args.batch_size):
            batch = object_ids[start : start + args.batch_size]
            filter_doc = {"objectId": {"$in": batch}}
            skip = 0
            while True:
                docs = post_find(
                    session,
                    base_url,
                    token,
                    remote_collection_name,
                    filter_doc,
                    limit=args.page_limit,
                    skip=skip,
                )
                operations = []
                for doc in docs:
                    recovered += 1
                    # candid must be stored as BSON Int64 so it matches the
                    # corresponding _id in <survey>_alerts_cutouts.
                    doc["_id"] = Int64(doc["_id"])
                    object_id = doc.get("objectId")
                    if object_id is not None:
                        found_objects.add(object_id)
                    if output_handle:
                        output_handle.write(json.dumps(doc, default=int) + "\n")
                    operations.append(
                        ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                    )
                if operations and mongo_collection is not None:
                    result = mongo_collection.bulk_write(
                        operations, ordered=False
                    )
                    inserted += result.upserted_count
                if len(docs) < args.page_limit:
                    break
                skip += args.page_limit
            done = min(start + args.batch_size, len(object_ids))
            print(
                f"  {done}/{len(object_ids)} objects queried, "
                f"{recovered} alerts recovered, {inserted} newly inserted",
                file=sys.stderr,
            )
    finally:
        if output_handle:
            output_handle.close()

    missing = [oid for oid in object_ids if oid not in found_objects]
    missing_path = args.input + ".missing.txt"
    with open(missing_path, "w") as handle:
        for object_id in missing:
            handle.write(f"{object_id}\n")

    action = "would insert" if args.dry_run else "inserted"
    print(
        f"done: {recovered} alerts recovered, {action} {inserted} new docs into "
        f"{args.mongo_db}.{local_collection_name}, "
        f"{len(missing)} objects with no alert found (listed in {missing_path})"
    )


if __name__ == "__main__":
    main()
