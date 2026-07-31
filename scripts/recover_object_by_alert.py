#!/usr/bin/env python3
"""Recover object aux docs by alert candid from a remote boom API.

`recover_alerts_by_candid.py` only restores `<survey>_alerts`; it never
rebuilds the per-object photometry in `<survey>_alerts_aux`.
`verify_recovered_photometry.py` produced two lists of candids that are still
not reflected in their object's `prv_candidates`:

  verify_recovered_photometry.aux_missing.txt        (object has no aux doc)
  verify_recovered_photometry.photometry_missing.txt (candid not in prv_candidates)

This script reads those candids, resolves each to its objectId via the LOCAL
`<survey>_alerts`, fetches the authoritative `<survey>_alerts_aux` document for
every such object from the remote boom `/queries/find` endpoint in batches
using `{"_id": {"$in": [...]}}`, and upserts every recovered aux document into
the local aux collection (ReplaceOne keyed on the string `_id == objectId`, so
re-running is idempotent and the whole remote doc replaces the local one).

ObjectIds that no aux doc was found for are written to `<first input>.not_on_remote.txt`.
An optional `--output` also dumps the recovered docs as JSONL.

Remote authentication: pass `--username`/`--password` (or set REMOTE_BOOM_USERNAME /
REMOTE_BOOM_PASSWORD), or pass an existing bearer token via `--token` / REMOTE_BOOM_TOKEN.

Local MongoDB: defaults mirror config.yaml (localhost:27017, db "boom", user
"mongoadmin", authSource=admin); password from BOOM_DATABASE__PASSWORD. Pass a
full `--mongo-uri` to override everything. The local target collection defaults
to the real `<survey>_alerts_aux`; pass `--local-collection test_ZTF_alerts_aux`
to dry-test against a throwaway collection first.

Example:
    REMOTE_BOOM_USERNAME=me REMOTE_BOOM_PASSWORD=secret BOOM_DATABASE__PASSWORD=dbpw \\
    python3 scripts/recover_object_by_alert.py \\
        --survey ztf --local-collection test_ZTF_alerts_aux
"""

import argparse
import json
import os
import sys
import time
from urllib.parse import quote_plus

import requests
from pymongo import MongoClient, ReplaceOne

DEFAULT_BASE_URL = "https://api.kaboom.caltech.edu"
# STREAM_NAME in src/alert/<survey>.rs: collections are "<STREAM>_alerts" and
# "<STREAM>_alerts_aux".
SURVEY_TO_ALERT_COLLECTION = {"ztf": "ZTF_alerts", "lsst": "LSST_alerts"}
SURVEY_TO_AUX_COLLECTION = {"ztf": "ZTF_alerts_aux", "lsst": "LSST_alerts_aux"}

DEFAULT_INPUTS = [
    "verify_recovered_photometry.aux_missing.txt",
    "verify_recovered_photometry.photometry_missing.txt",
]


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


def post_find(session, base_url, token, catalog_name, filter_doc, limit):
    """Run a single /queries/find request with retry on transient errors."""
    body = {
        "catalog_name": catalog_name,
        "filter": filter_doc,
        "limit": limit,
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
        # 429 and 5xx are worth retrying, the rest are fatal.
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


def read_candids(paths):
    """Read candids (one integer per line) from several files, deduplicated."""
    candids = []
    seen = set()
    for path in paths:
        if not os.path.exists(path):
            print(f"warning: input file {path} not found, skipping", file=sys.stderr)
            continue
        with open(path, "r") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                value = int(line)
                if value not in seen:
                    seen.add(value)
                    candids.append(value)
    return candids


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        nargs="+",
        default=DEFAULT_INPUTS,
        help="file(s) with one candid per line (defaults to the two "
        "verify_recovered_photometry outputs)",
    )
    parser.add_argument("--survey", choices=sorted(SURVEY_TO_AUX_COLLECTION), default="ztf")
    parser.add_argument(
        "--remote-collection",
        help="remote aux collection name (defaults from --survey)",
    )
    parser.add_argument(
        "--local-collection",
        help="local aux collection to upsert into (defaults from --survey; "
        "pass test_ZTF_alerts_aux for a dry test)",
    )
    parser.add_argument(
        "--local-alert-collection",
        help="local alerts collection used to map candid->objectId "
        "(defaults from --survey)",
    )
    parser.add_argument(
        "--output", help="optional JSONL dump of recovered aux docs"
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
        "--batch-size",
        type=int,
        default=200,
        help="objectIds per remote $in query (aux docs carry the full "
        "lightcurve, so keep this modest)",
    )
    parser.add_argument(
        "--alert-batch-size",
        type=int,
        default=5000,
        help="candids per local alerts $in lookup",
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
    remote_collection_name = args.remote_collection or SURVEY_TO_AUX_COLLECTION[args.survey]
    local_collection_name = args.local_collection or SURVEY_TO_AUX_COLLECTION[args.survey]
    local_alert_collection_name = (
        args.local_alert_collection or SURVEY_TO_ALERT_COLLECTION[args.survey]
    )

    candids = read_candids(args.input)
    print(f"read {len(candids)} unique candids from {', '.join(args.input)}")
    if not candids:
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

    mongo_client = MongoClient(build_mongo_uri(args))
    database = mongo_client[args.mongo_db]
    alerts = database[local_alert_collection_name]
    target_aux = None
    if not args.dry_run:
        target_aux = database[local_collection_name]

    # --- Step 1: map each candid to its objectId via local alerts -----------
    object_ids = []
    seen_objects = set()
    resolved = 0
    for start in range(0, len(candids), args.alert_batch_size):
        batch = candids[start : start + args.alert_batch_size]
        cursor = alerts.find(
            {"_id": {"$in": batch}},
            projection={"_id": 1, "objectId": 1},
        )
        for doc in cursor:
            resolved += 1
            object_id = doc.get("objectId")
            if object_id is not None and object_id not in seen_objects:
                seen_objects.add(object_id)
                object_ids.append(object_id)
        done = min(start + args.alert_batch_size, len(candids))
        print(
            f"  candid->objectId: {done}/{len(candids)} scanned, "
            f"{resolved} alerts resolved, {len(object_ids)} distinct objects",
            file=sys.stderr,
        )

    if resolved != len(candids):
        print(
            f"note: {len(candids) - resolved} candids had no local alert "
            f"in {local_alert_collection_name} (cannot be mapped to an "
            f"object); continuing with the rest",
            file=sys.stderr,
        )
    print(f"{len(object_ids)} distinct objects to recover", file=sys.stderr)

    # --- Step 2: fetch remote aux docs and upsert into the local target -----
    output_handle = open(args.output, "w") if args.output else None
    found_on_remote = set()
    recovered = 0
    inserted = 0
    replaced = 0

    try:
        for start in range(0, len(object_ids), args.batch_size):
            batch = object_ids[start : start + args.batch_size]
            docs = post_find(
                session,
                base_url,
                token,
                remote_collection_name,
                {"_id": {"$in": batch}},
                limit=len(batch),
            )
            operations = []
            for doc in docs:
                recovered += 1
                # aux _id is the objectId (string): keep it as-is, no Int64 cast.
                found_on_remote.add(doc["_id"])
                if output_handle:
                    output_handle.write(json.dumps(doc, default=int) + "\n")
                operations.append(
                    ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                )
            if operations and target_aux is not None:
                result = target_aux.bulk_write(operations, ordered=False)
                inserted += result.upserted_count
                replaced += result.modified_count
            done = min(start + args.batch_size, len(object_ids))
            print(
                f"  aux recovered: {done}/{len(object_ids)} objects queried, "
                f"{recovered} aux docs recovered, "
                f"{inserted} inserted, {replaced} replaced",
                file=sys.stderr,
            )
    finally:
        if output_handle:
            output_handle.close()

    not_on_remote = sorted(
        oid for oid in object_ids if oid not in found_on_remote
    )
    not_on_remote_path = args.input[0] + ".not_on_remote.txt"
    with open(not_on_remote_path, "w") as handle:
        for object_id in not_on_remote:
            handle.write(f"{object_id}\n")

    action = "would upsert" if args.dry_run else "upserted"
    print(
        f"done: {recovered} aux docs recovered, {action} "
        f"({inserted} new + {replaced} replaced) into "
        f"{args.mongo_db}.{local_collection_name}, "
        f"{len(not_on_remote)} objects with no aux on remote "
        f"(listed in {not_on_remote_path})"
    )


if __name__ == "__main__":
    main()