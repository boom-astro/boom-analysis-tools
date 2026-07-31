#!/usr/bin/env python3
"""Verify that every recovered alert is present in the photometry of its object.

`recover_alerts_by_candid.py` only upserts documents into `ZTF_alerts`; it does
not touch `ZTF_alerts_aux`. This script checks, for every candid listed in
`orphan_cutout_ids.txt`, that:

  1. the alert document now exists in `ZTF_alerts` (keyed on `_id == candid`),
  2. the alert's object has an aux document in `ZTF_alerts_aux`
     (keyed on `_id == objectId`),
  3. that candid appears in the object's `prv_candidates` photometry
     (some `prv_candidates` entry has `candid == <this candid>`).

Everything is hardcoded on purpose: this is a one-shot, read-only check, run by
hand. It connects with the read-only MongoDB user; the password is read from
READONLY_BOOM_DATABASE__PASSWORD (see .env), or pass a full BOOM_MONGO_URI to
override the whole connection string.

Results are written to three plain-text files (one candid per line):

  verify_recovered_photometry.alert_missing.txt
      candids whose alert is still absent from ZTF_alerts
  verify_recovered_photometry.aux_missing.txt
      candids whose object has no ZTF_alerts_aux document
  verify_recovered_photometry.photometry_missing.txt
      candids whose alert and aux both exist, but the candid is NOT in the
      object's prv_candidates photometry

If all three files are empty, every recovered alert is correctly reflected in
its object's photometry.
"""

import os
import sys
from urllib.parse import quote_plus

from pymongo import MongoClient

# --- Hardcoded configuration ------------------------------------------------

MONGO_USERNAME = "readonly"
MONGO_HOST = "localhost"
MONGO_PORT = 27017
DATABASE_NAME = "boom"
ALERT_COLLECTION = "ZTF_alerts"
ALERT_AUX_COLLECTION = "ZTF_alerts_aux"

INPUT_PATH = "orphan_cutout_ids.txt"
ALERT_MISSING_PATH = "verify_recovered_photometry.alert_missing.txt"
AUX_MISSING_PATH = "verify_recovered_photometry.aux_missing.txt"
PHOTOMETRY_MISSING_PATH = "verify_recovered_photometry.photometry_missing.txt"

# Candids per `$in` query (alerts lookup) and objectIds per `$in` query (aux).
ALERT_BATCH_SIZE = 5000
AUX_BATCH_SIZE = 2000


def build_mongo_uri():
    """Build the read-only MongoDB URI, taking the password from the env."""
    uri = os.environ.get("BOOM_MONGO_URI")
    if uri:
        return uri
    password = os.environ.get("READONLY_BOOM_DATABASE__PASSWORD")
    if not password:
        raise SystemExit(
            "set READONLY_BOOM_DATABASE__PASSWORD (see .env), "
            "or pass a full BOOM_MONGO_URI"
        )
    return (
        f"mongodb://{MONGO_USERNAME}:{quote_plus(password)}@"
        f"{MONGO_HOST}:{MONGO_PORT}/{DATABASE_NAME}"
        "?directConnection=true&authSource=boom"
    )


def read_candids(path):
    """Read candids (one integer per line), ignoring blanks and duplicates."""
    candids = []
    seen = set()
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


def write_lines(path, values):
    """Write one value per line to path."""
    with open(path, "w") as handle:
        for value in values:
            handle.write(f"{value}\n")


def main():
    candids = read_candids(INPUT_PATH)
    print(f"read {len(candids)} unique candids from {INPUT_PATH}")
    if not candids:
        return

    client = MongoClient(build_mongo_uri())
    database = client[DATABASE_NAME]
    alerts = database[ALERT_COLLECTION]
    alerts_aux = database[ALERT_AUX_COLLECTION]

    # --- Step 1: resolve each candid to its objectId via ZTF_alerts ---------
    # candid -> objectId for alerts that exist locally.
    candid_to_object = {}
    for start in range(0, len(candids), ALERT_BATCH_SIZE):
        batch = candids[start : start + ALERT_BATCH_SIZE]
        cursor = alerts.find(
            {"_id": {"$in": batch}},
            projection={"_id": 1, "objectId": 1},
        )
        for doc in cursor:
            candid_to_object[int(doc["_id"])] = doc.get("objectId")
        done = min(start + ALERT_BATCH_SIZE, len(candids))
        print(
            f"  alerts resolved: {done}/{len(candids)} candids scanned, "
            f"{len(candid_to_object)} alerts found",
            file=sys.stderr,
        )

    alert_missing = [c for c in candids if c not in candid_to_object]

    # Group the resolvable candids by objectId so each aux doc is fetched once.
    object_to_candids = {}
    for candid, object_id in candid_to_object.items():
        if object_id is None:
            # Alert exists but has no objectId: cannot live in any aux doc.
            alert_missing.append(candid)
            continue
        object_to_candids.setdefault(object_id, set()).add(candid)

    object_ids = list(object_to_candids)
    print(
        f"{len(candid_to_object)} alerts found, "
        f"spanning {len(object_ids)} objects; "
        f"{len(alert_missing)} candids have no usable alert",
        file=sys.stderr,
    )

    # --- Step 2: for each object, check candids against prv_candidates -------
    aux_missing = []
    photometry_missing = []
    objects_checked = 0

    for start in range(0, len(object_ids), AUX_BATCH_SIZE):
        batch = object_ids[start : start + AUX_BATCH_SIZE]
        cursor = alerts_aux.find(
            {"_id": {"$in": batch}},
            projection={"_id": 1, "prv_candidates.candid": 1},
        )
        seen_objects = set()
        for doc in cursor:
            object_id = doc["_id"]
            seen_objects.add(object_id)
            expected = object_to_candids.get(object_id, set())
            present = {
                entry["candid"]
                for entry in doc.get("prv_candidates", [])
                if entry.get("candid") is not None
            }
            for candid in expected:
                if candid not in present:
                    photometry_missing.append(candid)

        # Objects in this batch with no aux document at all.
        for object_id in batch:
            if object_id not in seen_objects:
                aux_missing.extend(sorted(object_to_candids[object_id]))

        objects_checked = min(start + AUX_BATCH_SIZE, len(object_ids))
        print(
            f"  aux checked: {objects_checked}/{len(object_ids)} objects, "
            f"{len(photometry_missing)} candids missing from photometry, "
            f"{len(aux_missing)} candids with no aux doc",
            file=sys.stderr,
        )

    # --- Step 3: report -----------------------------------------------------
    alert_missing.sort()
    aux_missing.sort()
    photometry_missing.sort()

    write_lines(ALERT_MISSING_PATH, alert_missing)
    write_lines(AUX_MISSING_PATH, aux_missing)
    write_lines(PHOTOMETRY_MISSING_PATH, photometry_missing)

    total = len(candids)
    ok = total - len(alert_missing) - len(aux_missing) - len(photometry_missing)
    print()
    print(f"checked {total} candids from {INPUT_PATH}")
    print(f"  OK (alert + aux + in prv_candidates): {ok}")
    print(
        f"  alert still missing from {ALERT_COLLECTION}: "
        f"{len(alert_missing)} -> {ALERT_MISSING_PATH}"
    )
    print(
        f"  object has no {ALERT_AUX_COLLECTION} doc: "
        f"{len(aux_missing)} -> {AUX_MISSING_PATH}"
    )
    print(
        f"  candid NOT in object prv_candidates: "
        f"{len(photometry_missing)} -> {PHOTOMETRY_MISSING_PATH}"
    )
    if ok == total:
        print("all recovered alerts are present in their object photometry")


if __name__ == "__main__":
    main()
