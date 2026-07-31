#!/bin/bash
set -euo pipefail

# --- Period to dump (YYYY-MM-DD, UTC; DATE_END is exclusive) ---
DATE_START="${1:-2026-03-08}"
DATE_END="${2:-2026-04-08}"
MAX_ALERTS_PER_NIGHT=1000
SURVEYS=("ZTF") # e.g. ("ZTF" "LSST")

DB="boom"
HOST="localhost"
PORT="27017"
USERNAME="mongoadmin"
PASSWORD="${BOOM_DATABASE__PASSWORD:?'Variable BOOM_DATABASE__PASSWORD must be set'}"
AUTH_ARGS="--host $HOST --port $PORT --username $USERNAME --password $PASSWORD --authenticationDatabase admin"
MONGOSH_AUTH="mongodb://$USERNAME:$PASSWORD@$HOST:$PORT/$DB?authSource=admin&directConnection=true"

# Julian Date of a YYYY-MM-DD date at 00:00:00 UTC
to_jd() {
    local EPOCH
    # GNU date (Linux), then BSD date (macOS)
    EPOCH=$(date -u -d "$1 00:00:00 UTC" +%s 2>/dev/null) \
        || EPOCH=$(date -j -u -f "%Y-%m-%d %H:%M:%S" "$1 00:00:00" +%s 2>/dev/null) \
        || { echo "Invalid date: $1" >&2; return 1; }
    awk -v e="$EPOCH" 'BEGIN { printf "%.5f", e / 86400.0 + 2440587.5 }'
}

JD_MIN=$(to_jd "$DATE_START")
JD_MAX=$(to_jd "$DATE_END")

OUT_DIR="/tmp/mongodump_between_${DATE_START}_and_${DATE_END}"
mkdir -p "$OUT_DIR"

echo "=== MongoDB dump between $DATE_START and $DATE_END ==="
echo "JD range: [$JD_MIN, $JD_MAX)"
echo "Output directory: $OUT_DIR"
echo ""

# Dump a single survey (ZTF or LSST)
dump_survey() {
    local SURVEY="$1"
    local SURVEY_DIR="$OUT_DIR/$SURVEY"
    mkdir -p "$SURVEY_DIR"

    echo "--- [$SURVEY] Extracting IDs (max $MAX_ALERTS_PER_NIGHT alerts per night) ---"
    # Iterate over each night (integer JD boundary = noon UTC), limited per night
    mongosh "$MONGOSH_AUTH" --quiet --eval "
        const jdMin = $JD_MIN;
        const jdMax = $JD_MAX;
        const maxPerNight = $MAX_ALERTS_PER_NIGHT;
        let allCandids = [];
        let allObjectIds = new Set();
        for (let jd = Math.floor(jdMin); jd < jdMax; jd++) {
            const from = Math.max(jd, jdMin);
            const to = Math.min(jd + 1, jdMax);
            const nightIds = db.${SURVEY}_alerts
                .find({\"candidate.jd\": {\$gte: from, \$lt: to}}, {_id: 1, objectId: 1})
                .limit(maxPerNight)
                .toArray();
            if (nightIds.length > 0) {
                print('JD ' + from + '-' + to + ': ' + nightIds.length + ' alerts');
                nightIds.forEach(d => { allCandids.push(d._id); allObjectIds.add(d.objectId); });
            }
        }
        const objectIds = [...allObjectIds];
        const candidStr = allCandids.map(id => '{\"\$numberLong\":\"' + id.toString() + '\"}').join(',');
        const candidQuery = '{\"_id\": {\"\$in\": [' + candidStr + ']}}';
        fs.writeFileSync('$SURVEY_DIR/alerts_query.json', candidQuery);
        fs.writeFileSync('$SURVEY_DIR/cutouts_query.json', candidQuery);
        fs.writeFileSync('$SURVEY_DIR/aux_query.json', '{\"_id\": {\"\$in\": ' + JSON.stringify(objectIds) + '}}');
        print('Total alerts: ' + allCandids.length);
        print('Unique objects: ' + objectIds.length);
        fs.writeFileSync('$SURVEY_DIR/n_alerts.txt', String(allCandids.length));
    "

    local N_ALERTS
    N_ALERTS=$(cat "$SURVEY_DIR/n_alerts.txt")

    if [ "$N_ALERTS" -eq 0 ]; then
        echo "[$SURVEY] No alerts found for this period, skipping cutouts and objects."
        return
    fi

    echo "--- [$SURVEY] Dumping alerts ($N_ALERTS documents) ---"
    mongodump $AUTH_ARGS \
        --db "$DB" \
        --collection "${SURVEY}_alerts" \
        --queryFile "$SURVEY_DIR/alerts_query.json" \
        --out "$SURVEY_DIR/alerts"

    echo "--- [$SURVEY] Dumping cutouts ($N_ALERTS documents) ---"
    mongodump $AUTH_ARGS \
        --db "$DB" \
        --collection "${SURVEY}_alerts_cutouts" \
        --queryFile "$SURVEY_DIR/cutouts_query.json" \
        --out "$SURVEY_DIR/cutouts"

    echo "--- [$SURVEY] Dumping objects (alerts_aux) ---"
    mongodump $AUTH_ARGS \
        --db "$DB" \
        --collection "${SURVEY}_alerts_aux" \
        --queryFile "$SURVEY_DIR/aux_query.json" \
        --out "$SURVEY_DIR/aux"

    # Cleanup temporary files
    rm -f "$SURVEY_DIR/alerts_query.json" "$SURVEY_DIR/cutouts_query.json" "$SURVEY_DIR/aux_query.json" "$SURVEY_DIR/n_alerts.txt"

    echo "[$SURVEY] Dump complete."
    echo ""
}

# Run dumps
for SURVEY in "${SURVEYS[@]}"; do
    dump_survey "$SURVEY"
done

# Summary
echo "=== Dump complete ==="
echo "Output: $OUT_DIR"
du -sh "$OUT_DIR"
