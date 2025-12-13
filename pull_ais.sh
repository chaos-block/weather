#!/bin/bash
set -euo pipefail

source .env
cd "$(dirname "$0")"
HOUR_UTC=$(date -u -d '3 hours ago' +%Y%m%dT%H)Z
OUTPUT_FILE="${CURRENT_DIR}/ais_${HOUR_UTC}.jsonl"
mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log() { echo "[$(date -u)] ais: $1" | tee -a "${LOGS_DIR}/ais.log"; }

# GraphQL query for HVP (adapt from https://servicedocs-sm.kpler.com/messages-api/)
QUERY='
{
  historicalVesselPoints(
    input: {
      mmsi: null,
      startTime: "'${HOUR_UTC:0:13}:00:00Z'",
      endTime: "'${HOUR_UTC:0:13}:59:59Z'",
      boundingBox: { minLat: '$LAT_MIN', maxLat: '$LAT_MAX', minLon: '$LON_MIN', maxLon: '$LON_MAX' }
    }
  ) {
    points { mmsi lat lon timestamp speed course heading }
  }
}'

RESPONSE=$(curl -s -X POST "$KPLER_BASE" \
  -H "Authorization: Bearer $KPLER_TOKEN" \
  -H "Content-Type: application/json" \
  -d "{\"query\":\"$QUERY\"}" | jq -r '.data.historicalVesselPoints.points[]')

echo "$RESPONSE" | sort -k1,1n -k4,4nr |  # Sort by MMSI asc, timestamp desc (latest first)
awk 'NR==1 {print} /"mmsi": '"$(echo $prev_mmsi)"'/ {next} {print; prev_mmsi=$1}' |  # Dedup
jq -c > "$OUTPUT_FILE"

log "AIS positions written: $OUTPUT_FILE ($(wc -l < "$OUTPUT_FILE") lines)"
