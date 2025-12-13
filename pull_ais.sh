#!/bin/bash
set -euo pipefail

# =============================================================================
# pull_ais.sh – MarineTraffic exportvesseltrack API (JSON protocol) – 2025-12-12
# Fetches latest observed AIS position per MMSI in SAR bbox for the previous completed hour (H-3 → H-2)
# Uses protocol:json → native array of objects (no CSV parsing required)
# Outputs ais_YYYYMMDDThhZ.jsonl exactly per Section 4.3 spec
# =============================================================================

source conf.env || { echo "Error: conf.env not found. Copy conf.env.example and edit."; exit 1; }
cd "$(dirname "$0")"

# Time window: previous completed hour (H-3 start → H-2 end)
HOUR_START_UTC=$(date -u -d '3 hours ago' +"%Y-%m-%d %H:00:00")
HOUR_END_UTC=$(date -u -d '2 hours ago' +"%Y-%m-%d %H:00:00")
FILE_TS=$(date -u -d '3 hours ago' +"%Y%m%dT%H")Z

OUTPUT_FILE="${CURRENT_DIR}/ais_${FILE_TS}.jsonl"
LOG_FILE="${LOGS_DIR}/ais.log"

mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] ais: $1" | tee -a "$LOG_FILE"; }

# Build API URL (protocol:json for native JSON array)
API_URL="https://services.marinetraffic.com/api/exportvesseltrack/${MARINETRAFFIC_APIKEY}"
API_URL="${API_URL}/protocol:json/minutes:60/msgtype:simple"
API_URL="${API_URL}/fromdate:${HOUR_START_UTC// /%20}/todate:${HOUR_END_UTC// /%20}"
API_URL="${API_URL}/minlat:${LAT_MIN}/maxlat:${LAT_MAX}/minlon:${LON_MIN}/maxlon:${LON_MAX}"

log "Fetching AIS (JSON) – window ${HOUR_START_UTC} → ${HOUR_END_UTC}"

RESPONSE=$(curl -sf --max-time 120 --retry 3 "$API_URL") || {
    log "curl failed or timed out"
    exit 1
}

if echo "$RESPONSE" | grep -q "^ERROR CODE"; then
    log "MarineTraffic API error: $RESPONSE"
    exit 1
fi

echo "$RESPONSE" | \
jq -c '.[] | {
    mmsi: .MMSI | tonumber,
    lat: .LAT | tonumber,
    lon: .LON | tonumber,
    timestamp: (.TIMESTAMP + "Z"),
    speed_kts: (.SPEED | tonumber / 10),
    course_deg: .COURSE | tonumber,
    heading_deg: .HEADING | tonumber
}' > "$OUTPUT_FILE"

VESSEL_COUNT=$(wc -l < "$OUTPUT_FILE")
log "Success: ${VESSEL_COUNT} vessels written to $OUTPUT_FILE"

echo "$(date -u +%Y-%m-%dT%H:%M:%SZ) ${FILE_TS} ${VESSEL_COUNT}" >> "${LOGS_DIR}/ais_completed.log"

exit 0
