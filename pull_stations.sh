#!/bin/bash
set -euo pipefail

source .env
cd "$(dirname "$0")"
HOUR_UTC=$(date -u -d '3 hours ago' +%Y%m%dT%H)Z  # H-3
OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}.jsonl"
mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log() { echo "[$(date -u)] stations: $1" | tee -a "${LOGS_DIR}/stations.log"; }

# Build JSONL from stations CSV (env var STATIONS_CSV)
echo "$STATIONS_CSV" | tail -n +2 | while IFS=, read -r station_id name lat lon source fields; do
    case $source in
        NOAA)
            # CO-OPS API: tides/waves/winds (e.g., https://api.tidesandcurrents.noaa.gov/api/prod/datagetter)
            URL="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&product=water_level&datum=MLLW&units=english&time_zone=gmt&application=web_services&format=json&interval=hilo&begin_date=${HOUR_UTC:0:10}&end_date=${HOUR_UTC:0:10}"
            [ -n "$NOAA_TOKEN" ] && URL="${URL}&token=${NOAA_TOKEN}"
            DATA=$(curl -s "$URL" | jq -r '.data[0] // empty')
            if [ -n "$DATA" ]; then
                tide_height=$(echo "$DATA" | jq -r '.v // null')
                # Similar for tide_speed, dir, vis, cloud, wave_ht, wind_spd/dir (map fields)
                # Moon/sunrise/sunset computed via astropy or fixed Pacific times
                echo "{\"station_id\":\"$station_id\",\"timestamp\":\"${HOUR_UTC:0:13}:00:00Z\",\"tide_height_ft\":$tide_height,...}" >> "$OUTPUT_FILE"  # Expand fields
            fi
            ;;
        SMN)
            # SMN API: winds/vis/cloud (e.g., https://smn.conagua.gob.mx/api/v1/observations/station/${station_id})
            URL="${SMN_BASE}v1/observations/station/${station_id}?datetime=${HOUR_UTC:0:10}T${HOUR_UTC:9:2}:00:00Z&token=${SMN_TOKEN}"
            DATA=$(curl -s -H "Authorization: Bearer $SMN_TOKEN" "$URL" | jq -r '.observations[0] // empty')
            # Map to wind_spd/dir, visibility_mi, cloud_pct; tide pred if available
            ;;
    esac
done

# Post-process: Add moon_phase_pct, sunrise/sunset (use Python snippet if needed, e.g., via astropy)
log "Stations file written: $OUTPUT_FILE (7 lines expected)"
