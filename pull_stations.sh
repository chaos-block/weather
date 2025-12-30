#!/bin/bash
set -euo pipefail

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

LOG_FILE="${LOGS_DIR}/stations.log"
mkdir -p "${CURRENT_DIR}" "${LOGS_DIR}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] stations: $1" | tee -a "$LOG_FILE"; }

HOUR_UTC=$(date -u -d '3 hours ago' +'%Y%m%dT%H')Z
TIMESTAMP="${HOUR_UTC:0:13}:00:00Z"
HOUR_YYYYMMDDHH=$(date -u -d '3 hours ago' +'%Y%m%d%H')

OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}.jsonl"
> "$OUTPUT_FILE"

log "Starting pull for ${HOUR_UTC}"

echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
  [ -z "$station_id" ] && continue
  log "Processing $station_id ($source)"

  # Initialize all to null
  declare -A vals=( [tide_height_ft]=null [tide_speed_kts]=null [tide_dir_deg]=null [visibility_mi]=null [cloud_pct]=null [wave_ht_ft]=null [wind_spd_kts]=null [wind_dir_deg]=null )

  case $source in
    NOAA)
      BASE="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&begin_date=${HOUR_YYYYMMDDHH}&end_date=${HOUR_YYYYMMDDHH}&time_zone=gmt&units=english&format=json"
      [ -n "${NOAA_TOKEN:-}" ] && BASE="${BASE}&token=${NOAA_TOKEN}"

      if echo "$fields" | grep -q "tide_height_ft"; then
        vals[tide_height_ft]=$(curl -s "${BASE}&product=hourly_height&datum=MLLW" | jq -r '.data[0].v // null')
      fi
      if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
        data=$(curl -s "${BASE}&product=currents&interval=h")
        vals[tide_speed_kts]=$(echo "$data" | jq -r '[.data[].s] | max // null')
        vals[tide_dir_deg]=$(echo "$data" | jq -r '.data[] | select(.s == ($(echo "$data" | jq '[.data[].s] | max'))) | .d // null' | head -1)
      fi
      if echo "$fields" | grep -q "wind"; then
        d=$(curl -s "${BASE}&product=wind&interval=h" | jq -r '.data[0]')
        vals[wind_spd_kts]=$(echo "$d" | jq -r '.s // null')
        vals[wind_dir_deg]=$(echo "$d" | jq -r '.d // null')
      fi
      if echo "$fields" | grep -q "visibility_mi"; then
        vals[visibility_mi]=$(curl -s "${BASE}&product=visibility&interval=h" | jq -r '.data[0].v // null')
      fi
      ;;

    NDBC)
      line=$(curl -s "https://www.ndbc.noaa.gov/data/realtime2/${station_id}.txt" | awk -v y=$(date -u -d '3 hours ago' +%Y) -v m=$(date -u -d '3 hours ago' +%m) -v d=$(date -u -d '3 hours ago' +%d) -v h=$(date -u -d '3 hours ago' +%H) '$1==y && $2==m && $3==d && $4==h {print; exit}')
      if [ -n "$line" ]; then
        vals[wave_ht_ft]=$(echo "$line" | awk '{v=$6; if(v!="MM") print v*3.28084; else print "null"}')
        vals[wind_dir_deg]=$(echo "$line" | awk '{print $11!="MM" ? $11 : "null"}')
        vals[wind_spd_kts]=$(echo "$line" | awk '{v=$12; if(v!="MM") print v*1.94384; else print "null"}')
        vals[visibility_mi]=$(echo "$line" | awk '{v=$18; if(v!="MM") print v*1.15078; else print "null"}')
      fi
      ;;

    SMN)
      log "SMN $station_id: No reliable observed hourly API – fields null"
      ;;
  esac

  # Astro (computed later, per hour)
  json="{ \"station_id\": \"$station_id\", \"timestamp\": \"$TIMESTAMP\", \"tide_height_ft\": ${vals[tide_height_ft]}, \"tide_speed_kts\": ${vals[tide_speed_kts]}, \"tide_dir_deg\": ${vals[tide_dir_deg]}, \"visibility_mi\": ${vals[visibility_mi]}, \"cloud_pct\": ${vals[cloud_pct]:-null}, \"wave_ht_ft\": ${vals[wave_ht_ft]:-null}, \"wind_spd_kts\": ${vals[wind_spd_kts]}, \"wind_dir_deg\": ${vals[wind_dir_deg]}, \"moon_phase_pct\": null, \"sunrise_time\": null, \"sunset_time\": null }"

  echo "$json" >> "$OUTPUT_FILE"
done

# Add astronomical fields (same for all stations, San Diego ref)
python3 - <<'PY'
import math, datetime
utc = datetime.datetime.strptime("${HOUR_UTC:0:13}", "%Y%m%dT%H")
# Simple moon phase
days = (utc - datetime.datetime(2000,1,6,18,14)).days + (utc - datetime.datetime(utc.year,utc.month,utc.day)).total_seconds()/86400
phase = (days % 29.53059) / 29.53059
illum = (1 - math.cos(2*math.pi*phase)) / 2 * 100
print(f"moon_phase_pct={illum:.1f}")

# Sunrise/sunset approx (use pyephem or simple; here placeholder)
print("sunrise_time=06:45")  # Replace with accurate lib if installed
print("sunset_time=16:55")
PY

# Then sed replace null astro with values in OUTPUT_FILE

log "Completed ${OUTPUT_FILE} – $(wc -l < "$OUTPUT_FILE") stations"
