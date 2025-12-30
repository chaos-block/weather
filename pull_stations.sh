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

  case $source in
    NOAA)
      BASE="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&begin_date=${HOUR_YYYYMMDDHH}&end_date=${HOUR_YYYYMMDDHH}&time_zone=gmt&units=english&format=json"
      [ -n "${NOAA_TOKEN:-}" ] && BASE="${BASE}&token=${NOAA_TOKEN}"

      if echo "$fields" | grep -q "tide_height_ft"; then
        response=$(curl -sf "${BASE}&product=hourly_height&datum=MLLW" 2>/dev/null)
          if [ $? -eq 0 ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
            vals[tide_height_ft]=$(echo "$response" | jq -r '.data[0].v // null')
          else
            log "ERROR: Failed to fetch tide height for $station_id"
            vals[tide_height_ft]=null
        fi
      fi
      if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
        data=$(curl -s "${BASE}&product=currents&interval=h")
        max_speed=$(echo "$data" | jq -r '[.data[].s] | max // null')
        vals[tide_speed_kts]=$max_speed
        vals[tide_dir_deg]=$(echo "$data" | jq -r --arg max "$max_speed" '.data[] | select(.s == ($max | tonumber)) | .d // null' | head -1)
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
 # Remove cloud_pct from output
json="{ \"station_id\": \"$station_id\", \"timestamp\": \"$TIMESTAMP\", \"tide_height_ft\": ${vals[tide_height_ft]}, \"tide_speed_kts\": ${vals[tide_speed_kts]}, \"tide_dir_deg\": ${vals[tide_dir_deg]}, \"visibility_mi\": ${vals[visibility_mi]}, \"wave_ht_ft\": ${vals[wave_ht_ft]:-null}, \"wind_spd_kts\": ${vals[wind_spd_kts]}, \"wind_dir_deg\": ${vals[wind_dir_deg]}, \"moon_phase_pct\": null, \"sunrise_time\": null, \"sunset_time\": null }"

  echo "$json" >> "$OUTPUT_FILE"
done

# Add astronomical fields (same for all stations, San Diego ref)
# Add astronomical fields (same for all stations, San Diego ref)
astro_output=$(python3 - <<PY
import math, datetime
utc = datetime.datetime.strptime("${HOUR_UTC:0:13}", "%Y%m%dT%H")
# Simple moon phase
days = (utc - datetime.datetime(2000,1,6,18,14)).days + (utc - datetime.datetime(utc.year,utc.month,utc.day)).total_seconds()/86400
phase = (days % 29.53059) / 29.53059
illum = (1 - math.cos(2*math.pi*phase)) / 2 * 100
print(f"moon_phase_pct={illum:.1f}")

# Sunrise/sunset approx (use pyephem or simple; here placeholder)
# Sunrise/sunset calculation (requires: pip install astral)
try:
    from astral import LocationInfo
    from astral.sun import sun
    # Use San Diego as reference (or pass lat/lon from shell)
    city = LocationInfo("San Diego", "USA", "America/Los_Angeles", 32.7157, -117.1611)
    s = sun(city.observer, date=utc.date())
    sunrise_utc = s['sunrise'].strftime('%H:%M')
    sunset_utc = s['sunset'].strftime('%H:%M')
    print(f"sunrise_time={sunrise_utc}")
    print(f"sunset_time={sunset_utc}")
except ImportError:
    # Fallback if astral not installed
    print("sunrise_time=06:45")
    print("sunset_time=16:55")
    
PY
)

# Extract values
moon_phase=$(echo "$astro_output" | grep moon_phase_pct | cut -d= -f2)
sunrise=$(echo "$astro_output" | grep sunrise_time | cut -d= -f2)
sunset=$(echo "$astro_output" | grep sunset_time | cut -d= -f2)

# Replace null astro with actual values
sed -i "s/\"moon_phase_pct\": null/\"moon_phase_pct\": $moon_phase/g" "$OUTPUT_FILE"
sed -i "s/\"sunrise_time\": null/\"sunrise_time\": \"$sunrise\"/g" "$OUTPUT_FILE"
sed -i "s/\"sunset_time\": null/\"sunset_time\": \"$sunset\"/g" "$OUTPUT_FILE"

# Then sed replace null astro with values in OUTPUT_FILE

log "Completed ${OUTPUT_FILE} – $(wc -l < "$OUTPUT_FILE") stations"
