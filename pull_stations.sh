#!/bin/bash
set -euo pipefail

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

LOG_FILE="${LOGS_DIR}/stations.log"
mkdir -p "${CURRENT_DIR}" "${LOGS_DIR}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] stations: $1" | tee -a "$LOG_FILE"; }

# Calculate lookback time once
LOOKBACK_HOURS=2
LOOKBACK_DATE=$(date -u -d "$LOOKBACK_HOURS hours ago")
HOUR_UTC=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%dT%H')
TIMESTAMP=$(date -u -d "$LOOKBACK_DATE" +'%Y-%m-%dT%H:00:00Z')
HOUR_YYYYMMDDHH=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d%H')
DATE_YYYYMMDD=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d')

# Use temp file for atomic write
TEMP_FILE="${CURRENT_DIR}/.stations_${HOUR_UTC}.jsonl.tmp"
OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}.jsonl"
> "$TEMP_FILE"

log "Starting pull for ${HOUR_UTC}"

# Calculate astronomical data once (before loop)
# Remove trailing Z for Python parsing
astro_data=$(python3 - <<PY
import math, datetime

utc = datetime.datetime.strptime("${HOUR_UTC}", "%Y%m%dT%H")

# Moon phase calculation
days = (utc - datetime.datetime(2000,1,6,18,14)).days + \
       (utc - datetime.datetime(utc.year,utc.month,utc.day)).total_seconds()/86400
phase = (days % 29.53059) / 29.53059
illum = (1 - math.cos(2*math.pi*phase)) / 2 * 100

print(f"moon_phase_pct={illum:.1f}")

# Sunrise/sunset calculation (requires: pip install astral)
try:
    from astral import LocationInfo
    from astral.sun import sun
    # Use San Diego as reference location
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

# Extract astronomical values
moon_phase=$(echo "$astro_data" | grep moon_phase_pct | cut -d= -f2)
sunrise=$(echo "$astro_data" | grep sunrise_time | cut -d= -f2)
sunset=$(echo "$astro_data" | grep sunset_time | cut -d= -f2)

log "Astronomical data: moon=${moon_phase}%, sunrise=${sunrise}, sunset=${sunset}"

# Process each station
echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
  [ -z "$station_id" ] && continue
  
  # Validate station_id format (alphanumeric only)
  if ! [[ "$station_id" =~ ^[A-Z0-9]+$ ]]; then
    log "WARNING: Invalid station_id format: $station_id - skipping"
    continue
  fi
  
  log "Processing $station_id ($source)"

  # Initialize all fields to null
  declare -A vals=(
    [tide_height_ft]=null
    [tide_speed_kts]=null
    [tide_dir_deg]=null
    [visibility_mi]=null
    [wave_ht_ft]=null
    [wind_spd_kts]=null
    [wind_dir_deg]=null
  )

  case $source in
    NOAA)
      # NOAA API requires begin_date and end_date in YYYYMMDD format for single day
      DATE_YYYYMMDD=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d')
      BASE="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&begin_date=${DATE_YYYYMMDD}&end_date=${DATE_YYYYMMDD}&time_zone=gmt&units=english&format=json"
      [ -n "${NOAA_TOKEN:-}" ] && BASE="${BASE}&application=${NOAA_TOKEN}"

      # Tide height - use water_level with hourly interval for real-time data
      if echo "$fields" | grep -q "tide_height_ft"; then
        url="${BASE}&product=water_level&datum=MLLW&interval=h"
        response=$(curl -sf "$url" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          # Find the specific hour we want
          vals[tide_height_ft]=$(echo "$response" | jq -r --arg ts "$TIMESTAMP" \
            '.data[] | select(.t | startswith($ts[0:13])) | .v // null' | head -1)
          [ -z "${vals[tide_height_ft]}" ] && vals[tide_height_ft]=null
        else
          error_msg=$(echo "$response" | jq -r '.error.message // "HTTP error"' 2>/dev/null || echo "Connection failed")
          log "WARNING: Failed to fetch tide height for $station_id: $error_msg"
        fi
      fi

      # Tidal currents (speed and direction)
      if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
        url="${BASE}&product=currents"
        response=$(curl -sf "$url" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          # Get data for our specific hour
          hour_data=$(echo "$response" | jq -r --arg ts "$TIMESTAMP" \
            '[.data[] | select(.t | startswith($ts[0:13]))]')
          
          if [ "$(echo "$hour_data" | jq '. | length')" -gt 0 ]; then
            max_speed=$(echo "$hour_data" | jq -r '[.[].s] | max // null')
            vals[tide_speed_kts]=$max_speed
            
            # Get direction corresponding to max speed
            if [ "$max_speed" != "null" ]; then
              vals[tide_dir_deg]=$(echo "$hour_data" | jq -r --arg max "$max_speed" \
                '.[] | select(.s == ($max | tonumber)) | .d // null' | head -1)
            fi
          fi
        else
          error_msg=$(echo "$response" | jq -r '.error.message // "HTTP error"' 2>/dev/null || echo "Connection failed")
          log "WARNING: Failed to fetch currents for $station_id: $error_msg"
        fi
      fi

      # Wind
      if echo "$fields" | grep -q "wind"; then
        response=$(curl -sf "${BASE}&product=wind&interval=h" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          wind_data=$(echo "$response" | jq -r '.data[0]')
          vals[wind_spd_kts]=$(echo "$wind_data" | jq -r '.s // null')
          vals[wind_dir_deg]=$(echo "$wind_data" | jq -r '.d // null')
        else
          log "WARNING: Failed to fetch wind for $station_id"
        fi
      fi

      # Visibility
      if echo "$fields" | grep -q "visibility_mi"; then
        response=$(curl -sf "${BASE}&product=visibility&interval=h" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          vals[visibility_mi]=$(echo "$response" | jq -r '.data[0].v // null')
        else
          log "WARNING: Failed to fetch visibility for $station_id"
        fi
      fi
      ;;

    NDBC)
      response=$(curl -sf "https://www.ndbc.noaa.gov/data/realtime2/${station_id}.txt" 2>/dev/null || echo "")
      if [ -n "$response" ]; then
        # Extract line matching our target hour
        year=$(date -u -d "$LOOKBACK_DATE" +%Y)
        month=$(date -u -d "$LOOKBACK_DATE" +%m)
        day=$(date -u -d "$LOOKBACK_DATE" +%d)
        hour=$(date -u -d "$LOOKBACK_DATE" +%H)
        
        line=$(echo "$response" | awk -v y="$year" -v m="$month" -v d="$day" -v h="$hour" \
                                       '$1==y && $2==m && $3==d && $4==h {print; exit}')
        
        if [ -n "$line" ]; then
          # Wave height (meters to feet: × 3.28084)
          vals[wave_ht_ft]=$(echo "$line" | awk '{v=$6; if(v!="MM" && v>=0) print v*3.28084; else print "null"}')
          
          # Wind direction (degrees)
          vals[wind_dir_deg]=$(echo "$line" | awk '{v=$11; if(v!="MM" && v>=0 && v<=360) print v; else print "null"}')
          
          # Wind speed (m/s to knots: × 1.94384)
          vals[wind_spd_kts]=$(echo "$line" | awk '{v=$12; if(v!="MM" && v>=0) print v*1.94384; else print "null"}')
          
          # Visibility (nautical miles to statute miles: × 1.15078)
          vals[visibility_mi]=$(echo "$line" | awk '{v=$18; if(v!="MM" && v>=0) print v*1.15078; else print "null"}')
        else
          log "WARNING: No matching data line found for $station_id at ${HOUR_UTC}"
        fi
      else
        log "WARNING: Failed to fetch NDBC data for $station_id"
      fi
      ;;

    SMN)
      log "WARNING: SMN $station_id - No reliable observed hourly API available, all fields null"
      ;;
    
    *)
      log "ERROR: Unknown source '$source' for station $station_id"
      ;;
  esac

  # Construct JSON output with all fields
  json=$(cat <<EOF
{ "station_id": "$station_id", "timestamp": "$TIMESTAMP", "tide_height_ft": ${vals[tide_height_ft]}, "tide_speed_kts": ${vals[tide_speed_kts]}, "tide_dir_deg": ${vals[tide_dir_deg]}, "visibility_mi": ${vals[visibility_mi]}, "wave_ht_ft": ${vals[wave_ht_ft]}, "wind_spd_kts": ${vals[wind_spd_kts]}, "wind_dir_deg": ${vals[wind_dir_deg]}, "moon_phase_pct": $moon_phase, "sunrise_time": "$sunrise", "sunset_time": "$sunset" }
EOF
)

  echo "$json" >> "$TEMP_FILE"
done

# Atomic move to final location
if [ -f "$TEMP_FILE" ]; then
  mv "$TEMP_FILE" "$OUTPUT_FILE"
  log "Completed ${OUTPUT_FILE} – $(wc -l < "$OUTPUT_FILE") stations"
else
  log "ERROR: Temp file not created, no output generated"
  exit 1
fi
