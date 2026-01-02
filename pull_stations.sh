#!/bin/bash
set -euo pipefail

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

LOG_FILE="${LOGS_DIR}/stations.log"
mkdir -p "${LOGS_DIR}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] stations:  $1" | tee -a "$LOG_FILE"; }

# Calculate lookback time once
# Set to 2 hours for real-time pulls (aligns with typical NOAA 2-hour latency)
# Can be overridden by OVERRIDE_TIMESTAMP for historical pulls
if [ -n "${OVERRIDE_TIMESTAMP:-}" ]; then
  # Historical mode: use provided timestamp
  TIMESTAMP="$OVERRIDE_TIMESTAMP"
  LOOKBACK_DATE=$(date -u -d "$TIMESTAMP")
else
  # Real-time mode: use lookback
  LOOKBACK_HOURS=2
  LOOKBACK_DATE=$(date -u -d "$LOOKBACK_HOURS hours ago")
  TIMESTAMP=$(date -u -d "$LOOKBACK_DATE" +'%Y-%m-%dT%H:00:00Z')
fi

HOUR_UTC=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%dT%H')
HOUR_ISO=$(date -u -d "$LOOKBACK_DATE" +'%Y-%m-%dT%H')
HOUR_YYYYMMDDHH=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d%H')
DATE_YYYYMMDD=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d')

# Output to /data/YYYY/ directory (new architecture)
YEAR=$(date -u -d "$LOOKBACK_DATE" +%Y)
OUTPUT_DIR="${OVERRIDE_OUTPUT_DIR:-${DATA_DIR}/${YEAR}}"
mkdir -p "${OUTPUT_DIR}"

# Use temp file for atomic write
TEMP_FILE="${OUTPUT_DIR}/.stations_${HOUR_UTC}Z.jsonl.tmp"
OUTPUT_FILE="${OUTPUT_DIR}/stations_${HOUR_UTC}Z.jsonl"
> "$TEMP_FILE"

log "Starting pull for ${HOUR_UTC}"
log "Target timestamp: ${TIMESTAMP}"

# Calculate astronomical data once (before loop)
astro_data=$(python3 - <<PY
import math, datetime

utc = datetime.datetime.strptime("${HOUR_UTC}", "%Y%m%dT%H")

# Moon phase calculation
days = (utc - datetime.datetime(2000,1,6,18,14)).days + \
       (utc - datetime.datetime(utc.year,utc.month,utc.day)).total_seconds()/86400
phase = (days % 29.53059) / 29.53059
illum = (1 - math.cos(2*math.pi*phase)) / 2 * 100

print(f"moon_phase_pct={illum:.1f}")

# Sunrise/sunset calculation (requires:  pip install astral)
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
moon_phase=$(echo "$astro_data" | grep moon_phase_pct | cut -d= -f2 | tr -d ' ')
sunrise=$(echo "$astro_data" | grep sunrise_time | cut -d= -f2 | tr -d ' ')
sunset=$(echo "$astro_data" | grep sunset_time | cut -d= -f2 | tr -d ' ')

log "Astronomical data:  moon=${moon_phase}%, sunrise=${sunrise}, sunset=${sunset}"

# Helper function to check if field is expected but missing
check_field() {
  local station_id="$1"
  local field_name="$2"
  local field_value="$3"
  local expected_fields="$4"
  
  # Check if this field is expected for this station (use word boundaries)
  if echo "$expected_fields" | grep -qw "$field_name"; then
    # Field is expected - check if it's null or empty
    if [ "$field_value" = "null" ] || [ -z "$field_value" ]; then
      log "WARNING: $field_name missing for $station_id (expected but got nothing)"
    fi
  fi
}

# Process each station
echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
  [ -z "$station_id" ] && continue
  
  # Validate station_id format (alphanumeric and hyphens)
  if ! [[ "$station_id" =~ ^[A-Z0-9-]+$ ]]; then
    log "WARNING: Invalid station_id format: $station_id - skipping"
    continue
  fi
  
  # Time the station processing
  START_TIME=$(date +%s%N)
  
  log "Processing $station_id ($source)"

  # Initialize all fields to null
  declare -A vals=(
    [tide_height_ft]=null
    [tide_speed_kts]=null
    [tide_dir_deg]=null
    [visibility_mi]=null
    [cloud_pct]=null
    [wave_ht_ft]=null
    [wind_spd_kts]=null
    [wind_dir_deg]=null
  )

  case $source in
    NOAA)
      # NOAA API requires begin_date and end_date in YYYYMMDD format
      # Use monthly date range to avoid API limits with hourly interval
      YEAR_MONTH=$(date -u -d "$LOOKBACK_DATE" +'%Y-%m')
      MONTH_START=$(date -u -d "${YEAR_MONTH}-01" +'%Y%m%d')
      # Get last day of month: go to first of next month, subtract one day
      MONTH_END=$(date -u -d "${YEAR_MONTH}-01 +1 month -1 day" +'%Y%m%d')
      
      BASE="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&begin_date=${MONTH_START}&end_date=${MONTH_END}&time_zone=gmt&units=english&format=json"
      [ -n "${NOAA_TOKEN:-}" ] && BASE="${BASE}&application=${NOAA_TOKEN}"

      # Create unique temp directory for this station to avoid collisions
      STATION_TEMP_DIR=$(mktemp -d)
      
      # Start all API calls in parallel (background jobs)
      # Tide height - OBSERVED data only:  use water_level with hourly interval
      if echo "$fields" | grep -q "tide_height_ft"; then
        (
          url="${BASE}&product=water_level&datum=MLLW&interval=h"
          response=$(curl -sf "$url" 2>/dev/null || echo "")
          if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
            # Extract the record matching our specific hour
            echo "$response" | jq -r --arg hour "$HOUR_ISO" \
              '[.data[] | select(.t | startswith($hour)) | .v | tonumber] | 
               if length > 0 then .[0] else null end' > "${STATION_TEMP_DIR}/tide_ht.tmp"
          else
            echo "null" > "${STATION_TEMP_DIR}/tide_ht.tmp"
            error_msg=$(echo "$response" | jq -r '.error.message // "HTTP error"' 2>/dev/null || echo "Connection failed")
            log "WARNING: Failed to fetch tide height for $station_id:  $error_msg"
          fi
        ) &
      fi

      # Tidal currents (speed and direction) - OBSERVED data only
      if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
        (
          url="${BASE}&product=currents&interval=h"
          response=$(curl -sf "$url" 2>/dev/null || echo "")
          if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
            # Get data for our specific hour
            echo "$response" | jq -r --arg hour "$HOUR_ISO" \
              '[.data[] | select(.t | startswith($hour))] | 
               if length > 0 then {
                 speed: (.[0].s // null),
                 dir: (.[0].d // null)
               } else {speed: null, dir: null} end' > "${STATION_TEMP_DIR}/currents.tmp"
          else
            echo '{"speed":null,"dir":null}' > "${STATION_TEMP_DIR}/currents.tmp"
            error_msg=$(echo "$response" | jq -r '.error.message // "HTTP error"' 2>/dev/null || echo "Connection failed")
            log "WARNING: Failed to fetch currents for $station_id: $error_msg"
          fi
        ) &
      fi

      # Wind - only if field is explicitly requested for this station
      if echo "$fields" | grep -q "wind_spd_kts\|wind_dir_deg"; then
        (
          response=$(curl -sf "${BASE}&product=wind&interval=h" 2>/dev/null || echo "")
          if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
            # Get first record matching our hour
            echo "$response" | jq -r --arg hour "$HOUR_ISO" \
              '[.data[] | select(.t | startswith($hour))] | 
               if length > 0 then {
                 spd: (.[0].s // null),
                 dir: (.[0].d // null)
               } else {spd: null, dir: null} end' > "${STATION_TEMP_DIR}/wind.tmp"
          else
            echo '{"spd":null,"dir":null}' > "${STATION_TEMP_DIR}/wind.tmp"
            log "WARNING: Failed to fetch wind for $station_id"
          fi
        ) &
      fi

      # Visibility - only if field is explicitly requested for this station
      if echo "$fields" | grep -q "visibility_mi"; then
        (
          response=$(curl -sf "${BASE}&product=visibility&interval=h" 2>/dev/null || echo "")
          if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
            # Get first record matching our hour
            echo "$response" | jq -r --arg hour "$HOUR_ISO" \
              '[.data[] | select(.t | startswith($hour))] | .[0].v // null' > "${STATION_TEMP_DIR}/vis.tmp"
          else
            echo "null" > "${STATION_TEMP_DIR}/vis.tmp"
            log "WARNING: Failed to fetch visibility for $station_id"
          fi
        ) &
      fi
      
      # Wait for all background jobs to complete
      wait
      
      # Read results from temp files (use consistent error handling pattern)
      vals[tide_height_ft]=$(cat "${STATION_TEMP_DIR}/tide_ht.tmp" 2>/dev/null || echo "null")
      vals[visibility_mi]=$(cat "${STATION_TEMP_DIR}/vis.tmp" 2>/dev/null || echo "null")
      
      if [ -f "${STATION_TEMP_DIR}/currents.tmp" ]; then
        currents=$(cat "${STATION_TEMP_DIR}/currents.tmp" 2>/dev/null || echo '{"speed":null,"dir":null}')
        vals[tide_speed_kts]=$(echo "$currents" | jq -r '.speed')
        vals[tide_dir_deg]=$(echo "$currents" | jq -r '.dir')
      fi
      
      if [ -f "${STATION_TEMP_DIR}/wind.tmp" ]; then
        wind=$(cat "${STATION_TEMP_DIR}/wind.tmp" 2>/dev/null || echo '{"spd":null,"dir":null}')
        vals[wind_spd_kts]=$(echo "$wind" | jq -r '.spd')
        vals[wind_dir_deg]=$(echo "$wind" | jq -r '.dir')
      fi
      
      # Clean up temp directory
      rm -rf "${STATION_TEMP_DIR}"
      ;;

    NDBC)
      response=$(curl -sf "https://www.ndbc.noaa.gov/data/realtime2/${station_id}.txt" 2>/dev/null || echo "")
      
      # Check for 404 or empty response
      if echo "$response" | grep -qi "404\|not found" || [ -z "$response" ]; then
        log "WARNING:  NDBC buoy $station_id offline or unavailable – all fields null"
      elif [ -n "$response" ]; then
        # Extract line matching our target hour
        # NDBC format: YY MM DD hh mm (columns 1-5)
        year=$(date -u -d "$LOOKBACK_DATE" +%Y)
        month=$(date -u -d "$LOOKBACK_DATE" +%m)
        day=$(date -u -d "$LOOKBACK_DATE" +%d)
        hour=$(date -u -d "$LOOKBACK_DATE" +%H)
        
        # Get all lines for the target hour (may be multiple readings)
        lines=$(echo "$response" | awk -v y="$year" -v m="$month" -v d="$day" -v h="$hour" \
                                       '$1==y && $2==m && $3==d && $4==h {print}')
        
        if [ -n "$lines" ]; then
          # Average wave height (column 9:  WVHT, meters to feet:  × 3.28084)
          if echo "$fields" | grep -q "wave_ht_ft"; then
            vals[wave_ht_ft]=$(echo "$lines" | awk '{
              sum=0; count=0;
              v=$9; if(v!="MM" && v>=0) {sum+=v; count++}
            } END {
              if(count>0) print (sum/count)*3.28084; else print "null"
            }')
          fi
          
          # Average wind direction (column 6: WDIR, degrees)
          if echo "$fields" | grep -q "wind_dir_deg"; then
            vals[wind_dir_deg]=$(echo "$lines" | awk '{
              sum=0; count=0;
              v=$6; if(v!="MM" && v>=0 && v<=360) {sum+=v; count++}
            } END {
              if(count>0) print sum/count; else print "null"
            }')
          fi
          
          # Average wind speed (column 7: WSPD, m/s to knots:  × 1.94384)
          if echo "$fields" | grep -q "wind_spd_kts"; then
            vals[wind_spd_kts]=$(echo "$lines" | awk '{
              sum=0; count=0;
              v=$7; if(v!="MM" && v>=0) {sum+=v; count++}
            } END {
              if(count>0) print (sum/count)*1.94384; else print "null"
            }')
          fi
          
          # Average visibility (column 17: VIS, nautical miles to statute miles:  × 1.15078)
          if echo "$fields" | grep -q "visibility_mi"; then
            vals[visibility_mi]=$(echo "$lines" | awk '{
              sum=0; count=0;
              v=$17; if(v!="MM" && v>=0) {sum+=v; count++}
            } END {
              if(count>0) print (sum/count)*1.15078; else print "null"
            }')
          fi
        else
          log "WARNING: No matching data line found for $station_id at ${HOUR_UTC}"
        fi
      fi
      ;;

    SMN)
      # SMN Mexico API:  fetch observed weather data
      if [ -z "${SMN_TOKEN:-}" ]; then
        log "WARNING: SMN $station_id - SMN_TOKEN not configured, skipping"
      else
        response=$(curl -sf "https://smn.conagua.gob.mx/api/datos/estacion/${station_id}" \
          -H "Authorization: Bearer ${SMN_TOKEN}" 2>/dev/null || echo "")
        
        if [ -n "$response" ] && echo "$response" | jq -e '.datos[0]' >/dev/null 2>&1; then
          # Parse most recent hourly observed record
          latest=$(echo "$response" | jq -r '.datos[0]')
          
          # Extract only observed fields (do NOT use predictions/forecasts)
          if echo "$fields" | grep -q "wind_spd_kts"; then
            # SMN wind speed in m/s, convert to knots (×1.94384)
            wind_ms=$(echo "$latest" | jq -r '.velocidad_viento // null')
            if [ "$wind_ms" != "null" ] && [ -n "$wind_ms" ]; then
              vals[wind_spd_kts]=$(awk "BEGIN {printf \"%.2f\", $wind_ms * 1.94384}")
            fi
          fi
          
          if echo "$fields" | grep -q "wind_dir_deg"; then
            vals[wind_dir_deg]=$(echo "$latest" | jq -r '.direccion_viento // null')
          fi
          
          if echo "$fields" | grep -q "visibility_mi"; then
            # SMN visibility in km, convert to statute miles (÷1.60934)
            vis_km=$(echo "$latest" | jq -r '.visibilidad // null')
            if [ "$vis_km" != "null" ] && [ -n "$vis_km" ]; then
              vals[visibility_mi]=$(awk "BEGIN {printf \"%.2f\", $vis_km / 1.60934}")
            fi
          fi
          
          if echo "$fields" | grep -q "wave_ht_ft"; then
            # SMN wave height in meters, convert to feet (×3.28084)
            wave_m=$(echo "$latest" | jq -r '.altura_ola // null')
            if [ "$wave_m" != "null" ] && [ -n "$wave_m" ]; then
              vals[wave_ht_ft]=$(awk "BEGIN {printf \"%.2f\", $wave_m * 3.28084}")
            fi
          fi
        else
          log "WARNING: Failed to fetch SMN data for $station_id"
        fi
      fi
      ;;
    
    *)
      log "ERROR: Unknown source '$source' for station $station_id"
      ;;
  esac

  # Check for expected fields that returned null/empty
  check_field "$station_id" "tide_height_ft" "${vals[tide_height_ft]}" "$fields"
  check_field "$station_id" "tide_speed_kts" "${vals[tide_speed_kts]}" "$fields"
  check_field "$station_id" "tide_dir_deg" "${vals[tide_dir_deg]}" "$fields"
  check_field "$station_id" "visibility_mi" "${vals[visibility_mi]}" "$fields"
  check_field "$station_id" "cloud_pct" "${vals[cloud_pct]}" "$fields"
  check_field "$station_id" "wave_ht_ft" "${vals[wave_ht_ft]}" "$fields"
  check_field "$station_id" "wind_spd_kts" "${vals[wind_spd_kts]}" "$fields"
  check_field "$station_id" "wind_dir_deg" "${vals[wind_dir_deg]}" "$fields"

  # Construct JSON output with all fields (using jq for safety)
  jq -nc \
    --arg station_id "$station_id" \
    --arg timestamp "$TIMESTAMP" \
    --argjson tide_height_ft "${vals[tide_height_ft]}" \
    --argjson tide_speed_kts "${vals[tide_speed_kts]}" \
    --argjson tide_dir_deg "${vals[tide_dir_deg]}" \
    --argjson visibility_mi "${vals[visibility_mi]}" \
    --argjson cloud_pct "${vals[cloud_pct]}" \
    --argjson wave_ht_ft "${vals[wave_ht_ft]}" \
    --argjson wind_spd_kts "${vals[wind_spd_kts]}" \
    --argjson wind_dir_deg "${vals[wind_dir_deg]}" \
    --argjson moon_phase_pct "$moon_phase" \
    --arg sunrise "$sunrise" \
    --arg sunset "$sunset" \
    '{
      station_id: $station_id,
      timestamp: $timestamp,
      tide_height_ft: $tide_height_ft,
      tide_speed_kts: $tide_speed_kts,
      tide_dir_deg: $tide_dir_deg,
      visibility_mi: $visibility_mi,
      cloud_pct: $cloud_pct,
      wave_ht_ft: $wave_ht_ft,
      wind_spd_kts: $wind_spd_kts,
      wind_dir_deg: $wind_dir_deg,
      moon_phase_pct: $moon_phase_pct,
      sunrise_time: $sunrise,
      sunset_time: $sunset
    } | del(.[] | select(. == null))' >> "$TEMP_FILE"
  
  # Calculate elapsed time
  END_TIME=$(date +%s%N)
  ELAPSED_MS=$(( (END_TIME - START_TIME) / 1000000 ))
  log "Processed $station_id in ${ELAPSED_MS}ms"
done

# Atomic move to final location
if [ -f "$TEMP_FILE" ]; then
  mv "$TEMP_FILE" "$OUTPUT_FILE"
  log "Completed ${OUTPUT_FILE} – $(wc -l < "$OUTPUT_FILE") stations"
else
  log "ERROR:  Temp file not created, no output generated"
  exit 1
fi
