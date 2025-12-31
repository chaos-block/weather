#!/bin/bash
set -euo pipefail

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

LOG_FILE="${LOGS_DIR}/stations.log"
mkdir -p "${CURRENT_DIR}" "${LOGS_DIR}"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] stations:  $1" | tee -a "$LOG_FILE"; }

# Calculate lookback time once
# INCREASED TO 4 HOURS:  NOAA observed water level data has 2-4 hour publication latency
LOOKBACK_HOURS=4
LOOKBACK_DATE=$(date -u -d "$LOOKBACK_HOURS hours ago")
HOUR_UTC=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%dT%H')
TIMESTAMP=$(date -u -d "$LOOKBACK_DATE" +'%Y-%m-%dT%H:00:00Z')
HOUR_YYYYMMDDHH=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d%H')
DATE_YYYYMMDD=$(date -u -d "$LOOKBACK_DATE" +'%Y%m%d')

# Use temp file for atomic write
TEMP_FILE="${CURRENT_DIR}/.stations_${HOUR_UTC}Z.jsonl.tmp"
OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}Z.jsonl"
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
    s = sun(city. observer, date=utc. date())
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

log "Astronomical data:  moon=${moon_phase}%, sunrise=${sunrise}, sunset=${sunset}"

# Process each station
echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
  [ -z "$station_id" ] && continue
  
  # Validate station_id format (alphanumeric and hyphens)
  if !  [[ "$station_id" =~ ^[A-Z0-9-]+$ ]]; then
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
    [cloud_pct]=null
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

      # Tide height - OBSERVED data only:  use water_level with 6-minute interval, aggregate to hourly
      if echo "$fields" | grep -q "tide_height_ft"; then
        url="${BASE}&product=water_level&datum=MLLW&interval=6"
        response=$(curl -sf "$url" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          # Aggregate all 6-minute observations within the target hour to get hourly mean
          vals[tide_height_ft]=$(echo "$response" | jq -r --arg hour "${HOUR_UTC: 0:13}" \
            '[.data[] | select(.t | startswith($hour)) | .v | tonumber] | 
             if length > 0 then (add / length) else null end')
        else
          error_msg=$(echo "$response" | jq -r '.error. message // "HTTP error"' 2>/dev/null || echo "Connection failed")
          log "WARNING: Failed to fetch tide height for $station_id:  $error_msg"
        fi
      fi

      # Tidal currents (speed and direction) - OBSERVED data only
      if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
        url="${BASE}&product=currents"
        response=$(curl -sf "$url" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          # Get data for our specific hour and calculate averages
          hour_data=$(echo "$response" | jq -r --arg hour "${HOUR_UTC:0:13}" \
            '[.data[] | select(.t | startswith($hour))]')
          
          if [ "$(echo "$hour_data" | jq '. | length')" -gt 0 ]; then
            # Average speed for the hour
            vals[tide_speed_kts]=$(echo "$hour_data" | jq -r \
              '[.[]. s | tonumber] | if length > 0 then (add / length) else null end')
            
            # Direction:  simple arithmetic mean
            vals[tide_dir_deg]=$(echo "$hour_data" | jq -r \
              '[.[].d | tonumber] | if length > 0 then (add / length) else null end')
          fi
        else
          error_msg=$(echo "$response" | jq -r '.error.message // "HTTP error"' 2>/dev/null || echo "Connection failed")
          log "WARNING: Failed to fetch currents for $station_id: $error_msg"
        fi
      fi

      # Wind - only if field is explicitly requested for this station
      if echo "$fields" | grep -q "wind_spd_kts\|wind_dir_deg"; then
        response=$(curl -sf "${BASE}&product=wind&interval=h" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '. data' >/dev/null 2>&1; then
          # Get first record matching our hour
          wind_data=$(echo "$response" | jq -r --arg hour "${HOUR_UTC:0:13}" \
            '[.data[] | select(. t | startswith($hour))] | .[0]')
          vals[wind_spd_kts]=$(echo "$wind_data" | jq -r '. s // null')
          vals[wind_dir_deg]=$(echo "$wind_data" | jq -r '. d // null')
        else
          log "WARNING: Failed to fetch wind for $station_id"
        fi
      fi

      # Visibility - only if field is explicitly requested for this station
      if echo "$fields" | grep -q "visibility_mi"; then
        response=$(curl -sf "${BASE}&product=visibility&interval=h" 2>/dev/null || echo "")
        if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
          # Get first record matching our hour
          vis_data=$(echo "$response" | jq -r --arg hour "${HOUR_UTC:0:13}" \
            '[.data[] | select(.t | startswith($hour))] | .[0]')
          vals[visibility_mi]=$(echo "$vis_data" | jq -r '.v // null')
        else
          log "WARNING: Failed to fetch visibility for $station_id"
        fi
      fi
      ;;

    NDBC)
      response=$(curl -sf "https://www.ndbc.noaa.gov/data/realtime2/${station_id}. txt" 2>/dev/null || echo "")
      
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
              v=$9; if(v! ="MM" && v>=0) {sum+=v; count++}
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

  # Construct JSON output with all fields
  json=$(cat <<EOF
{ "station_id": "$station_id", "timestamp": "$TIMESTAMP", "tide_height_ft": ${vals[tide_height_ft]}, "tide_speed_kts": ${vals[tide_speed_kts]}, "tide_dir_deg": ${vals[tide_dir_deg]}, "visibility_mi": ${vals[visibility_mi]}, "cloud_pct": ${vals[cloud_pct]}, "wave_ht_ft": ${vals[wave_ht_ft]}, "wind_spd_kts": ${vals[wind_spd_kts]}, "wind_dir_deg": ${vals[wind_dir_deg]}, "moon_phase_pct": $moon_phase, "sunrise_time": "$sunrise", "sunset_time": "$sunset" }
EOF
)

  echo "$json" >> "$TEMP_FILE"
done

# Atomic move to final location
if [ -f "$TEMP_FILE" ]; then
  mv "$TEMP_FILE" "$OUTPUT_FILE"
  log "Completed ${OUTPUT_FILE} – $(wc -l < "$OUTPUT_FILE") stations"
else
  log "ERROR:  Temp file not created, no output generated"
  exit 1
fi
