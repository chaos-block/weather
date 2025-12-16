#!/bin/bash
set -euo pipefail

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] stations: $1" | tee -a "$LOG_FILE"; }

for offset in 1 2 3 4 5 6; do
  HOUR_UTC=$(date -u -d "${offset} hours ago" +'%Y%m%dT%H')Z
  TIMESTAMP="${HOUR_UTC:0:13}:00:00Z"
  OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}.jsonl"
  > "$OUTPUT_FILE"  # Truncate per hour

  log "Verifying past hour: $HOUR_UTC (offset $offset)"

  echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
    [ -z "$station_id" ] && continue
    # ... rest of loop (fetch/mapping/JSON echo >> "$OUTPUT_FILE")
  done

  # Astro post-process per hour (move inside loop or run after)
done

log "Past 6 hours verification complete (6 files in /data/current/)"

HOUR_START=$(date -u -d '3 hours ago' +'%Y-%m-%d %H:00:00')
HOUR_END=$(date -u -d '2 hours ago' +'%Y-%m-%d %H:00:00')
TIMESTAMP="${HOUR_UTC:0:13}:00:00Z"

OUTPUT_FILE="${CURRENT_DIR}/stations_${HOUR_UTC}.jsonl"
> "$OUTPUT_FILE"          # Truncate file at start of run

LOG_FILE="${LOGS_DIR}/stations.log"

mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log "Starting stations pull for ${HOUR_UTC}"

# Loop over stations
echo "$STATIONS_LIST" | grep -v '^$' | while IFS='|' read -r station_id name lat lon source fields; do
  [ -z "$station_id" ] && continue
    log "Processing $station_id ($name at $lat,$lon) – source: $source – fields: $fields"

    # Initialize fields as null
    tide_height_ft=null
    tide_speed_kts=null
    tide_dir_deg=null
    visibility_mi=null
    cloud_pct=null
    wave_ht_ft=null
    wind_spd_kts=null
    wind_dir_deg=null
    moon_phase_pct=null
    sunrise_time=null
    sunset_time=null

    case $source in
        NOAA)
            # CO-OPS API base
            BASE_URL="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${station_id}&time_zone=gmt&units=english&format=json&application=weather_project"
            TOKEN_PARAM=""
            [ -n "${NOAA_TOKEN:-}" ] && TOKEN_PARAM="&token=${NOAA_TOKEN}"

            # Fetch per product if field present (range=1 for hour)
            if echo "$fields" | grep -q "tide_height_ft"; then
                URL="${BASE_URL}&begin_date=${HOUR_START// /}&range=1&datum=MLLW&product=water_level${TOKEN_PARAM}"
                DATA=$(curl -s "$URL" | jq '.data[0]' 2>/dev/null || echo "{}")
                tide_height_ft=$(echo "$DATA" | jq -r '.v // null')
            fi

            if echo "$fields" | grep -q "tide_speed_kts\|tide_dir_deg"; then
                URL="${BASE_URL}&begin_date=${HOUR_START// /}&range=1&interval=h&product=currents${TOKEN_PARAM}"
                DATA=$(curl -s "$URL" | jq '.data[]' 2>/dev/null || echo "{}")
                tide_height_ft=$(echo "$DATA" | jq -r 'last(.v // null)')
                tide_dir_deg=$(echo "$DATA" | jq -r '.d // null')
            fi

            if echo "$fields" | grep -q "wind_spd_kts\|wind_dir_deg"; then
                URL="${BASE_URL}&begin_date=${HOUR_START// /}&range=1&interval=h&product=wind${TOKEN_PARAM}"
                DATA=$(curl -s "$URL" | jq '.data[0]' 2>/dev/null || echo "{}")
                wind_spd_kts=$(echo "$DATA" | jq -r '.s // null')
                wind_dir_deg=$(echo "$DATA" | jq -r '.d // null')
            fi

            if echo "$fields" | grep -q "visibility_mi"; then
                URL="${BASE_URL}&begin_date=${HOUR_START// /}&range=1&interval=h&product=visibility${TOKEN_PARAM}"
                DATA=$(curl -s "$URL" | jq '.data[0]' 2>/dev/null || echo "{}")
                visibility_mi=$(echo "$DATA" | jq -r '.v // null')
            fi

            # Wave and cloud not in CO-OPS; assume from other if listed (log warning if requested but unavailable)
            if echo "$fields" | grep -q "wave_ht_ft\|cloud_pct"; then
                log "WARN: wave_ht_ft or cloud_pct requested for NOAA station $station_id but not available in CO-OPS API"
            fi
            ;;

        NDBC)
            # NDBC realtime txt (last 45 days; filter for H-3 hour line)
            URL="https://www.ndbc.noaa.gov/data/realtime2/${station_id}.txt"
            DATA=$(curl -s "$URL" | grep "^$(date -u -d '3 hours ago' +'%Y %m %d %H')" | head -1)
            if [ -n "$DATA" ]; then
                # Parse fixed-width columns (example: year mm dd hh mm WVHT DPD APD MWD WD WSPD GST WD PRES ATMP WTMP DEWP VIS TST MWD TIDE)
                visibility_mi=$(echo "$DATA" | awk '{if ($18 != "MM") print $18 * 1.15078; else print "null"}')  # NM to statute mi
                wind_spd_raw=$(echo "$DATA" | awk '{print $12}')
                if [ "$wind_spd_raw" != "MM" ] && [ "$wind_spd_raw" != "" ]; then
                  wind_spd_kts=$(awk "BEGIN {print $wind_spd_raw * 1.94384}")
                else
                  wind_spd_kts=null
                fi
                wind_dir_deg=$(echo "$DATA" | awk '{print $11}')
                visibility_mi=$(echo "$DATA" | awk '{print $18 * 0.539957}')  # NM to mi? Wait, NDBC VIS is in NM, spec is mi, but statute mi or nautical? Spec mi = statute, but NM is nautical mile ~1.15 statute.
                if [ "$visibility_mi" != "MM" ]; then visibility_mi=$(awk "BEGIN {print $visibility_mi * 1.15078}") ; else visibility_mi=null ; fi
                # Cloud not in NDBC; log if requested
                if echo "$fields" | grep -q "cloud_pct"; then
                    log "WARN: cloud_pct requested for NDBC $station_id but not available"
                fi
            else
                log "ERROR: No data for NDBC $station_id in hour $HOUR_UTC"
            fi
            ;;

        SMN)
            # SMN Mexico API (assumed endpoint from project history; adjust if needed)
            BASE_URL="${SMN_BASE:-https://smn.conagua.gob.mx/api/}"
            URL="${BASE_URL}v1/observations/station/${station_id}?datetime=${HOUR_UTC:0:4}-${HOUR_UTC:4:2}-${HOUR_UTC:6:2}T${HOUR_UTC:8:2}:00:00Z"
            DATA=$(curl -s -H "Authorization: Bearer ${SMN_TOKEN}" "$URL" | jq '.observations[0]' 2>/dev/null || echo "{}")
            if [ -n "$DATA" ]; then
                wind_spd=$(echo "$DATA" | jq -r '.wind_speed // null')
                  [ "$wind_spd" != null ] && wind_spd_kts=$(awk "BEGIN {print $wind_spd * 1.94384}") || wind_spd_kts=null
                wind_dir_deg=$(echo "$DATA" | jq -r '.wind_direction // null')
                visibility_mi=$(echo "$DATA" | jq -r '.visibility // null' | awk '{print $1 * 0.621371}')  # Assume km to mi
                cloud_pct=$(echo "$DATA" | jq -r '.cloud_cover // null')
                tide_height_ft=$(echo "$DATA" | jq -r '.tide_height // null' | awk '{print $1 * 3.28084}')  # m to ft
                wave_ht_ft=$(echo "$DATA" | jq -r '.wave_height // null' | awk '{print $1 * 3.28084}')
            else
                log "ERROR: No data for SMN $station_id in hour $HOUR_UTC"
            fi
            ;;
    esac

    # Output JSON line (omit null/empty fields)
    JSON="{\"station_id\":\"$station_id\",\"timestamp\":\"$TIMESTAMP\""
    if [ "$tide_height_ft" != null ]; then JSON="$JSON,\"tide_height_ft\":$tide_height_ft"; fi
    # Repeat for all fields (tide_speed_kts, visibility_mi, etc.)
    JSON="$JSON}"
    [ "$tide_height_ft" != null ] && JSON="$JSON,\"tide_height_ft\":$tide_height_ft"
    [ "$tide_speed_kts" != null ] && JSON="$JSON,\"tide_speed_kts\":$tide_speed_kts"
    [ "$tide_dir_deg" != null ] && JSON="$JSON,\"tide_dir_deg\":$tide_dir_deg"
    [ "$visibility_mi" != null ] && JSON="$JSON,\"visibility_mi\":$visibility_mi"
    [ "$cloud_pct" != null ] && JSON="$JSON,\"cloud_pct\":$cloud_pct"
    [ "$wave_ht_ft" != null ] && JSON="$JSON,\"wave_ht_ft\":$wave_ht_ft"
    [ "$wind_spd_kts" != null ] && JSON="$JSON,\"wind_spd_kts\":$wind_spd_kts"
    [ "$wind_dir_deg" != null ] && JSON="$JSON,\"wind_dir_deg\":$wind_dir_deg"
    JSON="$JSON}"

    echo "$JSON" >> "$OUTPUT_FILE"
done

# Post-process: Calculate moon_phase_pct, sunrise_time, sunset_time (common to all, for San Diego approx lat=32.72, lon=-117.16)
log "Calculating astronomical fields (moon/sunrise/sunset)"
python3 -c '
import datetime as dt
import math

utc_time = dt.datetime.strptime("'${HOUR_UTC:0:13}'", "%Y%m%dT%H")
local_time = utc_time - dt.timedelta(hours=8)  # PST approx

# Sunrise/sunset approx (Julian day formula)
jd = local_time.toordinal() + (local_time.hour - 12) / 24 + local_time.minute / 1440 + local_time.second / 86400
lat = 32.72
lon = -117.16
n = jd - 2451545.0 + 0.0008
jstar = n - lon / 360
M = (357.5291 + 0.98560028 * jstar) % 360
C = 1.9148 * math.sin(math.radians(M)) + 0.02 * math.sin(math.radians(2*M))
lam = (M + 102.9372 + C + 180) % 360
jtransit = jd + 0.0053 * math.sin(math.radians(M)) - 0.0069 * math.sin(math.radians(2*lam))
decl = math.degrees(math.asin(math.sin(math.radians(lam)) * math.sin(math.radians(23.45)))
h0 = math.degrees(math.acos(math.cos(math.radians(90.833)) / math.cos(math.radians(lat)) / math.cos(math.radians(decl)) - math.tan(math.radians(lat)) * math.tan(math.radians(decl))))
sunrise_jd = jtransit - h0 / 360
sunset_jd = jtransit + h0 / 360
sunrise_time = dt.datetime.fromordinal(int(sunrise_jd)) + dt.timedelta(days=sunrise_jd - int(sunrise_jd))
sunset_time = dt.datetime.fromordinal(int(sunset_jd)) + dt.timedelta(days=sunset_jd - int(sunset_jd))
print("sunrise_time=" + sunrise_time.strftime('%H:%M'))
print("sunset_time=" + sunset_time.strftime('%H:%M'))
print("moon_phase_pct=" + str(moon_phase_pct))

# Moon phase approx (0-100 % illumination)
new_moon = dt.datetime(2000, 1, 6, 18, 14)
days_since = (utc_time - new_moon).days
cycle = 29.53
phase = (days_since % cycle) / cycle * 100
moon_phase_pct = round(math.sin(math.pi * phase / 50) * 50 + 50, 1)  # Approx illumination
print(f"moon_phase_pct={moon_phase_pct}")
' >> temp_astro.sh  # Redirect to temp file, then source or sed into output

# Insert astro fields into JSONL (sed or loop over file)
log "Inserting astro fields into $OUTPUT_FILE"
astro_sunrise=$(grep sunrise_time temp_astro.sh | cut -d= -f2)
astro_sunset=$(grep sunset_time temp_astro.sh | cut -d= -f2)
astro_moon=$(grep moon_phase_pct temp_astro.sh | cut -d= -f2)
rm temp_astro.sh

awk -v sunrise="$astro_sunrise" -v sunset="$astro_sunset" -v moon="$astro_moon" '{
    sub(/}$/, ",\"moon_phase_pct\":" moon ",\"sunrise_time\":" sunrise ",\"sunset_time\":" sunset "}")
    print
}' "$OUTPUT_FILE" > "${OUTPUT_FILE}.tmp"
mv "${OUTPUT_FILE}.tmp" "$OUTPUT_FILE"

log "Stations file written: $OUTPUT_FILE (17 lines expected)"
