#!/bin/bash
set -euo pipefail

# =============================================================================
# verify_station_9410135.sh - Standalone verification for station 9410135
# Diagnostic script to verify NOAA API data collection and jq extraction
# 
# Usage: ./verify_station_9410135.sh START_DATE END_DATE
#        ./verify_station_9410135.sh 2025-12-01 2025-12-31
# =============================================================================

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

# Hard-coded configuration for station 9410135
STATION_ID="9410135"
PRODUCT="water_level"
DATUM="MLLW"
INTERVAL="h"
STATION_NAME="South San Diego Bay"

# Validate arguments
if [ $# -ne 2 ]; then
  echo "Usage: $0 START_DATE END_DATE"
  echo "Example: $0 2025-12-01 2025-12-31"
  exit 1
fi

START_DATE="$1"
END_DATE="$2"

# Validate date format
if ! date -d "$START_DATE" >/dev/null 2>&1 || ! date -d "$END_DATE" >/dev/null 2>&1; then
  echo "Error: Invalid date format. Use YYYY-MM-DD"
  exit 1
fi

# Check if jq is installed
if ! command -v jq >/dev/null 2>&1; then
  echo "Error: jq is not installed. Please install jq to use this script."
  exit 1
fi

# Check if curl is installed
if ! command -v curl >/dev/null 2>&1; then
  echo "Error: curl is not installed. Please install curl to use this script."
  exit 1
fi

mkdir -p "$LOGS_DIR"
LOG_FILE="${LOGS_DIR}/verify_9410135.log"
TEMP_DIR=$(mktemp -d)

log() { 
  echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] verify_9410135: $1" | tee -a "$LOG_FILE"
}

cleanup() {
  rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

log "========================================="
log "Verification for Station $STATION_ID ($STATION_NAME)"
log "Date range: $START_DATE to $END_DATE"
log "Product: $PRODUCT, Datum: $DATUM, Interval: $INTERVAL"
log "========================================="

# Print table header
printf "\n%-12s | %-12s | %-11s | %-8s | %s\n" "Date" "Hours NOAA" "Extracted" "Output" "Status"
printf "%.12s-+-%.12s-+-%.11s-+-%.8s-+-%s\n" "------------" "------------" "-----------" "--------" "---------"

# Track overall statistics
TOTAL_DAYS=0
TOTAL_OK=0
TOTAL_DISCREPANCIES=0
DEBUG_OUTPUT=""
TOTAL_HOURS_NOAA=0
TOTAL_HOURS_EXTRACTED=0
TOTAL_HOURS_OUTPUT=0

# Process each day in the range
CURRENT_EPOCH=$(date -d "$START_DATE" +%s)
END_EPOCH=$(date -d "$END_DATE" +%s)

while [ "$CURRENT_EPOCH" -le "$END_EPOCH" ]; do
  # Process each day in the range
  CURRENT_DATE=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m-%d)
  DATE_YYYYMMDD=$(date -u -d "@$CURRENT_EPOCH" +%Y%m%d)
  YEAR=$(date -u -d "@$CURRENT_EPOCH" +%Y)
  YEAR_MONTH=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m)
  
  TOTAL_DAYS=$((TOTAL_DAYS + 1))
  
  # Fetch NOAA API data for this day using monthly range
  # (same approach as pull_stations.sh to avoid API limits)
  MONTH_START=$(date -u -d "${YEAR_MONTH}-01" +'%Y%m%d')
  MONTH_END=$(date -u -d "${YEAR_MONTH}-01 +1 month -1 day" +'%Y%m%d')
  
  NOAA_URL="https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?station=${STATION_ID}&product=${PRODUCT}&begin_date=${MONTH_START}&end_date=${MONTH_END}&datum=${DATUM}&interval=${INTERVAL}&time_zone=gmt&units=english&format=json"
  
  if [ -n "${NOAA_TOKEN:-}" ]; then
    NOAA_URL="${NOAA_URL}&application=${NOAA_TOKEN}"
  fi
  
  # Fetch data
  response=$(curl -sf "$NOAA_URL" 2>/dev/null || echo "")
  
  # Count hours in NOAA API for this specific date
  hours_in_noaa=0
  hours_extracted=0
  hours_in_output=0
  day_data="[]"
  
  if [ -n "$response" ] && echo "$response" | jq -e '.data' >/dev/null 2>&1; then
    # Filter to this specific date once and save
    day_data=$(echo "$response" | jq -c --arg date "$CURRENT_DATE" '[.data[] | select(.t | startswith($date))]' 2>/dev/null || echo "[]")
    
    # Count records for this specific date
    hours_in_noaa=$(echo "$day_data" | jq 'length' 2>/dev/null || echo "0")
    
    # Test extraction using SAME jq logic as pull_stations.sh (lines 148-151)
    # NOTE: NOAA returns timestamps with SPACE format "2025-12-01 00:00", not ISO format "2025-12-01T00:00"
    # Therefore we must use space format for startswith() to match correctly
    for hour in {00..23}; do
      HOUR_SPACE="${CURRENT_DATE} ${hour}"
      extracted_value=$(echo "$day_data" | jq -r --arg hour "$HOUR_SPACE" \
        '[.[] | select(.t | startswith($hour))] | if length > 0 then (.[0].v | if . != null then tonumber else null end) else null end' \
        2>/dev/null || echo "null")
      
      if [ "$extracted_value" != "null" ]; then
        hours_extracted=$((hours_extracted + 1))
        
        # Save for debug output
        echo "${HOUR_SPACE}|${extracted_value}" >> "${TEMP_DIR}/extracted_${DATE_YYYYMMDD}.txt"
      fi
    done
  else
    # API error or no data
    error_msg=$(echo "$response" | jq -r '.error.message // "Connection failed"' 2>/dev/null || echo "Connection failed")
    log "WARNING: Failed to fetch data for $CURRENT_DATE: $error_msg"
  fi
  
  # Count hours in actual output files
  for hour in {00..23}; do
    hour_utc="${DATE_YYYYMMDD}T${hour}"
    stations_file="${DATA_DIR}/${YEAR}/stations_${hour_utc}Z.jsonl"
    
    if [ -f "$stations_file" ]; then
      # Check if this file has a record for our station with tide_height_ft
      record=$(jq -c --arg station "$STATION_ID" 'select(.station_id==$station) | select(.tide_height_ft != null)' "$stations_file" 2>/dev/null || echo "")
      if [ -n "$record" ]; then
        hours_in_output=$((hours_in_output + 1))
        
        # Save for debug output - use same format as extraction for easy comparison
        tide_value=$(echo "$record" | jq -r '.tide_height_ft')
        echo "${CURRENT_DATE} ${hour}|${tide_value}" >> "${TEMP_DIR}/output_${DATE_YYYYMMDD}.txt"
      fi
    fi
  done
  
  # Determine status
  status="✓ OK"
  if [ "$hours_in_noaa" -eq 0 ] && [ "$hours_extracted" -eq 0 ] && [ "$hours_in_output" -eq 0 ]; then
    status="✓ OK (no data)"
    TOTAL_OK=$((TOTAL_OK + 1))
  elif [ "$hours_in_noaa" -ne "$hours_extracted" ] || [ "$hours_extracted" -ne "$hours_in_output" ]; then
    missing_count=$((hours_extracted - hours_in_output))
    if [ "$missing_count" -gt 0 ]; then
      status="✗ MISSING ${missing_count}"
    elif [ "$missing_count" -lt 0 ]; then
      status="✗ EXTRA $((missing_count * -1))"
    else
      # NOAA vs extracted mismatch
      extraction_diff=$((hours_in_noaa - hours_extracted))
      status="✗ EXTRACTION FAIL ${extraction_diff}"
    fi
    TOTAL_DISCREPANCIES=$((TOTAL_DISCREPANCIES + 1))
    
    # Build debug output for this date
    DEBUG_OUTPUT="${DEBUG_OUTPUT}\n\n=== DEBUG: ${CURRENT_DATE} ===\n"
    
    # Show sample NOAA API response (first 3 records)
    if [ -n "$day_data" ] && [ "$day_data" != "[]" ]; then
      DEBUG_OUTPUT="${DEBUG_OUTPUT}\nNOAA API Response (first 3 records):\n"
      sample_api=$(echo "$day_data" | jq -c '.[:3][]' 2>/dev/null || echo "")
      if [ -n "$sample_api" ]; then
        while IFS= read -r line; do
          DEBUG_OUTPUT="${DEBUG_OUTPUT}  ${line}\n"
        done <<< "$sample_api"
      fi
    fi
    
    # Show extracted values (first 3)
    if [ -f "${TEMP_DIR}/extracted_${DATE_YYYYMMDD}.txt" ]; then
      DEBUG_OUTPUT="${DEBUG_OUTPUT}\nExtracted via jq (first 3 values):\n"
      sample_extracted=$(head -3 "${TEMP_DIR}/extracted_${DATE_YYYYMMDD}.txt" 2>/dev/null | cut -d'|' -f2 || echo "")
      if [ -n "$sample_extracted" ]; then
        while IFS= read -r line; do
          DEBUG_OUTPUT="${DEBUG_OUTPUT}  ${line}\n"
        done <<< "$sample_extracted"
      fi
    fi
    
    # Show actual output file records (first 3)
    DEBUG_OUTPUT="${DEBUG_OUTPUT}\nActual Output File (first 3 records with station 9410135):\n"
    output_records=""
    record_count=0
    for hour in {00..23}; do
      if [ "$record_count" -ge 3 ]; then
        break
      fi
      hour_utc="${DATE_YYYYMMDD}T${hour}"
      stations_file="${DATA_DIR}/${YEAR}/stations_${hour_utc}Z.jsonl"
      if [ -f "$stations_file" ]; then
        record=$(jq -c --arg station "$STATION_ID" 'select(.station_id==$station)' "$stations_file" 2>/dev/null || echo "")
        if [ -n "$record" ]; then
          if [ -z "$output_records" ]; then
            output_records="$record"
          else
            output_records="${output_records}"$'\n'"${record}"
          fi
          record_count=$((record_count + 1))
        fi
      fi
    done
    if [ -n "$output_records" ]; then
      while IFS= read -r line; do
        DEBUG_OUTPUT="${DEBUG_OUTPUT}  ${line}\n"
      done <<< "$output_records"
    else
      DEBUG_OUTPUT="${DEBUG_OUTPUT}  (no records found)\n"
    fi
    
    # Show the jq filter being used
    DEBUG_OUTPUT="${DEBUG_OUTPUT}\njq Filter Used:\n"
    DEBUG_OUTPUT="${DEBUG_OUTPUT}  [.data[] | select(.t | startswith(\$hour))] | if length > 0 then (.[0].v | tonumber) else null end\n"
    
    # Show HOUR_ISO and HOUR_SPACE examples
    DEBUG_OUTPUT="${DEBUG_OUTPUT}\nHOUR_ISO: ${CURRENT_DATE}T00\n"
    DEBUG_OUTPUT="${DEBUG_OUTPUT}HOUR_SPACE: ${CURRENT_DATE} 00\n"
  else
    TOTAL_OK=$((TOTAL_OK + 1))
  fi
  
  # Accumulate totals
  TOTAL_HOURS_NOAA=$((TOTAL_HOURS_NOAA + hours_in_noaa))
  TOTAL_HOURS_EXTRACTED=$((TOTAL_HOURS_EXTRACTED + hours_extracted))
  TOTAL_HOURS_OUTPUT=$((TOTAL_HOURS_OUTPUT + hours_in_output))
  
  # Print row
  printf "%-12s | %-12s | %-11s | %-8s | %s\n" "$CURRENT_DATE" "$hours_in_noaa" "$hours_extracted" "$hours_in_output" "$status"
  
  # Move to next day
  CURRENT_EPOCH=$((CURRENT_EPOCH + 86400))
done

# Print totals row
printf "%.12s-+-%.12s-+-%.11s-+-%.8s-+-%s\n" "------------" "------------" "-----------" "--------" "---------"
printf "%-12s | %-12s | %-11s | %-8s | %s\n" "TOTAL" "$TOTAL_HOURS_NOAA" "$TOTAL_HOURS_EXTRACTED" "$TOTAL_HOURS_OUTPUT" ""

# Print summary
printf "\n"
log "========================================="
log "SUMMARY"
log "========================================="
log "Total days checked: $TOTAL_DAYS"
log "Days with all data OK: $TOTAL_OK"
log "Days with discrepancies: $TOTAL_DISCREPANCIES"

# Print debug output if there were any discrepancies
if [ "$TOTAL_DISCREPANCIES" -gt 0 ]; then
  echo -e "$DEBUG_OUTPUT"
fi

log "========================================="
log "Verification complete"

# Exit with error code if there were discrepancies
if [ "$TOTAL_DISCREPANCIES" -gt 0 ]; then
  exit 1
else
  exit 0
fi
