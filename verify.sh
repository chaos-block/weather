#!/bin/bash
set -euo pipefail

# =============================================================================
# verify.sh – 14-day rolling verification of data completeness
# Verifies data for a date range (defaults to last 14 days)
# Checks which fields are present and logs missing expected fields per station
# Usage: ./verify.sh [DAYS]                    (defaults to last 14 days)
#        ./verify.sh YYYY-MM-DD YYYY-MM-DD     (date range)
# =============================================================================

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

mkdir -p "$LOGS_DIR"
LOG_FILE="${LOGS_DIR}/verify.log"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] verify: $1" | tee -a "$LOG_FILE"; }

# Parse arguments
if [ $# -eq 0 ]; then
  # Default: last 14 days
  END_DATE=$(date -u +%Y-%m-%d)
  START_DATE=$(date -u -d '14 days ago' +%Y-%m-%d)
elif [ $# -eq 1 ]; then
  # Days backward
  END_DATE=$(date -u +%Y-%m-%d)
  START_DATE=$(date -u -d "$1 days ago" +%Y-%m-%d)
else
  # Date range
  START_DATE="$1"
  END_DATE="$2"
fi

log "Verifying data from $START_DATE to $END_DATE"

CURRENT_DATE="$START_DATE"
END_EPOCH=$(date -d "$END_DATE" +%s)

TOTAL_RECORDS=0

while [ "$(date -d "$CURRENT_DATE" +%s)" -lt "$END_EPOCH" ]; do
  DATE_YYYYMMDD=$(date -d "$CURRENT_DATE" +%Y%m%d)
  YEAR=$(date -d "$CURRENT_DATE" +%Y)
  
  log "Verifying $CURRENT_DATE"
  
  # Get list of all station IDs from STATIONS_LIST
  STATION_IDS=$(echo "$STATIONS_LIST" | grep -v '^$' | grep -E '^[A-Z0-9-]+' | cut -d'|' -f1,6)
  
  # For each station, check what data exists
  echo "$STATION_IDS" | while IFS='|' read -r station_id expected_fields; do
    [ -z "$station_id" ] && continue
    
    # Count records for this station across all hours of the day
    count=0
    for hour in {00..23}; do
      hour_utc="${DATE_YYYYMMDD}T${hour}"
      stations_file="${DATA_DIR}/${YEAR}/stations_${hour_utc}Z.jsonl"
      
      if [ -f "$stations_file" ]; then
        records=$(jq -c "select(.station_id==\"$station_id\")" "$stations_file" 2>/dev/null | wc -l || echo "0")
        count=$((count + records))
      fi
    done
    
    if [ "$count" -gt 0 ]; then
      # Extract one record to check which fields are present
      record=$(find "${DATA_DIR}/${YEAR}" -name "stations_${DATE_YYYYMMDD}*.jsonl" -type f 2>/dev/null \
        -exec jq -c "select(.station_id==\"$station_id\")" {} + 2>/dev/null | head -1)
      
      if [ -n "$record" ]; then
        # Count fields in this record
        field_count=$(echo "$record" | jq 'keys | length' 2>/dev/null || echo "0")
        log "  $station_id: $count records, $field_count fields"
        
        # Check for expected fields that are missing
        # Parse expected fields and check each one
        if [ -n "$expected_fields" ]; then
          IFS=',' read -ra FIELDS <<< "$expected_fields"
          for field in "${FIELDS[@]}"; do
            if ! echo "$record" | jq -e ".$field" >/dev/null 2>&1; then
              log "    WARNING: $field missing for $station_id (expected but got nothing)"
            fi
          done
        fi
      fi
    else
      log "  $station_id: NO DATA (0 records)"
    fi
  done
  
  # Move to next day
  CURRENT_DATE=$(date -d "$CURRENT_DATE + 1 day" +%Y-%m-%d)
done

log "✅ Verification complete"
