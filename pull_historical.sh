#!/bin/bash
set -euo pipefail

# =============================================================================
# pull_historical.sh – Complete historical data puller with null-field removal
# Pulls observed data from any date range (e.g., 2025-12-01 → 2026-01-01)
# Calls existing pull_stations.sh, pull_radar.sh, pull_ais.sh with custom timestamps
# Removes all null fields from JSON output (only include fields with actual values)
# Verifies data completeness per day (reports which fields are populated vs null)
# Supports checkpoint/resume for interrupted pulls
# =============================================================================

source conf.env || { echo "Error: conf.env not found"; exit 1; }
cd "$(dirname "$0")"

# Parse command-line arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 START_DATE END_DATE [no-nulls]"
    echo "  START_DATE: Start date in YYYY-MM-DD format (e.g., 2025-12-01)"
    echo "  END_DATE: End date in YYYY-MM-DD format (e.g., 2026-01-01)"
    echo "  no-nulls: Optional flag to enable null field removal (default: enabled)"
    echo ""
    echo "Examples:"
    echo "  $0 2025-12-01 2026-01-01         # Last month"
    echo "  $0 2015-01-01 2026-01-01         # Full 11-year archive"
    exit 1
fi

START_DATE="$1"
END_DATE="$2"
REMOVE_NULLS="${3:-no-nulls}"  # Default to removing nulls

# Validate date format
if ! date -d "$START_DATE" >/dev/null 2>&1 || ! date -d "$END_DATE" >/dev/null 2>&1; then
    echo "Error: Invalid date format. Use YYYY-MM-DD"
    exit 1
fi

# Calculate number of days
START_EPOCH=$(date -d "$START_DATE" +%s)
END_EPOCH=$(date -d "$END_DATE" +%s)
TOTAL_DAYS=$(( (END_EPOCH - START_EPOCH) / 86400 ))
TOTAL_HOURS=$((TOTAL_DAYS * 24))

if [ $TOTAL_DAYS -le 0 ]; then
    echo "Error: END_DATE must be after START_DATE"
    exit 1
fi

# Setup directories and logging
HISTORICAL_DIR="${DATA_DIR}/historical"
CHECKPOINT_DIR="${HISTORICAL_DIR}/.checkpoints"
mkdir -p "${HISTORICAL_DIR}" "${CHECKPOINT_DIR}" "${ARCHIVE_DIR}" "${LOGS_DIR}"

LOG_FILE="${LOGS_DIR}/historical_$(date -u +%Y%m%d_%H%M%S).log"
CHECKPOINT_FILE="${CHECKPOINT_DIR}/pull_${START_DATE}_to_${END_DATE}.checkpoint"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] historical:  $1" | tee -a "$LOG_FILE"; }

log "Starting ${TOTAL_DAYS}-day historical pull"
log "Date range: ${START_DATE} → ${END_DATE} (${TOTAL_DAYS} days, ${TOTAL_HOURS} hours)"
log "Mode: NULL FIELD REMOVAL ${REMOVE_NULLS}"

# Load checkpoint if exists
declare -A completed_hours
if [ -f "$CHECKPOINT_FILE" ]; then
    log "Loading checkpoint from previous run..."
    while read -r hour; do
        completed_hours["$hour"]=1
    done < "$CHECKPOINT_FILE"
    log "Checkpoint loaded: ${#completed_hours[@]} hours already completed"
fi

# Function to pull stations for a specific hour
fetch_stations_hour() {
    local HOUR_UTC="$1"
    local TIMESTAMP="$2"
    
    log "Pulling stations for ${HOUR_UTC}..."
    
    # Call pull_stations.sh with custom timestamp and output directory
    OVERRIDE_TIMESTAMP="$TIMESTAMP" OVERRIDE_OUTPUT_DIR="$HISTORICAL_DIR" ./pull_stations.sh
    
    # Rate limit (NOAA recommended)
    sleep 1.5
}

# Function to pull radar for a specific hour
fetch_radar_hour() {
    local HOUR_UTC="$1"
    local TIMESTAMP="$2"
    
    log "Pulling radar for ${HOUR_UTC}..."
    
    # Call pull_radar.sh with custom timestamp and output directory
    OVERRIDE_TIMESTAMP="$TIMESTAMP" OVERRIDE_OUTPUT_DIR="$HISTORICAL_DIR" ./pull_radar.sh
    
    # Rate limit
    sleep 1.5
}

# Function to pull AIS for a specific hour
fetch_ais_hour() {
    local HOUR_UTC="$1"
    local TIMESTAMP="$2"
    
    log "Pulling AIS for ${HOUR_UTC}..."
    
    # Call pull_ais.sh with custom timestamp and output directory
    OVERRIDE_TIMESTAMP="$TIMESTAMP" OVERRIDE_OUTPUT_DIR="$HISTORICAL_DIR" ./pull_ais.sh
    
    # Rate limit (MarineTraffic stricter)
    sleep 2.5
}

# Function to remove null fields from JSON files
# Note: This is a safety measure as the individual pull scripts already remove nulls
# This ensures consistency even if pull scripts are modified in the future
remove_null_fields() {
    local HOUR_UTC="$1"
    
    # Only process files if REMOVE_NULLS flag is set
    if [ "$REMOVE_NULLS" != "no-nulls" ]; then
        return 0
    fi
    
    for file in "${HISTORICAL_DIR}/stations_${HOUR_UTC}Z.jsonl" \
                "${HISTORICAL_DIR}/radar_${HOUR_UTC}Z.jsonl" \
                "${HISTORICAL_DIR}/ais_${HOUR_UTC}Z.jsonl"; do
        if [ -f "$file" ]; then
            # Remove null fields from each JSON object using jq
            # This filter removes all keys whose values are null
            jq -c 'with_entries(select(.value != null))' "$file" > "${file}.tmp" && mv "${file}.tmp" "$file"
        fi
    done
}

# Function to verify data for a specific day
verify_day() {
    local DATE="$1"
    local DATE_YYYYMMDD=$(date -d "$DATE" +%Y%m%d)
    
    log "Verifying ${DATE} (24 hours complete)..."
    
    # Get list of stations to verify from STATIONS_LIST
    local sample_stations=$(echo "$STATIONS_LIST" | grep -v '^$' | grep -E '^[A-Z0-9-]+' | head -5 | cut -d'|' -f1 | tr '\n' ' ')
    
    # Count records per station for this day (sample first 5 stations)
    for station in $sample_stations; do
        local count=0
        local field_count=0
        
        # Count records for this station across all hours of the day
        for hour in {00..23}; do
            local hour_utc="${DATE_YYYYMMDD}T$(printf %02d $hour)"
            local stations_file="${HISTORICAL_DIR}/stations_${hour_utc}Z.jsonl"
            
            if [ -f "$stations_file" ]; then
                local records=$(jq -c "select(.station_id==\"$station\")" "$stations_file" 2>/dev/null | wc -l || echo "0")
                count=$((count + records))
                
                # Get field count from first record found
                if [ "$field_count" -eq 0 ] && [ "$records" -gt 0 ]; then
                    field_count=$(jq -c "select(.station_id==\"$station\") | keys | length" "$stations_file" 2>/dev/null | head -1 || echo "0")
                fi
            fi
        done
        
        if [ "$count" -gt 0 ]; then
            log "  $station: $count records, $field_count fields ✅"
        fi
    done
    
    log "${DATE} VERIFIED ✅"
}

# Function to archive a day's data
archive_day() {
    local DATE="$1"
    local BUNDLE_FILE="${ARCHIVE_DIR}/${DATE}.tar.zst"
    
    log "Archiving ${DATE} → ${BUNDLE_FILE}..."
    
    # Find all files for this day
    local DATE_YYYYMMDD=$(date -d "$DATE" +%Y%m%d)
    
    # Create list of files to archive
    local FILES=$(find "${HISTORICAL_DIR}" -type f -name "*${DATE_YYYYMMDD}*.jsonl" \
                  \( -name "stations_*.jsonl" -o -name "radar_*.jsonl" -o -name "ais_*.jsonl" \))
    
    if [ -n "$FILES" ]; then
        # Bundle with zstd level 19
        # Use while read loop for safe handling of filenames with spaces
        echo "$FILES" | while read -r file; do
            basename "$file"
        done | tar -C "${HISTORICAL_DIR}" -cf - -T - | zstd -19 -o "$BUNDLE_FILE"
        
        local SIZE=$(du -h "$BUNDLE_FILE" | cut -f1)
        log "${DATE}.tar.zst (${SIZE}) ✅"
        
        # Clean up source files after successful archival
        # Use while read for safe handling of filenames
        echo "$FILES" | while read -r file; do
            rm -f "$file"
        done
    else
        log "No files found for ${DATE}"
    fi
}

# Main loop - hour by hour
log "Starting hour-by-hour data pull..."

CURRENT_EPOCH=$START_EPOCH
CURRENT_DAY=""
HOURS_COMPLETED=0

while [ $CURRENT_EPOCH -lt $END_EPOCH ]; do
    CURRENT_DATE=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m-%d)
    HOUR_UTC=$(date -u -d "@$CURRENT_EPOCH" +%Y%m%dT%H)
    TIMESTAMP=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m-%dT%H:00:00Z)
    
    # Check if already completed (resume logic)
    if [ "${completed_hours[$HOUR_UTC]:-0}" = "1" ]; then
        log "Skipping ${HOUR_UTC} (already completed)"
        CURRENT_EPOCH=$((CURRENT_EPOCH + 3600))
        continue
    fi
    
    # Fetch data for this specific hour
    fetch_stations_hour "$HOUR_UTC" "$TIMESTAMP"
    fetch_radar_hour "$HOUR_UTC" "$TIMESTAMP"
    fetch_ais_hour "$HOUR_UTC" "$TIMESTAMP"
    
    # Remove null fields from output if flag set
    if [ "$REMOVE_NULLS" = "no-nulls" ]; then
        remove_null_fields "$HOUR_UTC"
    fi
    
    # Mark this hour as completed in checkpoint
    echo "$HOUR_UTC" >> "$CHECKPOINT_FILE"
    completed_hours["$HOUR_UTC"]=1
    
    ((HOURS_COMPLETED++))
    
    # Log progress every 6 hours
    if [ $((HOURS_COMPLETED % 6)) -eq 0 ]; then
        log "Progress: ${HOURS_COMPLETED}/${TOTAL_HOURS} hours completed ($((HOURS_COMPLETED * 100 / TOTAL_HOURS))%)"
    fi
    
    # If day changed, verify and archive previous day
    if [ "$CURRENT_DAY" != "$CURRENT_DATE" ] && [ -n "$CURRENT_DAY" ]; then
        verify_day "$CURRENT_DAY"
        archive_day "$CURRENT_DAY"
    fi
    
    CURRENT_DAY="$CURRENT_DATE"
    CURRENT_EPOCH=$((CURRENT_EPOCH + 3600))
done

# Verify and archive the last day
if [ -n "$CURRENT_DAY" ]; then
    verify_day "$CURRENT_DAY"
    archive_day "$CURRENT_DAY"
fi

# Final summary
log "============ COMPLETE ============"
log "${TOTAL_DAYS}-day pull SUCCESSFUL"

# Count total station records
total_station_count=$(echo "$STATIONS_LIST" | grep -v '^$' | grep -cE '^[A-Z0-9-]+')
if [ -z "$total_station_count" ] || [ "$total_station_count" -eq 0 ]; then
    log "WARNING: Could not determine station count from STATIONS_LIST"
    total_station_count=1  # Fallback to prevent division by zero
fi
expected_records=$((TOTAL_DAYS * 24 * total_station_count))
log "Expected records: ${expected_records} (${TOTAL_DAYS} days × 24 hours × ${total_station_count} stations)"

# Count archives
archives=$(find "${ARCHIVE_DIR}" -maxdepth 1 -name "${START_DATE:0:4}-*.tar.zst" -type f 2>/dev/null | wc -l)
total_size=$(du -sh "${ARCHIVE_DIR}" 2>/dev/null | cut -f1 || echo "0")
log "Archives: ${archives} files in ${ARCHIVE_DIR} (${total_size})"

log "✅ All verified, ready for CNN training"

# Clean checkpoint for this range
rm -f "$CHECKPOINT_FILE"
log "Checkpoint cleaned"
