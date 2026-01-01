#!/bin/bash
set -euo pipefail

# =============================================================================
# bootstrap.sh – Main orchestration script for weather data collection
# Simplified architecture with /data/YYYY/ structure
# Modes:
#   ./bootstrap.sh                                      # Default: pull last 72 hours (all 3 products)
#   ./bootstrap.sh verify                               # Verify last 14 days
#   ./bootstrap.sh verify 7                             # Verify last 7 days
#   ./bootstrap.sh verify 2025-12-01 2025-12-31         # Verify date range
#   ./bootstrap.sh bundle                               # Bundle previous month
#   ./bootstrap.sh bundle 2025-12                       # Bundle specific month
# =============================================================================

cd "$(dirname "$0")"

TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

# Load configuration
if [ ! -f "conf.env" ]; then
    log "ERROR: No conf.env found – create from conf.env.example"
    exit 1
fi

source conf.env

# Ensure scripts are executable
chmod +x pull_stations.sh pull_radar.sh pull_ais.sh bundle.sh verify.sh 2>/dev/null || true

MODE="${1:-default}"

case "$MODE" in
  verify)
    log "Starting verification mode"
    if [ $# -eq 1 ]; then
      # Default: last 14 days
      ./verify.sh
    elif [ $# -eq 2 ]; then
      # Verify last N days
      ./verify.sh "$2"
    elif [ $# -eq 3 ]; then
      # Verify date range
      ./verify.sh "$2" "$3"
    else
      log "ERROR: Invalid arguments for verify mode"
      log "Usage: $0 verify [DAYS | START_DATE END_DATE]"
      exit 1
    fi
    ;;
    
  bundle)
    log "Starting bundle mode"
    if [ $# -eq 1 ]; then
      # Bundle previous month
      ./bundle.sh
    elif [ $# -eq 2 ]; then
      # Bundle specific month
      ./bundle.sh "$2"
    else
      log "ERROR: Invalid arguments for bundle mode"
      log "Usage: $0 bundle [YYYY-MM]"
      exit 1
    fi
    ;;
    
  default)
    log "Minimal bootstrap started"
    log "Running all pulls for last 72 hours"
    
    # Helper function to pull data for a specific hour
    pull_hour() {
      local HOUR_TIMESTAMP="$1"
      local HOUR_UTC="$2"
      local PRODUCT="$3"
      
      if ! OVERRIDE_TIMESTAMP="$HOUR_TIMESTAMP" "./pull_${PRODUCT}.sh"; then
        log "WARNING: ${PRODUCT^} pull failed for $HOUR_UTC"
      fi
    }
    
    # Calculate date range for last 72 hours
    END_HOUR=$(date -u +%Y-%m-%dT%H:00:00Z)
    START_HOUR=$(date -u -d '72 hours ago' +%Y-%m-%dT%H:00:00Z)
    
    log "Data range: $START_HOUR → $END_HOUR"
    
    # Pull data for each hour in the last 72 hours
    CURRENT_EPOCH=$(date -u -d "$START_HOUR" +%s)
    END_EPOCH=$(date -u -d "$END_HOUR" +%s)
    
    HOURS_COMPLETED=0
    TOTAL_HOURS=72
    
    while [ $CURRENT_EPOCH -lt $END_EPOCH ]; do
      HOUR_TIMESTAMP=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m-%dT%H:00:00Z)
      HOUR_UTC=$(date -u -d "@$CURRENT_EPOCH" +%Y%m%dT%H)
      YEAR=$(date -u -d "@$CURRENT_EPOCH" +%Y)
      
      log "Starting pull for ${HOUR_UTC}"
      log "Data directory: ${DATA_DIR}/${YEAR}/"
      
      # Pull stations, radar, and AIS for this hour
      pull_hour "$HOUR_TIMESTAMP" "$HOUR_UTC" "stations"
      pull_hour "$HOUR_TIMESTAMP" "$HOUR_UTC" "radar"
      pull_hour "$HOUR_TIMESTAMP" "$HOUR_UTC" "ais"
      
      ((HOURS_COMPLETED++))
      
      # Log progress every 12 hours
      if [ $((HOURS_COMPLETED % 12)) -eq 0 ]; then
        log "Progress: ${HOURS_COMPLETED}/${TOTAL_HOURS} hours completed"
      fi
      
      # Move to next hour
      CURRENT_EPOCH=$((CURRENT_EPOCH + 3600))
    done
    
    log "All pulls complete"
    log "Data stored in: ${DATA_DIR}/YYYY/ (no subdirectories)"
    ;;
    
  *)
    log "ERROR: Unknown mode: $MODE"
    log "Usage: $0 [verify|bundle|default]"
    log "  default  - Pull last 72 hours (all 3 products)"
    log "  verify   - Verify data completeness"
    log "  bundle   - Bundle monthly data"
    exit 1
    ;;
esac

log "Bootstrap complete"

