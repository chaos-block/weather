#!/bin/bash
set -euo pipefail

# =============================================================================
# bootstrap.sh – Main orchestration script for weather data collection
# Simplified architecture with /data/YYYY/ structure
# Modes:
#   ./bootstrap.sh                                      # Default: pull last 72 hours (all 3 products)
#   ./bootstrap.sh pull 2025-12-24                      # Pull last 72 hours ending at date
#   ./bootstrap.sh pull 2025-12-22 2025-12-26           # Pull specific date range
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

# Auto-update bootstrap.sh from GitHub
auto_update_bootstrap() {
  log "Checking for bootstrap.sh updates from GitHub..."
  
  # Fetch latest from origin
  if ! git fetch origin main --quiet 2>/dev/null; then
    log "WARNING: Could not fetch from GitHub (offline?), continuing with local version"
    return 0
  fi
  
  # Check if remote version differs from local version
  if git diff --quiet origin/main -- bootstrap.sh 2>/dev/null; then
    # Local bootstrap is up-to-date
    return 0
  fi
  
  # Newer version exists on GitHub
  log "Newer version of bootstrap.sh available, updating from GitHub..."
  
  if git checkout origin/main -- bootstrap.sh 2>/dev/null; then
    log "Bootstrap updated successfully - re-executing with updated version"
    # Re-execute this script with updated version
    exec "$0" "$@"
  else
    log "WARNING: Could not update bootstrap.sh (permission denied?), continuing with local version"
    return 0
  fi
}

# Validate all required scripts exist
validate_scripts() {
  local required_scripts=(
    "pull_stations.sh"
    "pull_radar.sh"
    "pull_ais.sh"
    "verify.sh"
    "bundle.sh"
  )
  
  local missing=0
  for script in "${required_scripts[@]}"; do
    if [ ! -f "$script" ]; then
      log "ERROR: Required script not found: $script"
      missing=1
    fi
  done
  
  if [ $missing -eq 1 ]; then
    log "ERROR: One or more required scripts are missing"
    log "ERROR: Restore missing scripts from your git repository"
    exit 1
  fi
}

# Call at startup
auto_update_bootstrap
validate_scripts

MODE="${1:-default}"

# Helper function to pull data for a specific hour (used by both default and pull modes)
pull_hour() {
  local HOUR_TIMESTAMP="$1"
  local HOUR_UTC="$2"
  local PRODUCT="$3"
  
  if ! OVERRIDE_TIMESTAMP="$HOUR_TIMESTAMP" "./pull_${PRODUCT}.sh"; then
    log "WARNING: ${PRODUCT^} pull failed for $HOUR_UTC"
  fi
}

case "$MODE" in
  pull)
    log "Starting historical pull for date range"
    
    # Parse arguments
    if [ $# -eq 2 ]; then
      # Single date: pull last 72 hours ending at that date
      END_DATE="$2"
      START_DATE=$(date -u -d "$END_DATE 00:00:00 UTC - 72 hours" +%Y-%m-%d 2>/dev/null) || {
        log "ERROR: Invalid date format: $END_DATE"
        exit 1
      }
    elif [ $# -eq 3 ]; then
      # Date range
      START_DATE="$2"
      END_DATE="$3"
    else
      log "ERROR: Invalid arguments for pull mode"
      log "Usage: $0 pull YYYY-MM-DD [END_DATE]"
      exit 1
    fi
    
    # Validate dates
    START_EPOCH=$(date -u -d "$START_DATE 00:00:00" +%s 2>/dev/null) || {
      log "ERROR: Invalid START_DATE format: $START_DATE"
      exit 1
    }
    END_EPOCH=$(date -u -d "$END_DATE + 1 day" +%s 2>/dev/null) || {
      log "ERROR: Invalid END_DATE format: $END_DATE"
      exit 1
    }
    
    if [ $START_EPOCH -ge $END_EPOCH ]; then
      log "ERROR: START_DATE must be before END_DATE"
      exit 1
    fi
    
    TOTAL_HOURS=$(( (END_EPOCH - START_EPOCH) / 3600 ))
    log "Date range: ${START_DATE}T00:00:00Z → ${END_DATE}T23:59:59Z ($TOTAL_HOURS hours)"
    
    # Loop hour-by-hour through date range
    CURRENT_EPOCH=$START_EPOCH
    HOURS_COMPLETED=0
    
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
    log "Usage: $0 [pull|verify|bundle|default]"
    log "  default  - Pull last 72 hours (all 3 products)"
    log "  pull     - Pull historical date range"
    log "  verify   - Verify data completeness"
    log "  bundle   - Bundle monthly data"
    exit 1
    ;;
esac

log "Bootstrap complete"

