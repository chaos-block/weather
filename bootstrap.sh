#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/chaos-block/weather.git"
RUN_DIR="$(pwd)"  # Persistent directory where bootstrap is run
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="${RUN_DIR}/logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Minimal bootstrap started – fresh fetch"

# Fresh temp workspace
WORK_DIR=$(mktemp -d /tmp/weather.XXXXXX)
log "Cloning fresh repo to $WORK_DIR"
git clone "$REPO_URL" "$WORK_DIR"

# Load persistent local conf.env (custom tokens preserved)
if [ -f "$RUN_DIR/conf.env" ]; then
    cp "$RUN_DIR/conf.env" "$WORK_DIR/conf.env"
    log "Loaded local persistent conf.env"
else
    log "ERROR: No local conf.env in $RUN_DIR – create from conf.env.example"
    exit 1
fi

cd "$WORK_DIR"

chmod +x pull_stations.sh pull_radar.sh pull_ais.sh pull_historical.sh archive.sh 2>/dev/null || true
log "Ensured script executables"

source conf.env

mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}"

log "Bootstrap complete – fresh scripts in $WORK_DIR"
log "Run pulls manually: ./pull_stations.sh (stations), ./pull_radar.sh (radar), ./pull_ais.sh (AIS)"
log "Data outputs: /data/current/*.jsonl (last 72h)"

# Optional auto-execution
MODE="${1:-}"
if [ "$MODE" = "1" ]; then
    log "Auto-running stations only (mode 1)"
    ./pull_stations.sh
elif [ "$MODE" = "2" ]; then
    log "Auto-running radar only (mode 2)"
    ./pull_radar.sh
elif [ "$MODE" = "all" ]; then
    log "Auto-running all pulls"
    ./pull_stations.sh && ./pull_radar.sh && ./pull_ais.sh
elif [ "$MODE" = "historical" ]; then
    # historical N - Pull last N days
    DAYS="${2:-3}"
    START_DATE=$(date -u -d "${DAYS} days ago" +%Y-%m-%d)
    END_DATE=$(date -u +%Y-%m-%d)
    log "Running historical pull for last ${DAYS} days (${START_DATE} → ${END_DATE})"
    ./pull_historical.sh "$START_DATE" "$END_DATE" no-nulls
elif [ "$MODE" = "historical_range" ]; then
    # historical_range START END - Custom date range
    if [ $# -lt 3 ]; then
        log "ERROR: historical_range requires START_DATE and END_DATE"
        log "Usage: $0 historical_range YYYY-MM-DD YYYY-MM-DD [no-nulls]"
        exit 1
    fi
    START_DATE="$2"
    END_DATE="$3"
    NULLS_FLAG="${4:-no-nulls}"
    log "Running historical pull for custom range (${START_DATE} → ${END_DATE})"
    ./pull_historical.sh "$START_DATE" "$END_DATE" "$NULLS_FLAG"
elif [ "$MODE" = "historical_full" ]; then
    # historical_full - Full archive from 2015 to present
    START_DATE="2015-01-01"
    END_DATE=$(date -u +%Y-%m-%d)
    log "WARNING: Running FULL historical pull from 2015 to present"
    log "This will take several days/weeks to complete"
    read -p "Are you sure? (yes/no) " -r
    echo
    if [[ $REPLY =~ ^yes$ ]]; then
        log "Starting full historical pull (${START_DATE} → ${END_DATE})"
        ./pull_historical.sh "$START_DATE" "$END_DATE" no-nulls
    else
        log "Full historical pull cancelled"
    fi
elif [ "$MODE" = "verify" ]; then
    # verify START END - Audit data completeness
    if [ $# -lt 3 ]; then
        log "ERROR: verify requires START_DATE and END_DATE"
        log "Usage: $0 verify YYYY-MM-DD YYYY-MM-DD"
        exit 1
    fi
    START_DATE="$2"
    END_DATE="$3"
    log "Running data verification for (${START_DATE} → ${END_DATE})"
    
    # Verification script - check existing data
    current_date="$START_DATE"
    END_EPOCH=$(date -d "$END_DATE" +%s)
    
    while [ "$(date -d "$current_date" +%s)" -lt "$END_EPOCH" ]; do
        date_yyyymmdd=$(date -d "$current_date" +%Y%m%d)
        
        # Check station files
        stations_count=$(find "${CURRENT_DIR}" "${DATA_DIR}/historical" -name "stations_${date_yyyymmdd}*.jsonl" -type f 2>/dev/null -exec cat {} + 2>/dev/null | wc -l)
        radar_count=$(find "${CURRENT_DIR}" "${DATA_DIR}/historical" -name "radar_${date_yyyymmdd}*.jsonl" -type f 2>/dev/null -exec cat {} + 2>/dev/null | wc -l)
        ais_count=$(find "${CURRENT_DIR}" "${DATA_DIR}/historical" -name "ais_${date_yyyymmdd}*.jsonl" -type f 2>/dev/null -exec cat {} + 2>/dev/null | wc -l)
        
        log "${current_date}: Stations=${stations_count:-0} Radar=${radar_count:-0} AIS=${ais_count:-0}"
        
        # Move to next day
        current_date=$(date -d "$current_date + 1 day" +%Y-%m-%d)
    done
    
    log "Verification complete"
fi

