#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/chaos-block/weather.git"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

# Fix logging dir early
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Bootstrap started (ephemeral mode – fresh fetch every run)"

# Auto-install git (as before)
if ! command -v git >/dev/null; then
    log "git not found – auto-installing"
    # ... (same package manager block as previous)
fi

# Fresh temp workspace
WORK_DIR=$(mktemp -d /tmp/weather.XXXXXX)
log "Fetching fresh repo to $WORK_DIR"
git clone "$REPO_URL" "$WORK_DIR"
cd "$WORK_DIR"

# Config handling (persistent or from template)
if [ -f conf.env ]; then  # Assume persistent in current dir or adjust path
    log "Using existing conf.env"
elif [ -f conf.env.example ]; then
    cp conf.env.example conf.env
    log "Created conf.env from template – edit MARINETRAFFIC_APIKEY before pulls"
else
    log "ERROR: No conf.env or template – create manually"
    exit 1
fi

set -a
source conf.env
set +a

mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOG_DIR"
log "Data directories ready"

# Selective pull execution
MODE="${1:-all}"  # Default all; 1=stations, 2=radar

log "Execution mode: $MODE"

if [ "$MODE" = "1" ] || [ "$MODE" = "all" ]; then
    if [ -x pull_stations.sh ]; then
        log "Running pull_stations.sh (H-3 hourly observed stations)"
        ./pull_stations.sh
    else
        log "WARN: pull_stations.sh missing or not executable – stations skipped"
    fi
fi

if [ "$MODE" = "2" ] || [ "$MODE" = "all" ]; then
    if [ -x pull_radar.sh ]; then
        log "Running pull_radar.sh (H-3 hourly radar grid)"
        ./pull_radar.sh
    else
        log "WARN: pull_radar.sh missing – radar skipped"
    fi
fi

if [ "$MODE" = "all" ]; then
    if [ -x pull_ais.sh ]; then
        log "Running pull_ais.sh (H-3 hourly AIS positions)"
        ./pull_ais.sh
    else
        log "WARN: pull_ais.sh missing – AIS skipped"
    fi
fi

log "Bootstrap complete (workspace: $WORK_DIR retained temporarily)"
log "Data in ${CURRENT_DIR} (check ls /data/current/*.jsonl)"

# Cleanup old temp dirs
find /tmp -type d -name 'weather.*' -mtime +1 -exec rm -rf {} + 2>/dev/null || true
