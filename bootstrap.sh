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
source conf.env

mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}"

log "Bootstrap complete – fresh scripts in $WORK_DIR"
log "Run pulls manually: ./pull_stations.sh (stations), ./pull_radar.sh (radar), ./pull_ais.sh (AIS)"
log "Data outputs: /data/current/*.jsonl (last 72h)"

# Optional auto-execution
if [ "${1:-}" = "1" ]; then
    log "Auto-running stations only (mode 1)"
    ./pull_stations.sh
elif [ "${1:-}" = "2" ]; then
    log "Auto-running radar only (mode 2)"
    ./pull_radar.sh
elif [ "${1:-}" = "all" ]; then
    log "Auto-running all pulls"
    ./pull_stations.sh && ./pull_radar.sh && ./pull_ais.sh
fi
