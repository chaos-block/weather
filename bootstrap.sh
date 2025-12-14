#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/chaos-block/weather.git"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Bootstrap started (ephemeral mode – fresh fetch every run)"

# Auto-install git (same as before)
# ... (retain package manager block)

# Fresh temp workspace
WORK_DIR=$(mktemp -d /tmp/weather.XXXXXX)
log "Fetching fresh repo to $WORK_DIR"
git clone "$REPO_URL" "$WORK_DIR"
cd "$WORK_DIR"

# Config: Prefer persistent local conf.env (from run dir), fallback to template
RUN_DIR="$(pwd)"  # Persistent run dir before cd to temp
if [ -f "$RUN_DIR/conf.env" ]; then
    cp "$RUN_DIR/conf.env" conf.env
    log "Loaded persistent local conf.env (custom tokens preserved)"
elif [ -f conf.env.example ]; then
    cp conf.env.example conf.env
    log "Created conf.env from template"
else
    log "ERROR: No conf.env"
    exit 1
fi

# Config handling
if [ -f conf.env ]; then
    log "Using existing conf.env"
elif [ -f conf.env.example ]; then
    cp conf.env.example conf.env
    log "Created conf.env from template"
else
    log "ERROR: No conf.env or template"
    exit 1
fi

set -a
source conf.env
set +a

mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOG_DIR"
log "Data directories ready"

# Mode-aware validation + execution
MODE="${1:-all}"

missing=()
if [ "$MODE" = "all" ]; then
    [ -z "${MARINETRAFFIC_APIKEY:-}" ] && missing+=("MARINETRAFFIC_APIKEY (required for AIS)")
fi
if [ ${#missing[@]} -ne 0 ]; then
    log "WARN: Missing vars for mode $MODE: ${missing[*]} (proceeding – affected pulls may fail)"
fi

log "Execution mode: $MODE"

if [ "$MODE" = "1" ] || [ "$MODE" = "all" ]; then
    if [ -x pull_stations.sh ]; then
        log "Running pull_stations.sh (stations only – no MarineTraffic needed)"
        ./pull_stations.sh
    else
        log "ERROR: pull_stations.sh missing/not executable"
    fi
fi

if [ "$MODE" = "2" ] || [ "$MODE" = "all" ]; then
    if [ -x pull_radar.sh ]; then
        log "Running pull_radar.sh"
        ./pull_radar.sh
    else
        log "WARN: pull_radar.sh missing – skipped"
    fi
fi

if [ "$MODE" = "all" ]; then
    if [ -x pull_ais.sh ]; then
        log "Running pull_ais.sh (requires MARINETRAFFIC_APIKEY)"
        ./pull_ais.sh
    else
        log "WARN: pull_ais.sh missing – skipped"
    fi
fi

log "Bootstrap complete – check /data/current/stations_*.jsonl for output"
