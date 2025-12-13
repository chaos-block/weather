#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, pipe failures

REPO_URL="https://github.com/chaos-block/weather.git"
REPO_DIR="$(pwd)/weather"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

# Ensure logs directory exists early
mkdir -p "$LOG_DIR"

# Function to log (now safe)
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Bootstrap started"

# Clone or pull fresh repo
if [ ! -d "$REPO_DIR/.git" ]; then
    log "Cloning fresh repo from $REPO_URL"
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
else
    log "Pulling latest changes in existing repo"
    cd "$REPO_DIR"
    git pull origin main
fi

# Config handling (conf.env template)
if [ ! -f conf.env ]; then
    if [ -f conf.env.example ]; then
        cp conf.env.example conf.env
        log "Created conf.env from template. Edit with your MarineTraffic API key and paths."
    else
        log "ERROR: conf.env.example not found. Create conf.env manually with required vars."
        exit 1
    fi
fi

set -a
source conf.env
set +a

# Validate key vars
missing=()
[ -z "${MARINETRAFFIC_APIKEY:-}" ] && missing+=("MARINETRAFFIC_APIKEY")
[ -z "${DATA_DIR:-}" ] && missing+=("DATA_DIR")
if [ ${#missing[@]} -ne 0 ]; then
    log "ERROR: Missing required vars in conf.env: ${missing[*]}"
    exit 1
fi

# Ensure data directories
mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOGS_DIR"

log "Bootstrap complete. Ready for pulls (e.g., ./pull_ais.sh)"
log "Data paths: ${CURRENT_DIR} (last 72h), ${ARCHIVE_DIR} (daily bundles)"
