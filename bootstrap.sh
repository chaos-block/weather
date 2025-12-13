#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, pipe failures

REPO_URL="https://github.com/chaos-block/weather.git"
REPO_DIR="$(pwd)/weather"
LOG_FILE="logs/bootstrap_$(date +%Y%m%d_%H%M%S).log"

# Function to log
log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

# Check if in repo dir or clone fresh
if [ ! -d "$REPO_DIR/.git" ]; then
    log "Cloning fresh repo from $REPO_URL"
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
else
    log "Pulling latest changes"
    cd "$REPO_DIR"
    git pull origin main
fi

# Copy and load .env (create from example if missing)
if [ ! -f .env ]; then
    cp .env.example .env
    log "Created .env from template. Edit with your API keys before running."
    exit 1
fi
set -a  # Auto-export vars
source .env
set +a

# Validate required configs
missing=()
[ -z "${NOAA_TOKEN:-}" ] && missing+=("NOAA_TOKEN")
[ -z "${SMN_TOKEN:-}" ] && missing+=("SMN_TOKEN")
[ -z "${KPLER_TOKEN:-}" ] && missing+=("KPLER_TOKEN")
[ ${#missing[@]} -ne 0 ] && {
    log "ERROR: Missing env vars: ${missing[*]}. See .env.example."
    exit 1
}

# Optional: Run pulls (pass --pull flag)
if [ "${1:-}" = "--pull" ]; then
    log "Running station pull"
    ./pull_stations.sh
    log "Running radar pull"
    ./pull_radar.sh
    log "Running AIS pull"
    ./pull_ais.sh
    log "Archiving previous day"
    ./archive.sh
fi

log "Bootstrap complete. Data in /data/{stations,radar,ais}/current/"
