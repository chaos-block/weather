#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/chaos-block/weather.git"
REPO_DIR="$(pwd)/weather"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Bootstrap started"

# Check for git
if ! command -v git >/dev/null; then
    log "ERROR: git not found. Install git (e.g., apt install git) and retry."
    exit 1
fi

if [ ! -d "$REPO_DIR/.git" ]; then
    log "Cloning fresh repo from $REPO_URL"
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
else
    log "Pulling latest changes"
    cd "$REPO_DIR"
    git pull origin main
fi

# Config (add conf.env.example if missing in repo)
if [ ! -f conf.env ]; then
    if [ -f conf.env.example ]; then
        cp conf.env.example conf.env
        log "Created conf.env from template. Edit API key."
    else
        log "WARN: conf.env.example missing. Create conf.env manually."
    fi
fi

# Source and validate (if conf.env exists)
if [ -f conf.env ]; then
    set -a
    source conf.env
    set +a
    mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOGS_DIR"
fi

log "Bootstrap complete"
