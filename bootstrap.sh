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

# Auto-detect and install git if missing
if ! command -v git >/dev/null; then
    log "git not found – attempting automatic installation"
    
    if command -v apt-get >/dev/null; then
        log "Detected Debian/Ubuntu – installing git via apt"
        sudo apt-get update && sudo apt-get install -y git
    elif command -v yum >/dev/null; then
        log "Detected CentOS/RHEL – installing git via yum"
        sudo yum install -y git
    elif command -v dnf >/dev/null; then
        log "Detected Fedora – installing git via dnf"
        sudo dnf install -y git
    elif command -v apk >/dev/null; then
        log "Detected Alpine – installing git via apk"
        sudo apk add git
    elif command -v pacman >/dev/null; then
        log "Detected Arch – installing git via pacman"
        sudo pacman -Sy git --noconfirm
    else
        log "ERROR: Unsupported package manager. Install git manually (e.g., sudo apt install git) and retry."
        exit 1
    fi
    
    log "git installed successfully"
fi

# Proceed with clone/pull
if [ ! -d "$REPO_DIR/.git" ]; then
    log "Cloning fresh repo from $REPO_URL"
    git clone "$REPO_URL" "$REPO_DIR"
    cd "$REPO_DIR"
else
    log "Pulling latest changes"
    cd "$REPO_DIR"
    git pull origin main
fi

# Config handling
if [ ! -f conf.env ]; then
    if [ -f conf.env.example ]; then
        cp conf.env.example conf.env
        log "Created conf.env from template. Edit with MarineTraffic API key."
    else
        log "WARN: conf.env.example missing – create conf.env manually with required variables."
    fi
fi

# Source config and create data dirs (if conf.env exists)
if [ -f conf.env ]; then
    set -a
    source conf.env
    set +a
    
    missing=()
    [ -z "${MARINETRAFFIC_APIKEY:-}" ] && missing+=("MARINETRAFFIC_APIKEY")
    [ -z "${DATA_DIR:-}" ] && missing+=("DATA_DIR")
    if [ ${#missing[@]} -ne 0 ]; then
        log "WARN: Missing vars in conf.env: ${missing[*]} (pulls may fail)"
    fi
    
    mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOGS_DIR"
    log "Data directories ensured: ${CURRENT_DIR}, ${ARCHIVE_DIR}"
fi

log "Bootstrap complete – system ready for hourly pulls"
