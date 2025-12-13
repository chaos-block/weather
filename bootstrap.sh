#!/bin/bash
set -euo pipefail

REPO_URL="https://github.com/chaos-block/weather.git"
TIMESTAMP=$(date -u +%Y%m%d_%H%M%S)
LOG_DIR="logs"
LOG_FILE="${LOG_DIR}/bootstrap_${TIMESTAMP}.log"

# Persistent logs dir (runtime, not versioned)
mkdir -p "$LOG_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $1" | tee -a "$LOG_FILE"; }

log "Bootstrap started (ephemeral mode – fresh fetch every run)"

# Auto-install git if missing
if ! command -v git >/dev/null; then
    log "git not found – auto-installing"
    if command -v apt-get >/dev/null; then
        sudo apt-get update && sudo apt-get install -y git
    elif command -v yum >/dev/null; then
        sudo yum install -y git
    elif command -v dnf >/dev/null; then
        sudo dnf install -y git
    elif command -v apk >/dev/null; then
        sudo apk add git
    elif command -v pacman >/dev/null; then
        sudo pacman -Sy git --noconfirm
    else
        log "ERROR: Unsupported OS – install git manually"
        exit 1
    fi
    log "git installed"
fi

# Fresh temp workspace (no persistent copies)
WORK_DIR=$(mktemp -d /tmp/weather.XXXXXX)
log "Fetching fresh repo to $WORK_DIR"

git clone "$REPO_URL" "$WORK_DIR"
cd "$WORK_DIR"

# Config: Use persistent conf.env if exists, else create from example
if [ -f /path/to/persistent/conf.env ]; then  # Adjust path if needed (e.g., /etc/weather/conf.env)
    cp /path/to/persistent/conf.env conf.env
    log "Loaded persistent conf.env"
elif [ -f conf.env.example ]; then
    cp conf.env.example conf.env
    log "Created conf.env from template – edit MARINETRAFFIC_APIKEY"
else
    log "WARN: No conf.env.example – create conf.env manually"
fi

# Source config and create data dirs
if [ -f conf.env ]; then
    set -a
    source conf.env
    set +a
    mkdir -p "${CURRENT_DIR}" "${ARCHIVE_DIR}" "$LOG_DIR"
    log "Data directories ready: ${CURRENT_DIR}, ${ARCHIVE_DIR}"
fi

log "Fresh scripts ready – bootstrap complete (workspace: $WORK_DIR)"
log "Run pulls manually from $WORK_DIR (e.g., ./pull_ais.sh) or add to cron with full path"

# Optional: Auto-clean old temp dirs (>1 day)
find /tmp -type d -name 'weather.*' -mtime +1 -exec rm -rf {} + 2>/dev/null || true
