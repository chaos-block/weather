#!/bin/bash
set -euo pipefail

# =============================================================================
# archive.sh – Daily bundling of observed data products
# Bundles previous day's stations + radar + ais files into YYYY-MM-DD.tar.zst (zstd level 19)
# Cleans /current/ files older than 72 hours
# Run daily at 03:00 UTC (per Section 6 schedule)
# =============================================================================

source conf.env || { echo "Error: conf.env not found. Copy conf.env.example to conf.env."; exit 1; }
cd "$(dirname "$0")"

# Previous day (UTC) for bundling
YESTERDAY_UTC=$(date -u -d 'yesterday' +"%Y-%m-%d")
BUNDLE_FILE="${ARCHIVE_DIR}/${YESTERDAY_UTC}.tar.zst"

mkdir -p "$ARCHIVE_DIR" "$LOGS_DIR"
LOG_FILE="${LOGS_DIR}/archive.log"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] archive: $1" | tee -a "$LOG_FILE"; }

log "Starting daily archive for ${YESTERDAY_UTC}"

# Find and bundle all previous-day files (stations, radar, ais in /current/)
FILES_TO_BUNDLE=$(find "${CURRENT_DIR}" -type f -name "*_${YESTERDAY_UTC}*" \(
    -name "stations_*.jsonl" -o
    -name "radar_*.jsonl" -o
    -name "ais_*.jsonl"
\))

if [ -z "$FILES_TO_BUNDLE" ]; then
    log "No files found for ${YESTERDAY_UTC} – skipping bundle"
else
    log "Bundling $(echo "$FILES_TO_BUNDLE" | wc -l) files → $BUNDLE_FILE"
    tar -C "${DATA_DIR}/current" -cf - $(basename -a $FILES_TO_BUNDLE) | \
    zstd -19 -o "$BUNDLE_FILE"

    log "Bundle complete: $BUNDLE_FILE ($(du -h "$BUNDLE_FILE" | cut -f1))"
fi

# Clean /current/ older than 72 hours (retain last 72h per Section 5)
log "Cleaning files older than 72 hours in ${CURRENT_DIR}"
find "${CURRENT_DIR}" -type f -name "*.jsonl" -mtime +3 -delete

log "Archive run complete"
