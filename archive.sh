#!/bin/bash
set -euo pipefail

source .env
cd "$(dirname "$0")"
YESTERDAY=$(date -u -d '1 day ago' +%Y-%m-%d)
BUNDLE="${ARCHIVE_DIR}/${YESTERDAY}.tar.zst"
mkdir -p "$ARCHIVE_DIR" "$LOGS_DIR"

log() { echo "[$(date -u)] archive: $1" | tee -a "${LOGS_DIR}/archive.log"; }

tar -C "$DATA_DIR" -cf - stations/current/stations_${YESTERDAY}* radar/current/radar_${YESTERDAY}* ais/current/ais_${YESTERDAY}* | \
zstd -19 -o "$BUNDLE"

# Clean current >72h
find "$CURRENT_DIR" -name "*.jsonl" -mtime +3 -delete

log "Archive bundle: $BUNDLE"
