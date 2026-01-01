#!/bin/bash
set -euo pipefail

# =============================================================================
# bundle.sh – Monthly bundling of observed data products
# Bundles all hourly files for a given month into YYYY-MM.tar.zst (zstd level 19)
# Cleans up individual files after successful bundling
# Run monthly at start of new month (typically 1st of each month for previous month)
# Usage: ./bundle.sh [YYYY-MM]  (optional month, defaults to previous month)
# =============================================================================

source conf.env || { echo "Error: conf.env not found. Copy conf.env.example to conf.env."; exit 1; }
cd "$(dirname "$0")"

mkdir -p "$LOGS_DIR"
LOG_FILE="${LOGS_DIR}/bundle.log"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] bundle: $1" | tee -a "$LOG_FILE"; }

# Determine which month to bundle
if [ -n "${1:-}" ]; then
  MONTH="$1"  # e.g., 2025-12
else
  MONTH=$(date -u -d 'first day of last month' +%Y-%m)  # Previous month
fi

YEAR=$(echo "$MONTH" | cut -d- -f1)
MONTH_MM=$(echo "$MONTH" | cut -d- -f2)

log "Starting bundle for $MONTH"

# Validate that the data directory exists
if [ ! -d "${DATA_DIR}/${YEAR}" ]; then
  log "ERROR: Data directory ${DATA_DIR}/${YEAR} does not exist"
  exit 1
fi

# Find all files for this month (pattern: *_YYYYMM*.jsonl)
FILES=$(find "${DATA_DIR}/${YEAR}" -type f \
  \( -name "stations_${YEAR}${MONTH_MM}*.jsonl" \
  -o -name "radar_${YEAR}${MONTH_MM}*.jsonl" \
  -o -name "ais_${YEAR}${MONTH_MM}*.jsonl" \) 2>/dev/null || true)

if [ -z "$FILES" ]; then
  log "No files found for $MONTH - skipping"
  exit 0
fi

FILE_COUNT=$(echo "$FILES" | wc -l)
log "Found $FILE_COUNT files to bundle"

# Bundle with zstd level 19
BUNDLE_FILE="${DATA_DIR}/${YEAR}/${MONTH}.tar.zst"

# Create tar with basenames only (files are already in the correct directory)
echo "$FILES" | xargs -n1 basename | tar -C "${DATA_DIR}/${YEAR}" -cf - -T - | zstd -19 -o "$BUNDLE_FILE"

if [ ! -f "$BUNDLE_FILE" ]; then
  log "ERROR: Bundle file creation failed"
  exit 1
fi

BUNDLE_SIZE=$(du -h "$BUNDLE_FILE" | cut -f1)
log "Bundle created: $BUNDLE_FILE ($BUNDLE_SIZE)"

# Clean up individual files after successful bundling
log "Cleaning up individual files..."
echo "$FILES" | xargs rm -f

log "Bundle complete for $MONTH"
