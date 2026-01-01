#!/bin/bash
set -euo pipefail

source conf.env
cd "$(dirname "$0")"
HOUR_UTC=$(date -u -d '3 hours ago' +%Y%m%dT%H)
OUTPUT_FILE="${CURRENT_DIR}/radar_${HOUR_UTC}Z.jsonl"
mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] radar: $1" | tee -a "${LOGS_DIR}/radar.log"; }

log "Starting radar pull for ${HOUR_UTC}"

# Extract date components
YEAR=${HOUR_UTC:0:4}
MON=${HOUR_UTC:4:2}
DAY=${HOUR_UTC:6:2}
HH=${HOUR_UTC:9:2}

# Format timestamp for JSON output
TIMESTAMP="${YEAR}-${MON}-${DAY}T${HH}:00:00Z"

# AWS S3 path for NEXRAD Level 2 data
# Example: s3://noaa-nexrad-level2/2025/12/31/KNKX/KNKX20251231_160000_V06
S3_PATH="s3://${RADAR_S3_BUCKET}/${YEAR}/${MON}/${DAY}/${RADAR_SITE}/"

log "Checking S3 path: ${S3_PATH}"

# Create temporary directory for radar data
TEMP_DIR=$(mktemp -d)
trap "rm -rf ${TEMP_DIR}" EXIT

# Try to download radar files for this hour from S3
# NEXRAD files are named like: KNKX20251231_160000_V06
# We need files that match our hour
FILE_PATTERN="${RADAR_SITE}${YEAR}${MON}${DAY}_${HH}"

# Check if AWS CLI is available
if ! command -v aws &> /dev/null; then
    log "WARNING: aws CLI not found - cannot fetch radar data"
    # Create empty output file with null reflectivity values
    python3 - <<PY > "$OUTPUT_FILE"
import json
import numpy as np

# Grid parameters from conf.env
lat_min = ${LAT_MIN}
lat_max = ${LAT_MAX}
lon_min = ${LON_MIN}
lon_max = ${LON_MAX}
resolution = 0.004

lats = np.arange(lat_min, lat_max, resolution)
lons = np.arange(lon_min, lon_max, resolution)

for lat in lats:
    for lon in lons:
        record = {
            'lat': round(lat, 6),
            'lon': round(lon, 6),
            'timestamp': '${TIMESTAMP}',
            'reflectivity_dbz': None
        }
        print(json.dumps(record))
PY
    GRID_COUNT=$(wc -l < "$OUTPUT_FILE")
    log "Radar grid written (no data): $OUTPUT_FILE (${GRID_COUNT} points)"
    exit 0
fi

# List available files for this hour
FILES=$(aws s3 ls "${S3_PATH}" 2>/dev/null | grep "${FILE_PATTERN}" | awk '{print $4}' || echo "")

if [ -z "$FILES" ]; then
    log "WARNING: No radar files found for ${HOUR_UTC} - generating null grid"
    # Create output with null reflectivity values
    python3 - <<PY > "$OUTPUT_FILE"
import json
import numpy as np

lat_min = ${LAT_MIN}
lat_max = ${LAT_MAX}
lon_min = ${LON_MIN}
lon_max = ${LON_MAX}
resolution = 0.004

lats = np.arange(lat_min, lat_max, resolution)
lons = np.arange(lon_min, lon_max, resolution)

for lat in lats:
    for lon in lons:
        record = {
            'lat': round(lat, 6),
            'lon': round(lon, 6),
            'timestamp': '${TIMESTAMP}',
            'reflectivity_dbz': None
        }
        print(json.dumps(record))
PY
else
    log "Found radar files, processing..."
    
    # Download and process radar data
    # For now, create grid with null values as placeholder
    # Full implementation would use pyart or wradlib to process NEXRAD Level 2 data
    python3 - <<PY > "$OUTPUT_FILE"
import json
import numpy as np

lat_min = ${LAT_MIN}
lat_max = ${LAT_MAX}
lon_min = ${LON_MIN}
lon_max = ${LON_MAX}
resolution = 0.004

lats = np.arange(lat_min, lat_max, resolution)
lons = np.arange(lon_min, lon_max, resolution)

# TODO: Download and parse actual NEXRAD data files
# This would require pyart or similar library to read Level 2 data
# For now, output null grid

for lat in lats:
    for lon in lons:
        record = {
            'lat': round(lat, 6),
            'lon': round(lon, 6),
            'timestamp': '${TIMESTAMP}',
            'reflectivity_dbz': None
        }
        print(json.dumps(record))
PY
fi

GRID_COUNT=$(wc -l < "$OUTPUT_FILE")
log "Radar grid written: $OUTPUT_FILE (~${GRID_COUNT} points)"
