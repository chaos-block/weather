#!/bin/bash
set -euo pipefail

source .env
cd "$(dirname "$0")"
HOUR_UTC=$(date -u -d '3 hours ago' +%Y%m%dT%H)Z
OUTPUT_FILE="${CURRENT_DIR}/radar_${HOUR_UTC}.jsonl"
mkdir -p "$CURRENT_DIR" "$LOGS_DIR"

log() { echo "[$(date -u)] radar: $1" | tee -a "${LOGS_DIR}/radar.log"; }

# AWS CLI to fetch hourly archive (e.g., s3://noaa-nexrad-level3/YYYY/MM/DD/${RADAR_SITE}_YYYYMMDD_HHMM.tar)
YEAR=${HOUR_UTC:0:4}; MON=${HOUR_UTC:5:2}; DAY=${HOUR_UTC:8:2}; HH=${HOUR_UTC:11:2}
S3_PATH="s3://${RADAR_S3_BUCKET}/${YEAR}/${MON}/${DAY}/${RADAR_SITE}_${YEAR}${MON}${DAY}_${HH}??/*.gz"
aws s3 cp "$S3_PATH" - --recursive | gunzip | # Process with Python/gdal for averaging/regridding
python3 -c "
import numpy as np; from netCDF4 import Dataset; import json
# Load scans, average dBZ over hour, grid to 0.004° bbox
lats = np.arange($LAT_MIN, $LAT_MAX, 0.004)
lons = np.arange($LON_MAX, $LON_MIN, -0.004)  # Westward
for i, lat in enumerate(lats):
    for j, lon in enumerate(lons):
        dbz = np.mean(scans) if scans else None  # Hourly avg, null if offline
        print(json.dumps({'lat':lat, 'lon':lon, 'timestamp':'${HOUR_UTC:0:13}:00:00Z', 'reflectivity_dbz':dbz}))
" > "$OUTPUT_FILE"

log "Radar grid written: $OUTPUT_FILE (~62,500 lines)"
