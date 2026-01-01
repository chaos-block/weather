#!/bin/bash
set -euo pipefail

source conf.env
cd "$(dirname "$0")"

# Calculate time for radar pull
# Can be overridden by OVERRIDE_TIMESTAMP for historical pulls
if [ -n "${OVERRIDE_TIMESTAMP:-}" ]; then
  # Historical mode: use provided timestamp
  TIMESTAMP="$OVERRIDE_TIMESTAMP"
  LOOKBACK_DATE=$(date -u -d "$TIMESTAMP")
else
  # Real-time mode: use 3 hours ago lookback
  LOOKBACK_DATE=$(date -u -d '3 hours ago')
  TIMESTAMP=$(date -u -d "$LOOKBACK_DATE" +"%Y-%m-%dT%H:00:00Z")
fi

HOUR_UTC=$(date -u -d "$LOOKBACK_DATE" +%Y%m%dT%H)

# Output to /data/YYYY/ directory (new architecture)
YEAR=$(date -u -d "$LOOKBACK_DATE" +%Y)
OUTPUT_DIR="${OVERRIDE_OUTPUT_DIR:-${DATA_DIR}/${YEAR}}"
mkdir -p "$OUTPUT_DIR" "$LOGS_DIR"

OUTPUT_FILE="${OUTPUT_DIR}/radar_${HOUR_UTC}Z.jsonl"

log() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] radar: $1" | tee -a "${LOGS_DIR}/radar.log"; }

log "Starting radar pull for ${HOUR_UTC}"

# Extract date components
YEAR=${HOUR_UTC:0:4}
MON=${HOUR_UTC:4:2}
DAY=${HOUR_UTC:6:2}
HH=${HOUR_UTC:9:2}

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
    log "WARNING: aws CLI not found - generating null grid"
    # Create empty output file with null reflectivity values using parallel awk
    # Calculate number of latitude points
    LAT_POINTS=$(awk -v lat_min="${LAT_MIN}" -v lat_max="${LAT_MAX}" -v res="0.004" \
      'BEGIN {print int((lat_max - lat_min) / res)}')
    
    # Generate grid in parallel using xargs (4 parallel jobs)
    # Each process writes to a separate temp file, then we concatenate
    seq 0 $((LAT_POINTS - 1)) | \
    xargs -P 4 -I {} sh -c 'awk -v lat_idx={} \
      -v lat_min="'"${LAT_MIN}"'" -v lon_min="'"${LON_MIN}"'" \
      -v lon_max="'"${LON_MAX}"'" -v res="0.004" \
      -v timestamp="'"${TIMESTAMP}"'" \
      '"'"'BEGIN {
        lat = lat_min + (lat_idx * res)
        for (lon = lon_min; lon < lon_max; lon += res) {
          printf "{\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":\"%s\",\"reflectivity_dbz\":null}\n", lat, lon, timestamp
        }
      }'"'"' > "'"${OUTPUT_FILE}"'.tmp.{}"'
    
    # Concatenate all temp files in order
    for i in $(seq 0 $((LAT_POINTS - 1))); do
      cat "${OUTPUT_FILE}.tmp.$i" >> "$OUTPUT_FILE"
      rm -f "${OUTPUT_FILE}.tmp.$i"
    done
    
    GRID_COUNT=$(wc -l < "$OUTPUT_FILE")
    log "Radar grid written (no AWS CLI): $OUTPUT_FILE (${GRID_COUNT} points)"
    exit 0
fi

# List available files for this hour
FILES=$(aws s3 ls "${S3_PATH}" 2>/dev/null | grep "${FILE_PATTERN}" | awk '{print $4}' || echo "")

if [ -z "$FILES" ]; then
    log "WARNING: No radar files found for ${HOUR_UTC} - generating null grid"
    # Create output with null reflectivity values using parallel awk processing
    # Calculate number of latitude points
    LAT_POINTS=$(awk -v lat_min="${LAT_MIN}" -v lat_max="${LAT_MAX}" -v res="0.004" \
      'BEGIN {print int((lat_max - lat_min) / res)}')
    
    # Generate grid in parallel using xargs (4 parallel jobs)
    # Each process writes to a separate temp file, then we concatenate
    seq 0 $((LAT_POINTS - 1)) | \
    xargs -P 4 -I {} sh -c 'awk -v lat_idx={} \
      -v lat_min="'"${LAT_MIN}"'" -v lon_min="'"${LON_MIN}"'" \
      -v lon_max="'"${LON_MAX}"'" -v res="0.004" \
      -v timestamp="'"${TIMESTAMP}"'" \
      '"'"'BEGIN {
        lat = lat_min + (lat_idx * res)
        for (lon = lon_min; lon < lon_max; lon += res) {
          printf "{\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":\"%s\",\"reflectivity_dbz\":null}\n", lat, lon, timestamp
        }
      }'"'"' > "'"${OUTPUT_FILE}"'.tmp.{}"'
    
    # Concatenate all temp files in order
    for i in $(seq 0 $((LAT_POINTS - 1))); do
      cat "${OUTPUT_FILE}.tmp.$i" >> "$OUTPUT_FILE"
      rm -f "${OUTPUT_FILE}.tmp.$i"
    done
else
    log "Found radar files, processing..."
    
    # Download and process radar data
    # For now, create grid with null values as placeholder
    # Full implementation would use pyart or wradlib to process NEXRAD Level 2 data
    # Calculate number of latitude points
    LAT_POINTS=$(awk -v lat_min="${LAT_MIN}" -v lat_max="${LAT_MAX}" -v res="0.004" \
      'BEGIN {print int((lat_max - lat_min) / res)}')
    
    # Generate grid in parallel using xargs (4 parallel jobs)
    # Each process writes to a separate temp file, then we concatenate
    seq 0 $((LAT_POINTS - 1)) | \
    xargs -P 4 -I {} sh -c 'awk -v lat_idx={} \
      -v lat_min="'"${LAT_MIN}"'" -v lon_min="'"${LON_MIN}"'" \
      -v lon_max="'"${LON_MAX}"'" -v res="0.004" \
      -v timestamp="'"${TIMESTAMP}"'" \
      '"'"'BEGIN {
        lat = lat_min + (lat_idx * res)
        for (lon = lon_min; lon < lon_max; lon += res) {
          printf "{\"lat\":%.6f,\"lon\":%.6f,\"timestamp\":\"%s\",\"reflectivity_dbz\":null}\n", lat, lon, timestamp
        }
      }'"'"' > "'"${OUTPUT_FILE}"'.tmp.{}"'
    
    # Concatenate all temp files in order
    for i in $(seq 0 $((LAT_POINTS - 1))); do
      cat "${OUTPUT_FILE}.tmp.$i" >> "$OUTPUT_FILE"
      rm -f "${OUTPUT_FILE}.tmp.$i"
    done
fi

GRID_COUNT=$(wc -l < "$OUTPUT_FILE")
log "Radar grid written: $OUTPUT_FILE (~${GRID_COUNT} points)"
