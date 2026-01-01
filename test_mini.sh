#!/bin/bash
set -euo pipefail

source conf.env
cd "$(dirname "$0")"

# Test with just 2 hours: 2025-12-31 00:00 and 01:00
START_DATE="2025-12-31"
END_DATE="2025-12-31"  
REMOVE_NULLS="no-nulls"

START_EPOCH=$(date -d "$START_DATE" +%s)
# Only run for 2 hours
END_EPOCH=$((START_EPOCH + 7200))

HISTORICAL_DIR="${DATA_DIR}/historical"
mkdir -p "${HISTORICAL_DIR}"

echo "Testing with 2 hours only..."

CURRENT_EPOCH=$START_EPOCH
count=0

while [ $CURRENT_EPOCH -lt $END_EPOCH ]; do
    TIMESTAMP=$(date -u -d "@$CURRENT_EPOCH" +%Y-%m-%dT%H:00:00Z)
    HOUR_UTC=$(date -u -d "@$CURRENT_EPOCH" +%Y%m%dT%H)
    
    echo "[$count] Fetching: $TIMESTAMP ($HOUR_UTC)"
    
    # Test pull_stations.sh
    OVERRIDE_TIMESTAMP="$TIMESTAMP" OVERRIDE_OUTPUT_DIR="$HISTORICAL_DIR" timeout 30 ./pull_stations.sh 2>&1 | tail -5
    
    # Check if file was created
    if [ -f "${HISTORICAL_DIR}/stations_${HOUR_UTC}Z.jsonl" ]; then
        lines=$(wc -l < "${HISTORICAL_DIR}/stations_${HOUR_UTC}Z.jsonl")
        echo "  ✓ Created stations file with $lines lines"
        # Show first record
        head -1 "${HISTORICAL_DIR}/stations_${HOUR_UTC}Z.jsonl" | jq -c .
    fi
    
    ((count++))
    CURRENT_EPOCH=$((CURRENT_EPOCH + 3600))
done

echo "Test complete!"
ls -lh "${HISTORICAL_DIR}/" || true
