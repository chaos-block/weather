# weather
Collect weather data for CNN training

## Overview
This repository provides tools to collect historical and real-time weather data from multiple sources (NOAA, NDBC, SMN Mexico) for machine learning applications, particularly CNN training.

**NEW ARCHITECTURE (2026-01):** Simplified data storage with `/data/YYYY/` structure, monthly tar bundling, and 14-day rolling verification.

## Features
- **Real-time data collection**: Pull current weather observations from 17 stations
- **Simplified data structure**: All data stored in `/data/YYYY/` (no subdirectories)
- **Monthly bundling**: Automatic compression of monthly data into `.tar.zst` archives
- **14-day verification**: Built-in data completeness checks with field-level reporting
- **No external dependencies**: Pure bash/awk (no numpy, graceful AWS CLI handling)
- **Field-level logging**: Reports when expected fields return null values

## Quick Start

### Initial Setup
```bash
# Copy configuration template
cp conf.env.example conf.env

# Edit conf.env and add your API tokens:
# - NOAA_TOKEN (optional, for higher rate limits)
# - SMN_TOKEN (required for Mexican stations)
# - MARINETRAFFIC_APIKEY (required for AIS data)

# Create data directories
mkdir -p /data logs
```

### Real-time Data Collection (Last 72 Hours)
```bash
# Pull last 72 hours of all data products (stations, radar, AIS)
./bootstrap.sh

# Or pull individual products manually
./pull_stations.sh  # 2-hour lookback
./pull_radar.sh     # 3-hour lookback
./pull_ais.sh       # 3-hour lookback
```

### Data Verification
```bash
# Verify last 14 days (default)
./bootstrap.sh verify

# Verify last 7 days
./bootstrap.sh verify 7

# Verify specific date range
./bootstrap.sh verify 2025-12-01 2025-12-31
```

### Monthly Bundling
```bash
# Bundle previous month (run on 1st of each month)
./bootstrap.sh bundle

# Bundle specific month
./bootstrap.sh bundle 2025-12
```

## Data Format

All data files follow the naming convention: `{product}_YYYYMMDDThhZ.jsonl`

### File Structure

**Before bundling (raw hourly files):**
```
/data/
  2025/
    stations_20251201T00Z.jsonl
    radar_20251201T00Z.jsonl
    ais_20251201T00Z.jsonl
    stations_20251201T01Z.jsonl
    ...
    (24 files per day × 3 products = 72 files per day)
```

**After monthly bundling (1st of next month):**
```
/data/
  2025/
    2025-12.tar.zst           # Bundled December data (zstd level 19)
    stations_20260101T00Z.jsonl  # January files (not yet bundled)
    radar_20260101T00Z.jsonl
    ais_20260101T00Z.jsonl
    ...
```

### Stations Data
File: `stations_YYYYMMDDThhZ.jsonl`

Each line contains:
- `station_id`: Station identifier (e.g., "9410135")
- `timestamp`: Observation time in ISO 8601 format
- `tide_height_ft`: Tide height in feet (if available)
- `tide_speed_kts`: Tidal current speed in knots (if available)
- `tide_dir_deg`: Tidal current direction in degrees (if available)
- `visibility_mi`: Visibility in statute miles (if available)
- `cloud_pct`: Cloud coverage percentage (if available)
- `wave_ht_ft`: Wave height in feet (if available)
- `wind_spd_kts`: Wind speed in knots (if available)
- `wind_dir_deg`: Wind direction in degrees (if available)
- `moon_phase_pct`: Moon illumination percentage
- `sunrise_time`: Sunrise time in HH:MM format
- `sunset_time`: Sunset time in HH:MM format

**Note**: Null fields are automatically removed from output. Each station only includes fields it can provide.

Example:
```json
{"station_id":"9410135","timestamp":"2025-12-31T20:00:00Z","wave_ht_ft":5.2,"wind_spd_kts":12.3,"wind_dir_deg":245,"moon_phase_pct":94.4,"sunrise_time":"06:45","sunset_time":"16:55"}
```

### Radar Data
File: `radar_YYYYMMDDThhZ.jsonl`

Each line contains a grid point with:
- `lat`: Latitude (0.004° resolution)
- `lon`: Longitude (0.004° resolution)
- `timestamp`: Observation time
- `reflectivity_dbz`: Radar reflectivity in dBZ (null if unavailable)

Grid: ~62,500 points covering 32.500°N-33.496°N, 118.000°W-117.000°W

### AIS Data
File: `ais_YYYYMMDDThhZ.jsonl`

Each line contains:
- `mmsi`: Maritime Mobile Service Identity
- `lat`: Latitude
- `lon`: Longitude
- `timestamp`: Observation time
- `speed_kts`: Speed in knots
- `course_deg`: Course over ground in degrees
- `heading_deg`: Vessel heading in degrees

## Data Sources

### NOAA Tides and Currents (9 stations)
- **Provides**: Tide height, tidal currents, wind, visibility
- **API**: https://api.tidesandcurrents.noaa.gov/api/prod/datagetter
- **Rate limit**: 1-2 seconds between requests
- **Stations**: 9410135, 9410155, 9410166, 9410170, 9410179, 9410196, 9410230, 9410396, 9410580

### NDBC Buoys (4 stations)
- **Provides**: Wave height, wind speed/direction, visibility
- **Data**: https://www.ndbc.noaa.gov/data/realtime2/
- **Rate limit**: 2 seconds between requests
- **Stations**: 46047, 46222, 46232, 46258

### SMN Mexico (4 stations)
- **Provides**: Wind, visibility, wave height
- **API**: https://smn.conagua.gob.mx/api/
- **Rate limit**: 2-3 seconds between requests
- **Requires**: Authentication token from CONAGUA
- **Stations**: SMN1401, SMN1403, SMN1405, SMN-PB1

### NEXRAD Radar
- **Provides**: Reflectivity grid (0.004° resolution)
- **Source**: NOAA NEXRAD Level 2 (KNKX San Diego)
- **S3 Bucket**: s3://noaa-nexrad-level2/
- **Note**: Requires AWS CLI for data access (generates null grid if unavailable)

### MarineTraffic AIS
- **Provides**: Vessel positions, speed, course, heading
- **API**: https://www.marinetraffic.com/en/ais-api-services
- **Requires**: API key subscription
- **Coverage**: San Diego SAR region (32.5°N-33.5°N, 118°W-117°W)

## Architecture

### Scripts
- **`pull_stations.sh`**: Real-time station data collection (2-hour lookback)
- **`pull_radar.sh`**: Real-time radar data collection (3-hour lookback, pure bash/awk)
- **`pull_ais.sh`**: Real-time AIS data collection (3-hour lookback)
- **`bundle.sh`**: Monthly data bundling into `.tar.zst` archives
- **`verify.sh`**: Data completeness verification with field-level checks
- **`bootstrap.sh`**: Main orchestration script

### Data Flow
1. Scripts pull data from APIs with rate limiting
2. Data is written to `/data/YYYY/*.jsonl` (year-based directories)
3. Null fields are removed from JSON output
4. Monthly data is bundled to `/data/YYYY/YYYY-MM.tar.zst` (typically on 1st of next month)
5. Individual hourly files are cleaned up after successful bundling
6. Verification runs check last 14 days for missing fields and log warnings

### New vs Old Architecture

**OLD (deprecated):**
```
/data/current/      # Last 72 hours
/data/archive/      # Daily .tar.zst bundles
/data/historical/   # Historical pulls
```

**NEW (current):**
```
/data/YYYY/         # All data for year YYYY
  stations_*.jsonl  # Hourly files (unbundled)
  radar_*.jsonl     # Hourly files (unbundled)
  ais_*.jsonl       # Hourly files (unbundled)
  YYYY-MM.tar.zst   # Monthly bundles
```

## Troubleshooting

### Permission Denied
```bash
# Make scripts executable
chmod +x *.sh
```

### API Rate Limits
The scripts include automatic rate limiting:
- NOAA: 1.5 seconds between requests
- NDBC: 2 seconds between requests
- SMN: 2.5 seconds between requests

### Missing Dependencies
```bash
# Install jq for JSON processing
sudo apt-get install jq  # Debian/Ubuntu
brew install jq          # macOS

# Install Python with astral for sunrise/sunset calculations
pip install astral

# AWS CLI (optional, for radar data)
# If not installed, radar script generates null grids
sudo apt-get install awscli  # Debian/Ubuntu
brew install awscli          # macOS
```

### No numpy Required
The radar script previously required numpy, but now uses pure bash/awk for grid generation. If you see ModuleNotFoundError for numpy, the old version is still running.

### Data Directory Permissions
```bash
# Create data directories with proper permissions
mkdir -p /data logs
chmod 755 /data logs
```

### Field-Level Warnings
If you see warnings like:
```
[2026-01-01T04:27:39Z] stations: WARNING: tide_height_ft missing for 9410155 (expected but got nothing)
```

This indicates that the station is configured to provide that field (in conf.env STATIONS_LIST), but the API returned null. This is expected for:
- Stations temporarily offline
- Sensor malfunctions
- API rate limiting
- Data not yet available for the requested hour

## Migration from Old Architecture

If you have data in the old structure (`/data/current/`, `/data/archive/`, `/data/historical/`):

```bash
# 1. Move current data to year-based directories
for file in /data/current/*.jsonl; do
  year=$(echo "$file" | grep -oP '\d{4}(?=\d{4})')
  mkdir -p "/data/$year"
  mv "$file" "/data/$year/"
done

# 2. Extract archived data
for archive in /data/archive/*.tar.zst; do
  basename=$(basename "$archive" .tar.zst)
  year=${basename:0:4}
  mkdir -p "/data/$year"
  tar -xf "$archive" -C "/data/$year"
done

# 3. Move historical data
for file in /data/historical/*.jsonl; do
  year=$(echo "$file" | grep -oP '\d{4}(?=\d{4})')
  mkdir -p "/data/$year"
  mv "$file" "/data/$year/"
done

# 4. Clean up old directories
rm -rf /data/current /data/archive /data/historical
```

## License
See repository license
