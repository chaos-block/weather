# weather
Collect weather

## Overview
This repository provides tools to collect historical and real-time weather data from multiple sources (NOAA, NDBC, SMN Mexico) for machine learning applications, particularly CNN training.

## Features
- **Real-time data collection**: Pull current weather observations from 17 stations
- **Historical data collection**: Pull historical data from any date range with null-field removal
- **Data completeness verification**: Audit which fields are populated vs null for each station
- **Automatic archiving**: Bundle daily data into compressed archives
- **Checkpoint/resume**: Resume interrupted historical pulls

## Quick Start

### Initial Setup
```bash
# Copy configuration template
cp conf.env.example conf.env

# Edit conf.env and add your API tokens:
# - NOAA_TOKEN (optional, for higher rate limits)
# - SMN_TOKEN (required for Mexican stations)
# - KPLER_TOKEN (required for AIS data)

# Create data directories
mkdir -p data/current data/archive logs
```

### Real-time Data Collection
```bash
# Pull current stations data (2 hours ago)
./pull_stations.sh

# Pull current radar data (3 hours ago)
./pull_radar.sh

# Pull current AIS data (3 hours ago)
./pull_ais.sh

# Or use bootstrap to run all
./bootstrap.sh all
```

### Historical Data Collection
```bash
# Pull last 3 days of historical data
./bootstrap.sh historical 3

# Pull specific date range (e.g., December 2025)
./bootstrap.sh historical_range 2025-12-01 2026-01-01

# Pull full archive from 2015 to present (long-running!)
./bootstrap.sh historical_full

# Or run pull_historical.sh directly
./pull_historical.sh 2025-12-01 2026-01-01 no-nulls
```

### Data Verification
```bash
# Verify data completeness for a date range
./bootstrap.sh verify 2025-12-01 2026-01-01
```

## Data Format

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
- `lat`: Latitude
- `lon`: Longitude
- `timestamp`: Observation time
- `reflectivity_dbz`: Radar reflectivity in dBZ (if available)

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
- Provides: Tide height, tidal currents, wind, visibility
- API: https://api.tidesandcurrents.noaa.gov/api/prod/datagetter
- Rate limit: 1-2 seconds between requests

### NDBC Buoys (4 stations)
- Provides: Wave height, wind speed/direction, visibility
- Data: https://www.ndbc.noaa.gov/data/realtime2/
- Rate limit: 2 seconds between requests

### SMN Mexico (4 stations)
- Provides: Wind, visibility, wave height
- API: https://smn.conagua.gob.mx/api/
- Rate limit: 2-3 seconds between requests
- Requires authentication token

## Architecture

### Scripts
- `pull_stations.sh`: Real-time station data collection (2-hour lookback)
- `pull_radar.sh`: Real-time radar data collection (3-hour lookback)
- `pull_ais.sh`: Real-time AIS data collection (3-hour lookback)
- `pull_historical.sh`: Historical data collection for any date range
- `archive.sh`: Bundle daily data into compressed archives
- `bootstrap.sh`: Main orchestration script

### Data Flow
1. Scripts pull data from APIs with rate limiting
2. Data is written to `/data/current/*.jsonl` (real-time) or `/data/historical/*.jsonl` (historical)
3. Null fields are removed from JSON output
4. Daily data is archived to `/data/archive/YYYY-MM-DD.tar.zst`
5. Files older than 72 hours are cleaned from `/data/current/`

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
# Install required Python packages
pip install astral

# Install jq for JSON processing
sudo apt-get install jq  # Debian/Ubuntu
brew install jq          # macOS
```

### Data Directory Permissions
```bash
# Create data directories with proper permissions
mkdir -p data/current data/archive logs
chmod 755 data data/current data/archive logs
```

## License
See repository license
