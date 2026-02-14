# ATAC Real-Time Transit Data Pipeline

A lightweight pipeline that continuously collects Rome's public transport real-time data from ATAC / Roma Mobilità GTFS-RT feeds and stores it as daily Parquet files. Designed to run unattended on a Raspberry Pi (or any always-on Linux box) with minimal resource usage.

## Why

Rome's transit agency (ATAC) publishes real-time vehicle positions, trip delay updates, and service alerts as GTFS-RT protobuf feeds — refreshed roughly every 60 seconds. This project captures that ephemeral stream into a durable, query-friendly format so you can later analyse delay patterns, service reliability, vehicle speeds, and more.

## Architecture

```
GTFS-RT protobuf feeds (3 feeds, ~60s refresh)
        │
        ▼
  cron (every minute) → ingest_once.py
        │
        ▼
  Daily Parquet files  (raw/{feed_type}/YYYY-MM-DD.parquet)
        │
        ▼
  DuckDB warehouse     (atac.duckdb — static GTFS + raw feeds)
```

## Data Sources

All feeds are **free and public** — no API key required.

| Feed | Format | Refresh | Description |
|------|--------|---------|-------------|
| [Static GTFS](https://romamobilita.it/sites/default/files/rome_static_gtfs.zip) | ZIP/CSV | ~daily | Stops, routes, trips, schedules, shapes |
| [Vehicle Positions](https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb) | Protobuf | ~60s | GPS coordinates of active vehicles |
| [Trip Updates](https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb) | Protobuf | ~60s | Predicted arrival/departure delays per stop |
| [Service Alerts](https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb) | Protobuf | ~60s | Disruptions, detours, cancellations |

## Project Structure

```
atac-realtime/
├── scripts/
│   ├── ingest.py           # Core ingestion: fetch → parse → Parquet
│   ├── ingest_once.py      # Single-shot wrapper (for cron)
│   ├── load_duckdb.py      # Load static GTFS + raw Parquet into DuckDB
│   ├── refresh_static.sh   # Download new GTFS schedule if MD5 changed
│   ├── cron_install.sh     # Install cron jobs
│   └── cron_remove.sh      # Remove cron jobs
├── raw/                    # Daily Parquet files (gitignored)
│   ├── vehicle_positions/
│   ├── trip_updates/
│   └── alerts/
├── static/                 # GTFS schedule files (gitignored)
├── logs/                   # Cron log output (gitignored)
├── pyproject.toml
└── README.md
```

## Setup

Requires **Python 3.13+** and [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/paolobietolini/adab.git
cd adab/atac-realtime

# Create venv and install dependencies
uv venv
uv pip install -r pyproject.toml

# Download the static GTFS schedule
mkdir -p static raw/{vehicle_positions,trip_updates,alerts} logs
bash scripts/refresh_static.sh
```

## Usage

### Start collecting data

Install the cron jobs (runs ingestion every minute, refreshes static GTFS daily at 4am):

```bash
bash scripts/cron_install.sh
```

Verify it's running:

```bash
crontab -l | grep atac-realtime
tail -f logs/ingest.log
```

### Stop collecting

```bash
bash scripts/cron_remove.sh
```

### Load into DuckDB

After collecting some data, load everything into a single DuckDB database for analysis:

```bash
# Full load (static GTFS + real-time Parquet)
python scripts/load_duckdb.py

# Only reload real-time tables (faster, skip static)
python scripts/load_duckdb.py --rt-only
```

Then query it:

```bash
duckdb atac.duckdb
```

```sql
-- Position pings per day
SELECT date_trunc('day', to_timestamp(feed_timestamp)) AS day,
       count(*) AS pings
FROM raw_vehicle_positions
GROUP BY 1 ORDER BY 1;

-- Average delay by route
SELECT r.route_short_name, avg(tu.arrival_delay) / 60.0 AS avg_delay_min,
       count(*) AS observations
FROM raw_trip_updates tu
JOIN routes r ON tu.route_id = r.route_id
WHERE tu.arrival_delay IS NOT NULL
GROUP BY 1 HAVING observations > 1000
ORDER BY avg_delay_min DESC
LIMIT 20;

-- Worst stops for delays
SELECT s.stop_name, avg(tu.arrival_delay) / 60.0 AS avg_delay_min,
       count(*) AS n
FROM raw_trip_updates tu
JOIN stops s ON tu.stop_id = s.stop_id
WHERE tu.arrival_delay IS NOT NULL
GROUP BY 1 HAVING n > 500
ORDER BY avg_delay_min DESC
LIMIT 20;
```

## Parquet Schemas

### vehicle_positions

| Column | Type | Description |
|--------|------|-------------|
| `feed_timestamp` | int | Feed-level UNIX timestamp |
| `entity_id` | string | Unique entity ID from feed |
| `trip_id` | string | GTFS trip ID |
| `route_id` | string | GTFS route ID |
| `direction_id` | int | 0 or 1 |
| `start_date` | string | Service date (YYYYMMDD) |
| `vehicle_id` | string | Vehicle identifier |
| `vehicle_label` | string | Human-readable vehicle label |
| `latitude` | float | GPS latitude |
| `longitude` | float | GPS longitude |
| `bearing` | float | Heading in degrees |
| `speed` | float | Speed in m/s |
| `stop_id` | string | Current or next stop |
| `current_status` | int | 0=INCOMING, 1=STOPPED, 2=IN_TRANSIT |
| `vehicle_timestamp` | int | Vehicle-level UNIX timestamp |

### trip_updates

| Column | Type | Description |
|--------|------|-------------|
| `feed_timestamp` | int | Feed-level UNIX timestamp |
| `trip_id` | string | GTFS trip ID |
| `route_id` | string | GTFS route ID |
| `vehicle_id` | string | Vehicle identifier |
| `stop_sequence` | int | Stop order within the trip |
| `stop_id` | string | GTFS stop ID |
| `arrival_delay` | int | Predicted arrival delay (seconds, positive = late) |
| `departure_delay` | int | Predicted departure delay (seconds) |
| `schedule_relationship` | int | 0=SCHEDULED, 1=SKIPPED, 2=NO_DATA |

### alerts

| Column | Type | Description |
|--------|------|-------------|
| `feed_timestamp` | int | Feed-level UNIX timestamp |
| `cause` | int | Alert cause enum |
| `effect` | int | Alert effect enum |
| `header_text` | string | Short alert title |
| `description_text` | string | Full alert description |
| `route_id` | string | Affected route (if any) |
| `stop_id` | string | Affected stop (if any) |

## Expected Data Volume

- ~300–1,500 active vehicles depending on time of day
- 1 fetch/minute = 1,440 fetches/day
- **~2M vehicle position rows/day** at peak
- **~5–10M trip update rows/day** (one row per stop per trip)
- Parquet compression keeps this at roughly **50–100 MB/day**

DuckDB handles months of this on a Raspberry Pi without breaking a sweat.

## Analysis Ideas

- **Delay heatmap** — which routes, stops, and hours are worst?
- **Ghost buses** — trips in the schedule that never appear in the real-time feed
- **Bus bunching** — detect when 2+ buses on the same route arrive at a stop within 2 minutes
- **Speed profiles** — actual vehicle speeds between stops vs. scheduled
- **Weather impact** — cross with [Open-Meteo](https://open-meteo.com/) to measure rain/heat effects on delays
- **Weekend vs weekday** — service reliability comparison
- **Metro vs bus** — reliability across transit modes

## License

Data is published by [Roma Mobilità](https://romamobilita.it/) / ATAC under CC-BY-SA.
