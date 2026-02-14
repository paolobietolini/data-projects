# ATAC Real-Time Transit Data Pipeline

Ingest, store, and model Rome's public transport real-time data from ATAC/Roma Mobilità GTFS-RT feeds.

## Architecture

```
GTFS-RT feeds (protobuf, every 60s)
        │
        ▼
  Python ingestion script (cron / systemd timer)
        │
        ▼
  Raw Parquet files (partitioned by date)
        │
        ▼
  DuckDB (warehouse)
        │
        ▼
  dbt models (staging → intermediate → marts)
```

## Data Sources

All feeds are free, no API key needed.

| Feed | URL | Format | Update |
|------|-----|--------|--------|
| Static GTFS | https://romamobilita.it/sites/default/files/rome_static_gtfs.zip | ZIP/CSV | ~daily |
| Vehicle Positions | https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb | Protobuf | ~60s |
| Trip Updates | https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb | Protobuf | ~60s |
| Service Alerts | https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb | Protobuf | ~60s |

## Step 0: Project Setup

```bash
mkdir -p atac-realtime/{raw/vehicle_positions,raw/trip_updates,raw/alerts,static,scripts,dbt}
cd atac-realtime

python -m venv .venv
source .venv/bin/activate

pip install gtfs-realtime-bindings requests pandas pyarrow duckdb
```

## Step 1: Download the Static GTFS

The static feed is your dimension data — stops, routes, trips, calendars. You need this to make sense of the real-time data.

```bash
cd static
curl -O https://romamobilita.it/sites/default/files/rome_static_gtfs.zip
unzip rome_static_gtfs.zip -d gtfs
```

You'll get these key files:
- `stops.txt` — ~8,000 stops with lat/lon
- `routes.txt` — bus, tram, metro lines
- `trips.txt` — individual trip instances
- `stop_times.txt` — scheduled arrival/departure at each stop (this is the big one)
- `calendar.txt` + `calendar_dates.txt` — which services run which days
- `shapes.txt` — route geometries

Re-download this weekly (it changes with schedule updates). You can check the MD5:
```bash
curl -s https://romamobilita.it/sites/default/files/rome_static_gtfs.zip.md5
```

## Step 2: Write the Ingestion Script

Create `scripts/ingest.py`. This is the core loop:

1. Fetch each GTFS-RT protobuf feed
2. Parse the protobuf into Python objects
3. Flatten into rows
4. Append to a Parquet file (one per day per feed)

### Parsing GTFS-RT Protobuf

```python
from google.transit import gtfs_realtime_pb2
import requests

def fetch_feed(url):
    """Fetch and parse a GTFS-RT protobuf feed."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed
```

### Vehicle Positions Schema

Each entity in the vehicle positions feed gives you:

```python
def parse_vehicle_positions(feed):
    """Flatten vehicle position entities into dicts."""
    rows = []
    timestamp = feed.header.timestamp  # feed-level timestamp
    for entity in feed.entity:
        vp = entity.vehicle
        rows.append({
            "feed_timestamp": timestamp,
            "entity_id": entity.id,
            "trip_id": vp.trip.trip_id if vp.HasField("trip") else None,
            "route_id": vp.trip.route_id if vp.HasField("trip") else None,
            "direction_id": vp.trip.direction_id if vp.HasField("trip") else None,
            "start_date": vp.trip.start_date if vp.HasField("trip") else None,
            "vehicle_id": vp.vehicle.id if vp.HasField("vehicle") else None,
            "vehicle_label": vp.vehicle.label if vp.HasField("vehicle") else None,
            "latitude": vp.position.latitude if vp.HasField("position") else None,
            "longitude": vp.position.longitude if vp.HasField("position") else None,
            "bearing": vp.position.bearing if vp.HasField("position") else None,
            "speed": vp.position.speed if vp.HasField("position") else None,
            "current_stop_sequence": vp.current_stop_sequence,
            "stop_id": vp.stop_id,
            "current_status": vp.current_status,  # 0=INCOMING, 1=STOPPED, 2=IN_TRANSIT
            "vehicle_timestamp": vp.timestamp,
        })
    return rows
```

### Trip Updates Schema

Trip updates tell you about delays — the actual vs. scheduled arrival at each stop:

```python
def parse_trip_updates(feed):
    """Flatten trip update entities into dicts (one row per stop_time_update)."""
    rows = []
    timestamp = feed.header.timestamp
    for entity in feed.entity:
        tu = entity.trip_update
        trip_id = tu.trip.trip_id if tu.HasField("trip") else None
        route_id = tu.trip.route_id if tu.HasField("trip") else None
        start_date = tu.trip.start_date if tu.HasField("trip") else None
        vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None

        for stu in tu.stop_time_update:
            rows.append({
                "feed_timestamp": timestamp,
                "entity_id": entity.id,
                "trip_id": trip_id,
                "route_id": route_id,
                "start_date": start_date,
                "vehicle_id": vehicle_id,
                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id,
                "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
                "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                "departure_time": stu.departure.time if stu.HasField("departure") else None,
                "schedule_relationship": stu.schedule_relationship,
            })
    return rows
```

### Writing to Parquet

Append to daily partitioned Parquet files:

```python
import pandas as pd
from datetime import datetime, timezone

def append_to_parquet(rows, feed_type):
    """Append rows to a daily Parquet file."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    path = f"raw/{feed_type}/{today}.parquet"

    # If file exists, concatenate
    try:
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)
    except FileNotFoundError:
        pass

    df.to_parquet(path, index=False)
```

> **Note**: This append-by-reading-and-rewriting approach is simple but gets slow as files grow. Once a day's file gets large, switch to writing one Parquet file per fetch (timestamped) and let DuckDB glob them: `SELECT * FROM read_parquet('raw/vehicle_positions/2026-02-14/*.parquet')`. Your call on when to optimize this.

### Main Loop

```python
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

FEEDS = {
    "vehicle_positions": "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb",
    "trip_updates": "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb",
    "alerts": "https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb",
}

PARSERS = {
    "vehicle_positions": parse_vehicle_positions,
    "trip_updates": parse_trip_updates,
    "alerts": lambda feed: [],  # implement later if you want
}

def run_once():
    for feed_type, url in FEEDS.items():
        try:
            feed = fetch_feed(url)
            rows = PARSERS[feed_type](feed)
            append_to_parquet(rows, feed_type)
            logging.info(f"{feed_type}: {len(rows)} rows")
        except Exception as e:
            logging.error(f"{feed_type}: {e}")

if __name__ == "__main__":
    while True:
        run_once()
        time.sleep(60)
```

## Step 3: Run It

### Option A: Simple (tmux/screen)

```bash
source .venv/bin/activate
python scripts/ingest.py
```

Detach with `Ctrl-B D` (tmux) or `Ctrl-A D` (screen).

### Option B: systemd (recommended for persistence)

Create `~/.config/systemd/user/atac-ingest.service`:

```ini
[Unit]
Description=ATAC GTFS-RT Ingestion

[Service]
Type=simple
WorkingDirectory=/path/to/atac-realtime
ExecStart=/path/to/atac-realtime/.venv/bin/python scripts/ingest.py
Restart=always
RestartSec=10

[Install]
WantedBy=default.target
```

```bash
systemctl --user daemon-reload
systemctl --user enable --now atac-ingest
systemctl --user status atac-ingest
journalctl --user -u atac-ingest -f
```

### Option C: cron (one-shot per minute)

Rewrite `ingest.py` to call `run_once()` without the while loop, then:

```bash
* * * * * cd /path/to/atac-realtime && .venv/bin/python scripts/ingest.py >> logs/ingest.log 2>&1
```

## Step 4: Load into DuckDB

Once you have a few days of data:

```sql
-- Create or attach database
-- duckdb atac.duckdb

-- Load static GTFS as tables
CREATE TABLE stops AS SELECT * FROM read_csv('static/gtfs/stops.txt', auto_detect=true);
CREATE TABLE routes AS SELECT * FROM read_csv('static/gtfs/routes.txt', auto_detect=true);
CREATE TABLE trips AS SELECT * FROM read_csv('static/gtfs/trips.txt', auto_detect=true);
CREATE TABLE stop_times AS SELECT * FROM read_csv('static/gtfs/stop_times.txt', auto_detect=true);
CREATE TABLE calendar AS SELECT * FROM read_csv('static/gtfs/calendar.txt', auto_detect=true);

-- Load real-time data (glob all daily files)
CREATE TABLE raw_vehicle_positions AS
  SELECT * FROM read_parquet('raw/vehicle_positions/*.parquet');

CREATE TABLE raw_trip_updates AS
  SELECT * FROM read_parquet('raw/trip_updates/*.parquet');
```

Quick sanity checks:

```sql
-- How many position pings per day?
SELECT date_trunc('day', to_timestamp(feed_timestamp)) AS day,
       count(*) AS pings
FROM raw_vehicle_positions
GROUP BY 1 ORDER BY 1;

-- Average delay by route (in seconds)
SELECT route_id,
       avg(arrival_delay) AS avg_delay_s,
       count(*) AS observations
FROM raw_trip_updates
WHERE arrival_delay IS NOT NULL
GROUP BY 1
ORDER BY avg_delay_s DESC
LIMIT 20;

-- Which stops have the worst delays?
SELECT tu.stop_id,
       s.stop_name,
       avg(tu.arrival_delay) AS avg_delay_s,
       count(*) AS n
FROM raw_trip_updates tu
JOIN stops s ON tu.stop_id = s.stop_id
WHERE tu.arrival_delay IS NOT NULL
GROUP BY 1, 2
HAVING n > 100
ORDER BY avg_delay_s DESC
LIMIT 20;
```

## Step 5: dbt Models (when you're ready)

Suggested layer structure:

```
dbt/
  models/
    staging/
      stg_vehicle_positions.sql    -- deduplicate, cast types, convert timestamps
      stg_trip_updates.sql         -- deduplicate, cast, compute delay_minutes
      stg_stops.sql                -- from static GTFS
      stg_routes.sql               -- from static GTFS
    intermediate/
      int_trip_delays.sql          -- join trip updates with stops and routes
      int_vehicle_speeds.sql       -- compute speed between consecutive pings
    marts/
      fct_delays_by_route.sql      -- avg/p50/p95 delay per route per day
      fct_delays_by_hour.sql       -- delay patterns by hour of day
      fct_delays_by_stop.sql       -- worst stops
      fct_service_reliability.sql  -- % of trips on time (< 5min delay)
      dim_routes.sql               -- route metadata
      dim_stops.sql                -- stop metadata with geometry
```

Use `dbt-duckdb` adapter:

```bash
pip install dbt-duckdb
dbt init atac_analytics
```

## Step 6: Refresh Static GTFS

The static feed changes frequently (route/schedule updates). Set up a weekly refresh:

```bash
# scripts/refresh_static.sh
#!/bin/bash
cd "$(dirname "$0")/../static"
NEW_MD5=$(curl -s https://romamobilita.it/sites/default/files/rome_static_gtfs.zip.md5)
OLD_MD5=$(md5sum rome_static_gtfs.zip 2>/dev/null | cut -d' ' -f1)

if [ "$NEW_MD5" != "$OLD_MD5" ]; then
    TODAY=$(date +%Y-%m-%d)
    echo "$(date): Static GTFS changed, downloading..."

    # Archive the current version before overwriting
    if [ -d gtfs ]; then
        mv gtfs "gtfs_${TODAY}"
        cp rome_static_gtfs.zip "rome_static_gtfs_${TODAY}.zip"
    fi

    curl -O https://romamobilita.it/sites/default/files/rome_static_gtfs.zip
    unzip -o rome_static_gtfs.zip -d gtfs
    echo "$(date): Done. Previous version archived as gtfs_${TODAY}/"
else
    echo "$(date): Static GTFS unchanged."
fi
```

This keeps a dated copy of each version so you can track schedule changes over time. The `gtfs/` directory always points to the latest.

```bash
chmod +x scripts/refresh_static.sh
# Add to crontab: run daily at 4am
0 4 * * * /path/to/atac-realtime/scripts/refresh_static.sh >> logs/static_refresh.log 2>&1
```

## Expected Data Volume

Back-of-envelope:
- ~1,500 active vehicles during peak hours
- 1 fetch/min = 1,440 fetches/day
- ~1,500 rows × 1,440 fetches = **~2.1M vehicle position rows/day**
- Trip updates are larger (one row per stop per trip) — expect **~5-10M rows/day**
- Parquet compression keeps this manageable: ~50-100MB/day raw

After a month you'll have ~200M+ rows. DuckDB handles this easily on a laptop.

## Ideas for Analysis

Once you have 1-2 weeks of data:

- **Delay heatmap**: which routes/stops/hours are worst?
- **Weekend vs weekday**: service reliability comparison
- **Speed analysis**: compute actual bus speeds between stops vs. scheduled
- **Ghost buses**: trips that appear in schedule but never show up in real-time feed
- **Bunching**: detect when 2+ buses on the same route arrive at a stop within 2 minutes
- **Rain effect**: cross with weather data (OpenMeteo API, free) to see impact on delays
- **Metro vs bus**: compare reliability across transit modes

## License

Data: CC-BY-SA (Roma Mobilità / ATAC).
