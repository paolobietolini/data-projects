"""
ATAC GTFS-RT ingestion script.
Fetches vehicle positions, trip updates, and service alerts every 60 seconds.
Writes daily Parquet files partitioned by feed type.
"""

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
from google.transit import gtfs_realtime_pb2

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

RAW_DIR = Path(__file__).resolve().parent.parent / "raw"

FEEDS = {
    "vehicle_positions": "https://romamobilita.it/sites/default/files/rome_rtgtfs_vehicle_positions_feed.pb",
    "trip_updates": "https://romamobilita.it/sites/default/files/rome_rtgtfs_trip_updates_feed.pb",
    "alerts": "https://romamobilita.it/sites/default/files/rome_rtgtfs_service_alerts_feed.pb",
}


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    """Fetch and parse a GTFS-RT protobuf feed."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(response.content)
    return feed


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------

def parse_vehicle_positions(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Flatten vehicle position entities into dicts."""
    rows = []
    ts = feed.header.timestamp
    for entity in feed.entity:
        vp = entity.vehicle
        has_trip = vp.HasField("trip")
        has_pos = vp.HasField("position")
        has_veh = vp.HasField("vehicle")
        rows.append({
            "feed_timestamp": ts,
            "entity_id": entity.id,
            "trip_id": vp.trip.trip_id if has_trip else None,
            "route_id": vp.trip.route_id if has_trip else None,
            "direction_id": vp.trip.direction_id if has_trip else None,
            "start_date": vp.trip.start_date if has_trip else None,
            "vehicle_id": vp.vehicle.id if has_veh else None,
            "vehicle_label": vp.vehicle.label if has_veh else None,
            "latitude": vp.position.latitude if has_pos else None,
            "longitude": vp.position.longitude if has_pos else None,
            "bearing": vp.position.bearing if has_pos else None,
            "speed": vp.position.speed if has_pos else None,
            "current_stop_sequence": vp.current_stop_sequence,
            "stop_id": vp.stop_id or None,
            "current_status": vp.current_status,
            "vehicle_timestamp": vp.timestamp,
        })
    return rows


def parse_trip_updates(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Flatten trip update entities into dicts (one row per stop_time_update)."""
    rows = []
    ts = feed.header.timestamp
    for entity in feed.entity:
        tu = entity.trip_update
        has_trip = tu.HasField("trip")
        has_veh = tu.HasField("vehicle")
        trip_id = tu.trip.trip_id if has_trip else None
        route_id = tu.trip.route_id if has_trip else None
        start_date = tu.trip.start_date if has_trip else None
        vehicle_id = tu.vehicle.id if has_veh else None

        for stu in tu.stop_time_update:
            has_arr = stu.HasField("arrival")
            has_dep = stu.HasField("departure")
            rows.append({
                "feed_timestamp": ts,
                "entity_id": entity.id,
                "trip_id": trip_id,
                "route_id": route_id,
                "start_date": start_date,
                "vehicle_id": vehicle_id,
                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id or None,
                "arrival_delay": stu.arrival.delay if has_arr else None,
                "arrival_time": stu.arrival.time if has_arr else None,
                "departure_delay": stu.departure.delay if has_dep else None,
                "departure_time": stu.departure.time if has_dep else None,
                "schedule_relationship": stu.schedule_relationship,
            })
    return rows


def parse_alerts(feed: gtfs_realtime_pb2.FeedMessage) -> list[dict]:
    """Flatten service alert entities into dicts."""
    rows = []
    ts = feed.header.timestamp
    for entity in feed.entity:
        alert = entity.alert
        # An alert can affect multiple routes/trips — create one row per informed entity
        informed = list(alert.informed_entity) or [None]
        header_text = ""
        if alert.header_text and alert.header_text.translation:
            header_text = alert.header_text.translation[0].text
        description_text = ""
        if alert.description_text and alert.description_text.translation:
            description_text = alert.description_text.translation[0].text

        for ie in informed:
            rows.append({
                "feed_timestamp": ts,
                "entity_id": entity.id,
                "cause": alert.cause,
                "effect": alert.effect,
                "header_text": header_text,
                "description_text": description_text,
                "route_id": ie.route_id if ie and ie.route_id else None,
                "trip_id": ie.trip.trip_id if ie and ie.HasField("trip") else None,
                "stop_id": ie.stop_id if ie and ie.stop_id else None,
                "agency_id": ie.agency_id if ie and ie.agency_id else None,
            })
    return rows


PARSERS = {
    "vehicle_positions": parse_vehicle_positions,
    "trip_updates": parse_trip_updates,
    "alerts": parse_alerts,
}


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

def append_to_parquet(rows: list[dict], feed_type: str) -> None:
    """Append rows to a daily Parquet file."""
    if not rows:
        return

    df = pd.DataFrame(rows)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    out_dir = RAW_DIR / feed_type
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{today}.parquet"

    if path.exists():
        existing = pd.read_parquet(path)
        df = pd.concat([existing, df], ignore_index=True)

    df.to_parquet(path, index=False)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_once() -> None:
    """Fetch all feeds once and append to daily Parquet files."""
    for feed_type, url in FEEDS.items():
        try:
            feed = fetch_feed(url)
            rows = PARSERS[feed_type](feed)
            append_to_parquet(rows, feed_type)
            log.info("%s: %d rows", feed_type, len(rows))
        except Exception:
            log.exception("Error fetching %s", feed_type)


def main() -> None:
    log.info("Starting ATAC GTFS-RT ingestion (polling every 60s)")
    log.info("Raw data dir: %s", RAW_DIR)
    while True:
        run_once()
        time.sleep(60)


if __name__ == "__main__":
    main()
