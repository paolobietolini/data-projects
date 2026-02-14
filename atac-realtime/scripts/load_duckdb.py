"""
Load static GTFS and raw real-time Parquet files into a DuckDB database.
Run this after you've collected some data to bootstrap the warehouse.

Usage:
    python scripts/load_duckdb.py              # full reload
    python scripts/load_duckdb.py --rt-only    # only reload real-time tables
"""

import argparse
import sys
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "atac.duckdb"
STATIC_DIR = PROJECT_DIR / "static"
RAW_DIR = PROJECT_DIR / "raw"


def load_static(con: duckdb.DuckDBPyConnection) -> None:
    """Load static GTFS text files as tables."""
    gtfs_files = {
        "stops": "stops.txt",
        "routes": "routes.txt",
        "trips": "trips.txt",
        "stop_times": "stop_times.txt",
        "calendar_dates": "calendar_dates.txt",
        "agency": "agency.txt",
    }
    # shapes.txt is huge (~22MB) — load only if needed
    # "shapes": "shapes.txt",

    for table, filename in gtfs_files.items():
        path = STATIC_DIR / filename
        if not path.exists():
            print(f"  SKIP {table} ({filename} not found)")
            continue
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(
            f"CREATE TABLE {table} AS SELECT * FROM read_csv('{path}', all_varchar=true, auto_detect=true)"
        )
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {table}: {count:,} rows")


def load_realtime(con: duckdb.DuckDBPyConnection) -> None:
    """Load raw real-time Parquet files as tables."""
    feeds = ["vehicle_positions", "trip_updates", "alerts"]

    for feed in feeds:
        feed_dir = RAW_DIR / feed
        parquet_files = sorted(feed_dir.glob("*.parquet"))
        if not parquet_files:
            print(f"  SKIP raw_{feed} (no parquet files)")
            continue

        table = f"raw_{feed}"
        glob_pattern = str(feed_dir / "*.parquet")
        con.execute(f"DROP TABLE IF EXISTS {table}")
        con.execute(
            f"CREATE TABLE {table} AS SELECT * FROM read_parquet('{glob_pattern}')"
        )
        count = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        days = len(parquet_files)
        print(f"  {table}: {count:,} rows ({days} day(s))")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print a quick summary of the database."""
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    print(f"\nDatabase: {DB_PATH}")
    print(f"Tables: {', '.join(t[0] for t in tables)}")

    # Quick stats if real-time data exists
    try:
        result = con.execute("""
            SELECT
                min(to_timestamp(feed_timestamp)) AS first_ping,
                max(to_timestamp(feed_timestamp)) AS last_ping,
                count(DISTINCT date_trunc('day', to_timestamp(feed_timestamp))) AS days,
                count(*) AS total_rows
            FROM raw_vehicle_positions
        """).fetchone()
        print(f"\nVehicle positions: {result[3]:,} rows")
        print(f"  From: {result[0]}")
        print(f"  To:   {result[1]}")
        print(f"  Days: {result[2]}")
    except Exception:
        pass

    try:
        result = con.execute("""
            SELECT count(*), count(DISTINCT route_id)
            FROM raw_trip_updates
        """).fetchone()
        print(f"\nTrip updates: {result[0]:,} rows, {result[1]} routes")
    except Exception:
        pass


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ATAC data into DuckDB")
    parser.add_argument("--rt-only", action="store_true", help="Only reload real-time tables")
    args = parser.parse_args()

    con = duckdb.connect(str(DB_PATH))

    if not args.rt_only:
        print("Loading static GTFS...")
        load_static(con)

    print("Loading real-time feeds...")
    load_realtime(con)

    print_summary(con)
    con.close()


if __name__ == "__main__":
    main()
