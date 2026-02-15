"""
Load all ISPRA raw data into a DuckDB database.

Usage:
    python scripts/load_duckdb.py
"""

from pathlib import Path

import duckdb
import pandas as pd

PROJECT_DIR = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_DIR / "ispra.duckdb"
RAW_DIR = PROJECT_DIR / "raw"


def load_land(con: duckdb.DuckDBPyConnection) -> None:
    """Load land consumption from the XLSX (Comuni sheet only)."""
    xlsx = RAW_DIR / "land" / "consumo_di_suolo_estratto_dati_2025_anni_2006_2024.xlsx"
    if not xlsx.exists():
        print("  SKIP land (xlsx not found)")
        return

    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("DROP TABLE IF EXISTS land_consumption")
    con.execute(f"""
        CREATE TABLE land_consumption AS
        SELECT * FROM st_read(
            '{xlsx}',
            layer='Comuni_2006_2024',
            open_options=['HEADERS=FORCE', 'FIELD_TYPES=AUTO']
        )
    """)
    count = con.execute("SELECT count(*) FROM land_consumption").fetchone()[0]
    print(f"  land_consumption: {count:,} rows")


def load_waste(con: duckdb.DuckDBPyConnection) -> None:
    """Load waste CSVs into a single table with a year column."""
    waste_dir = RAW_DIR / "waste"
    csv_files = sorted(waste_dir.glob("rifiuti_comuni_*.csv"))
    if not csv_files:
        print("  SKIP waste (no CSVs found)")
        return

    con.execute("DROP TABLE IF EXISTS waste")

    frames = []
    for f in csv_files:
        year = int(f.stem.split("_")[-1])
        # Data rows have leading tabs that shift columns — strip them before parsing
        import io
        raw = f.read_text(encoding="utf-8")
        lines = raw.splitlines()
        cleaned = "\n".join(line.lstrip("\t").rstrip(";") for line in lines[1:])  # skip title row
        df = pd.read_csv(io.StringIO(cleaned), sep=";", dtype=str)
        df.columns = [c.strip() for c in df.columns]
        df = df.apply(lambda col: col.str.strip() if col.dtype == "object" else col)
        df = df.dropna(how="all")
        df.insert(0, "anno", year)
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True)
    con.execute("CREATE TABLE waste AS SELECT * FROM combined")

    count = con.execute("SELECT count(*) FROM waste").fetchone()[0]
    years = con.execute("SELECT count(DISTINCT anno) FROM waste").fetchone()[0]
    print(f"  waste: {count:,} rows ({years} years)")


def load_air(con: duckdb.DuckDBPyConnection) -> None:
    """Load air quality CSVs into one table per pollutant."""
    air_dir = RAW_DIR / "air"
    csv_files = sorted(air_dir.glob("*.csv"))
    if not csv_files:
        print("  SKIP air (no CSVs found)")
        return

    for f in csv_files:
        table_name = f"air_{f.stem.split('_')[0]}"  # air_no2, air_pm10, etc.
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"""
            CREATE TABLE {table_name} AS
            SELECT * FROM read_csv('{f}', sep=';', auto_detect=true)
        """)
        count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name}: {count:,} rows")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    tables = con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' ORDER BY table_name"
    ).fetchall()
    print(f"\nDatabase: {DB_PATH}")
    print(f"Tables: {', '.join(t[0] for t in tables)}")


def main() -> None:
    con = duckdb.connect(str(DB_PATH))

    print("Loading land consumption...")
    load_land(con)

    print("Loading waste data...")
    load_waste(con)

    print("Loading air quality...")
    load_air(con)

    print_summary(con)
    con.close()


if __name__ == "__main__":
    main()
