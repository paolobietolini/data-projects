"""
Download all ISPRA raw datasets: land consumption, waste, and air quality.
Run this once to populate raw/. Re-run to refresh.
"""

from pathlib import Path

import pandas as pd
import requests

RAW_DIR = Path(__file__).resolve().parent.parent / "raw"


# ---------------------------------------------------------------------------
# Land consumption (already downloaded as XLSX)
# ---------------------------------------------------------------------------

def download_land():
    """Download the ISPRA land consumption XLSX if not present."""
    out = RAW_DIR / "land"/"consumo_di_suolo_estratto_dati_2025_anni_2006_2024.xlsx"
    if out.exists():
        print(f"  SKIP land (already exists: {out.name})")
        return
    url = "https://www.snpambiente.it/wp-content/uploads/2025/10/consumo_di_suolo_estratto_dati_2025_anni_2006_2024.xlsx"
    print("  Downloading land consumption XLSX...")
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    out.write_bytes(r.content)
    print(f"  Saved {out.name} ({out.stat().st_size / 1024:.0f} KB)")


# ---------------------------------------------------------------------------
# Waste (CSV per year from Catasto Rifiuti)
# ---------------------------------------------------------------------------

def download_waste(year_from: int = 2010, year_to: int = 2024):
    """Download municipal waste CSVs from Catasto Rifiuti."""
    waste_dir = RAW_DIR / "waste"
    waste_dir.mkdir(parents=True, exist_ok=True)
    for year in range(year_from, year_to + 1):
        out = waste_dir / f"rifiuti_comuni_{year}.csv"
        if out.exists():
            print(f"  SKIP waste {year} (already exists)")
            continue
        url = f"https://www.catasto-rifiuti.isprambiente.it/get/getDettaglioComunale.csv.php?&aa={year}"
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        out.write_text(r.text, encoding="utf-8")
        print(f"  Downloaded waste {year} ({out.stat().st_size / 1024:.0f} KB)")


# ---------------------------------------------------------------------------
# Air quality (station-level annual stats from ISPRA)
# ---------------------------------------------------------------------------

AIR_FILES = {
    "no2_2001_2022.csv": "https://www.isprambiente.gov.it/files2024/attivita/aria/2024-01-09-2001_2022-no2-_statistiche.csv",
    "pm10_2002_2022.csv": "https://www.isprambiente.gov.it/files2024/attivita/aria/2024-01-09_2002_2022_pm10__statistiche.csv",
    "pm25_2004_2022.csv": "https://www.isprambiente.gov.it/files2024/attivita/aria/2024-01-09_2004_2022_pm25__statistiche.csv",
    "o3_2002_2022.csv": "https://www.isprambiente.gov.it/files2024/attivita/aria/2024-02-26_2002_2022_o3_statistiche.csv",
}


def download_air():
    """Download air quality CSVs from ISPRA."""
    air_dir = RAW_DIR / "air"
    air_dir.mkdir(parents=True, exist_ok=True)
    for filename, url in AIR_FILES.items():
        out = air_dir / filename
        if out.exists():
            print(f"  SKIP air/{filename} (already exists)")
            continue
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        out.write_text(r.text, encoding="utf-8")
        print(f"  Downloaded air/{filename} ({out.stat().st_size / 1024:.0f} KB)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Downloading land consumption...")
    download_land()

    print("Downloading waste data...")
    download_waste()

    print("Downloading air quality data...")
    download_air()

    print("\nDone. Raw data:")
    for f in sorted(RAW_DIR.rglob("*")):
        if f.is_file():
            print(f"  {f.relative_to(RAW_DIR)} ({f.stat().st_size / 1024:.0f} KB)")


if __name__ == "__main__":
    main()
