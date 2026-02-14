# ISPRA — Italy's Environmental Pulse

Track Italy's environmental transition through ISPRA open data: land consumption, waste management, and air quality at the municipal level.

## Architecture

```
ISPRA data sources (web portals, SPARQL, WFS, CSV)
        │
        ▼
  Python extraction scripts (scheduled weekly/monthly)
        │
        ▼
  Raw Parquet files (one per dataset per extraction)
        │
        ▼
  DuckDB (warehouse)
        │
        ▼
  dbt models (staging → intermediate → marts)
```

## Data Sources

Three complementary datasets, all free:

| Dataset | Portal | Granularity | Period | Format |
|---------|--------|-------------|--------|--------|
| Land consumption (consumo di suolo) | isprambiente.gov.it | Comune | 2006–2024 | Shapefile, WFS, CSV |
| Municipal waste (rifiuti urbani) | catasto-rifiuti.isprambiente.it | Comune | 2010–2023 | Web scrape → CSV |
| Air quality | dati.isprambiente.it + regional ARPAs | Station | 2002–2024 | SPARQL/LOD, CSV |

---

## Step 0: Project Setup

```bash
mkdir -p ispra-environment/{raw/{land,waste,air},scripts,dbt}
cd ispra-environment

python -m venv .venv
source .venv/bin/activate

pip install requests pandas pyarrow duckdb beautifulsoup4 geopandas shapely
```

---

## Dataset 1: Land Consumption (Consumo di Suolo)

This is the strongest dataset. ISPRA publishes annual reports with commune-level data on how much soil has been sealed by urbanization.

### Where to Get It

**Option A: Download the CSV/Shapefile directly (recommended)**

ISPRA publishes the data tables at:
https://www.isprambiente.gov.it/it/attivita/suolo-e-territorio/suolo/il-consumo-di-suolo/i-dati-sul-consumo-di-suolo

Look for the "Dati comunali" (municipal data) download. It's usually an Excel or CSV with columns like:
- `COD_REG`, `COD_PRO`, `PRO_COM` — ISTAT codes
- `COMUNE` — municipality name
- `SUP_COM_HA` — total area (hectares)
- `CS_20XX_HA` — consumed soil in year XXXX (hectares)
- `CS_20XX_PERC` — consumed soil as % of total area
- `INCR_XX_XX_HA` — increment between two years

**Option B: WFS Service (for geospatial data)**

ISPRA exposes OGC Web Feature Services. You can query them with `geopandas`:

```python
import geopandas as gpd

# The WFS URL may change — check EcoAtlante or ISPRA geoportal for current endpoints
# Example pattern:
wfs_url = "https://geoserver.isprambiente.it/geoserver/ows"

# List available layers
params = {
    "service": "WFS",
    "version": "2.0.0",
    "request": "GetCapabilities",
}

# Once you find the land consumption layer name, fetch it:
gdf = gpd.read_file(
    f"{wfs_url}?service=WFS&version=2.0.0&request=GetFeature"
    f"&typeName=LAYER_NAME_HERE&outputFormat=json"
)
```

> **Note**: The WFS endpoint URL and layer names may need discovery. Start with the direct CSV download, fall back to WFS if you want geometries.

**Option C: SPARQL (Linked Open Data)**

```python
import requests
import pandas as pd

SPARQL_ENDPOINT = "https://dati.isprambiente.it/sparql"

query = """
PREFIX soilc: <https://dati.isprambiente.it/ontology/soilc/>
PREFIX place: <https://dati.isprambiente.it/ontology/place/>

SELECT ?comune ?name ?year ?consumed_ha ?consumed_pct
WHERE {
    ?obs soilc:refArea ?comune ;
         soilc:refPeriod ?year ;
         soilc:consumedSoilHa ?consumed_ha ;
         soilc:consumedSoilPct ?consumed_pct .
    ?comune place:name ?name .
}
LIMIT 100
"""

response = requests.get(SPARQL_ENDPOINT, params={
    "query": query,
    "format": "application/sparql-results+json"
})
results = response.json()

# Parse into DataFrame
rows = [{k: v["value"] for k, v in r.items()} for r in results["results"]["bindings"]]
df = pd.DataFrame(rows)
```

> **Important**: The SPARQL ontology URIs above are examples — you'll need to explore the actual schema. Start by browsing https://dati.isprambiente.it/ld/catalog/html to find the `soilc` dataset and its properties.

### Extraction Script

Create `scripts/extract_land.py`:

```python
"""
Download ISPRA land consumption data.
Start with the manual CSV download, then automate once you know the URL pattern.
"""
import pandas as pd
from datetime import datetime

# After downloading the CSV/Excel manually, load it:
def load_land_consumption(filepath):
    """Load and clean the ISPRA land consumption CSV."""
    df = pd.read_csv(filepath, encoding="latin-1")  # or read_excel

    # Standardize column names (adapt to actual columns)
    # Typical columns: PRO_COM, COMUNE, PROVINCIA, REGIONE, SUP_COM_HA, CS_2006_HA, ..., CS_2023_HA

    # Melt wide format into long format (one row per comune per year)
    year_cols = [c for c in df.columns if c.startswith("CS_") and c.endswith("_HA")]
    id_cols = [c for c in df.columns if c not in year_cols]

    df_long = df.melt(
        id_vars=["PRO_COM", "COMUNE", "PROVINCIA", "REGIONE", "SUP_COM_HA"],
        value_vars=year_cols,
        var_name="year_col",
        value_name="consumed_soil_ha",
    )

    # Extract year from column name (CS_2023_HA -> 2023)
    df_long["year"] = df_long["year_col"].str.extract(r"(\d{4})").astype(int)
    df_long["consumed_soil_pct"] = df_long["consumed_soil_ha"] / df_long["SUP_COM_HA"] * 100

    df_long = df_long.drop(columns=["year_col"])
    return df_long

# Save as parquet
# df = load_land_consumption("raw/land/consumo_suolo_comunale.csv")
# df.to_parquet("raw/land/land_consumption.parquet", index=False)
```

### What You'll Find

Key stats from recent reports:
- Italy consumed **83.7 km²** of soil in 2024 alone (+15.6% vs 2023)
- Lombardia, Veneto, Emilia-Romagna are the worst offenders
- Rome's municipality: ~30% urbanized, still growing at the edges

---

## Dataset 2: Municipal Waste (Rifiuti Urbani)

### Where to Get It

**Catasto Nazionale Rifiuti**: https://www.catasto-rifiuti.isprambiente.it/

The web interface lets you search by comune, but there's no direct bulk download API. You have two options:

**Option A: Download the annual report data tables**

ISPRA publishes summary tables with each annual report:
https://www.isprambiente.gov.it/it/pubblicazioni/rapporti/rapporto-rifiuti-urbani-edizione-2024

Look for "Appendici statistiche" — these are usually Excel files with municipal-level data.

**Option B: Scrape the Catasto search interface**

The Catasto has a search-by-comune interface. You can automate it:

```python
"""
Scrape waste data from Catasto Nazionale Rifiuti.
Be respectful: add delays between requests, cache results.
"""
import requests
from bs4 import BeautifulSoup
import time
import pandas as pd

BASE_URL = "https://www.catasto-rifiuti.isprambiente.it"

def get_waste_data(istat_code, year):
    """
    Fetch waste data for a specific comune and year.
    You'll need to inspect the actual form/URL structure.
    """
    # Step 1: Find the search form and its parameters
    # Step 2: Submit with ISTAT code and year
    # Step 3: Parse the results table

    # This is a skeleton — inspect the actual site to fill in the details.
    # The site uses PHP, so look for form action URLs and POST parameters.

    url = f"{BASE_URL}/index.php"
    params = {
        "pg": "detComune",
        "aa": str(year),
        "reession": istat_code[:2],   # region code
        "idprovincia": istat_code[:3], # province code
        "idcomune": istat_code,        # full ISTAT code
    }

    response = requests.get(url, params=params)
    soup = BeautifulSoup(response.text, "html.parser")

    # Parse the data tables — adapt selectors to the actual page structure
    # Expected data: total waste (kg), separate collection (kg), % recycling

    time.sleep(1)  # be polite
    return None  # fill in actual parsing

# To scrape all ~8,000 comuni:
# 1. Get list of ISTAT codes (from ISTAT or the land consumption dataset)
# 2. Loop through comuni and years
# 3. Save incrementally to avoid losing progress
```

> **Tip**: Before scraping all 8,000 comuni, try the annual report Excel appendices first. They may already have the bulk data you need without scraping.

### Expected Schema

```
comune_istat_code | year | total_waste_kg | separate_collection_kg | recycling_pct |
unsorted_waste_kg | per_capita_kg | cost_per_capita_eur
```

---

## Dataset 3: Air Quality

### Where to Get It

Air quality data in Italy is fragmented across regional ARPA agencies. ISPRA aggregates some of it.

**Option A: ISPRA Linked Open Data**

The SPARQL endpoint at https://dati.isprambiente.it/sparql has some air quality datasets. Browse the catalog at https://dati.isprambiente.it/ld/catalog/html.

**Option B: European Environment Agency (easier for bulk data)**

The EEA aggregates all EU air quality data reported by member states:

```bash
# Download Italy's air quality data from EEA
# https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
# Or use the API:
curl "https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw?\
CountryCode=IT&Pollutant=6001&Year_from=2020&Year_to=2024&Source=All&Output=TEXT&TimeCoverage=Year" \
  -o raw/air/italy_pm10_2020_2024.csv
```

Pollutant codes: `6001`=PM10, `5029`=PM2.5, `8`=NO2, `7`=O3, `10`=CO

**Option C: ARPA Lazio (Rome-specific)**

For Rome specifically, ARPA Lazio publishes real-time and historical data:
https://www.arpalazio.it/web/guest/aria/rilevamenti-automatici-aria

```python
# ARPA Lazio may have a data download section — check the website
# Some regional ARPAs have REST APIs, others have CSV downloads
```

### Extraction Script

Create `scripts/extract_air.py`:

```python
"""
Download air quality data from EEA (covers all Italian stations).
"""
import requests
import pandas as pd

EEA_BASE = "https://fme.discomap.eea.europa.eu/fmedatastreaming/AirQualityDownload/AQData_Extract.fmw"

POLLUTANTS = {
    "PM10": 6001,
    "PM2.5": 5029,
    "NO2": 8,
    "O3": 7,
}

def download_pollutant(pollutant_name, pollutant_code, year_from, year_to):
    """Download air quality data from EEA for Italy."""
    params = {
        "CountryCode": "IT",
        "Pollutant": pollutant_code,
        "Year_from": year_from,
        "Year_to": year_to,
        "Source": "All",
        "Output": "TEXT",
        "TimeCoverage": "Year",
    }

    response = requests.get(EEA_BASE, params=params, timeout=300)

    outpath = f"raw/air/italy_{pollutant_name.lower()}_{year_from}_{year_to}.csv"
    with open(outpath, "w") as f:
        f.write(response.text)

    print(f"Saved {outpath} ({len(response.text)} bytes)")

# Download each pollutant
# for name, code in POLLUTANTS.items():
#     download_pollutant(name, code, 2015, 2024)
```

### Expected Schema

From the EEA download:
```
station_code | station_name | lat | lon | pollutant | concentration | unit |
datetime_begin | datetime_end | validity | verification
```

---

## Step 1: Load into DuckDB

Once you have the raw data:

```sql
-- duckdb ispra.duckdb

-- Land consumption
CREATE TABLE raw_land_consumption AS
  SELECT * FROM read_parquet('raw/land/land_consumption.parquet');

-- Waste (if you got the Excel appendices)
CREATE TABLE raw_waste AS
  SELECT * FROM read_parquet('raw/waste/municipal_waste.parquet');

-- Air quality
CREATE TABLE raw_air_quality AS
  SELECT * FROM read_csv('raw/air/italy_*.csv', auto_detect=true, union_by_name=true);

-- ISTAT reference table (reuse from the Rome project or download fresh)
-- Useful for joining comune codes to names, provinces, regions
```

Quick sanity checks:

```sql
-- Top 20 comuni by soil consumption increase (latest year)
SELECT COMUNE, PROVINCIA, REGIONE,
       consumed_soil_ha,
       consumed_soil_pct,
       consumed_soil_ha - LAG(consumed_soil_ha) OVER (PARTITION BY PRO_COM ORDER BY year) AS annual_increase_ha
FROM raw_land_consumption
WHERE year = 2023
ORDER BY annual_increase_ha DESC NULLS LAST
LIMIT 20;

-- Recycling rate by region
SELECT REGIONE,
       avg(recycling_pct) AS avg_recycling_pct,
       count(DISTINCT comune_istat_code) AS n_comuni
FROM raw_waste
WHERE year = 2022
GROUP BY 1
ORDER BY avg_recycling_pct DESC;

-- Annual PM10 exceedances by station (days above 50 µg/m³)
SELECT station_name, EXTRACT(YEAR FROM datetime_begin) AS year,
       count(*) FILTER (WHERE concentration > 50) AS exceedance_days
FROM raw_air_quality
WHERE pollutant = 'PM10'
GROUP BY 1, 2
ORDER BY exceedance_days DESC
LIMIT 20;
```

---

## Step 2: dbt Models

```
dbt/
  models/
    staging/
      stg_land_consumption.sql   -- normalize codes, compute YoY change
      stg_waste.sql              -- clean units, compute per-capita
      stg_air_quality.sql        -- pivot pollutants, daily averages
      stg_comuni.sql             -- ISTAT reference (codes, names, regions, population)
    intermediate/
      int_land_waste_joined.sql  -- combine land + waste per comune
      int_air_daily.sql          -- daily station averages per pollutant
      int_air_comunale.sql       -- assign stations to nearest comune
    marts/
      fct_land_consumption.sql       -- annual soil sealing by comune with trends
      fct_waste_performance.sql      -- recycling rates, per-capita waste, rankings
      fct_air_quality_annual.sql     -- annual pollutant stats by station/area
      fct_environmental_scorecard.sql -- composite score per comune (land + waste + air)
      dim_comuni.sql                 -- comuni with region, province, population, area
```

The killer analysis: **fct_environmental_scorecard** — rank every Italian comune on a composite index of soil consumption + recycling rate + air quality. Nobody has done this at this granularity.

---

## Step 3: Refresh Schedule

| Dataset | Refresh | Why |
|---------|---------|-----|
| Land consumption | Yearly (October, when ISPRA publishes the new report) | Annual satellite analysis |
| Waste | Yearly (November/December, with new Rapporto Rifiuti) | Annual reporting cycle |
| Air quality | Monthly or quarterly (EEA data validated with ~6 month lag) | Station data needs validation |

---

## Expected Data Volume

- Land consumption: ~8,000 comuni × ~18 years = **~144K rows** (tiny)
- Waste: ~8,000 comuni × ~13 years = **~104K rows** (tiny)
- Air quality: ~650 stations × 365 days × 4 pollutants × 10 years = **~9.5M rows** (still small for DuckDB)

This is a "small data, big insight" project. The challenge is data collection and joining, not scale.

---

## Ideas for Visualization

- **Choropleth map**: soil consumption % by comune, color-coded
- **Scatter plot**: recycling rate vs. land consumption — are green comuni consistent?
- **Time series**: PM10 trends in Rome vs. Milan vs. Naples over 10 years
- **Rankings**: best and worst comuni on the composite environmental scorecard
- **Small multiples**: 20 regional maps showing soil consumption change 2006→2024

---

## Useful Links

- ISPRA Land Consumption Data: https://www.isprambiente.gov.it/it/attivita/suolo-e-territorio/suolo/il-consumo-di-suolo/i-dati-sul-consumo-di-suolo
- EcoAtlante (interactive maps): https://ecoatlante.isprambiente.it/
- Catasto Nazionale Rifiuti: https://www.catasto-rifiuti.isprambiente.it/
- ISPRA Linked Open Data: https://dati.isprambiente.it/
- ISPRA SPARQL Endpoint: https://dati.isprambiente.it/sparql
- EEA Air Quality Data: https://discomap.eea.europa.eu/map/fme/AirQualityExport.htm
- ARPA Lazio: https://www.arpalazio.it/
- ISPRA Annual Reports: https://www.isprambiente.gov.it/it/pubblicazioni/rapporti

## License

Data: IODL (Italian Open Data License) for ISPRA data, EEA data is open.
