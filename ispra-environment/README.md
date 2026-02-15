# ISPRA Environmental Data

Three environmental datasets covering every Italian municipality, sourced from ISPRA (Istituto Superiore per la Protezione e la Ricerca Ambientale).

## Datasets

| Dataset | Granularity | Period | Rows |
|---------|-------------|--------|------|
| **Land consumption** | ~7,900 comuni | 2006–2024 | 7,896 |
| **Municipal waste** | ~7,900 comuni × 15 years | 2010–2024 | 119,840 |
| **Air quality** | ~650 monitoring stations | 2001–2022 | 30,865 |

All data is free and public.

## Sources

- **Land consumption**: [ISPRA Consumo di Suolo](https://www.isprambiente.gov.it/it/attivita/suolo-e-territorio/suolo/il-consumo-di-suolo/i-dati-sul-consumo-di-suolo) — hectares of soil sealed by urbanization per comune, with net/gross increments per period
- **Waste**: [Catasto Nazionale Rifiuti](https://www.catasto-rifiuti.isprambiente.it/) — total municipal waste, separate collection by material type (organic, paper, glass, plastic, etc.), recycling rates
- **Air quality**: [ISPRA Qualità dell'Aria](https://www.isprambiente.gov.it/it/banche-dati/banche-dati-folder/aria/qualita-dellaria) — station-level annual statistics for NO2, PM10, PM2.5, and O3

## Project Structure

```
ispra-environment/
├── scripts/
│   ├── ingest.py          # Download all raw data
│   └── load_duckdb.py     # Load into DuckDB
├── raw/
│   ├── land/              # Land consumption XLSX
│   ├── waste/             # Municipal waste CSVs (one per year)
│   └── air/               # Air quality CSVs (one per pollutant)
├── pyproject.toml
└── README.md
```

## Setup

```bash
cd ispra-environment
uv venv
uv pip install -r pyproject.toml
```

## Usage

### Download raw data

```bash
python scripts/ingest.py
```

Downloads land consumption XLSX, 15 years of waste CSVs, and 4 air quality CSVs. Skips files already present.

### Load into DuckDB

```bash
python scripts/load_duckdb.py
```

Creates `ispra.duckdb` with 6 tables:

| Table | Description |
|-------|-------------|
| `land_consumption` | Land consumption per comune (wide format: increments per period + 2024 totals) |
| `waste` | Municipal waste per comune per year (all values as strings with Italian number formatting) |
| `air_no2` | NO2 station-level annual statistics (2001–2022) |
| `air_pm10` | PM10 station-level annual statistics (2002–2022) |
| `air_pm25` | PM2.5 station-level annual statistics (2004–2022) |
| `air_o3` | O3 station-level annual statistics (2002–2022) |

### Example Queries

```sql
-- Top 10 comuni by soil consumed
SELECT Nome_Comune, Nome_Provincia,
       "Suolo consumato 2024 [ettari]" AS ha,
       "Suolo consumato 2024 [%]" AS pct
FROM land_consumption
ORDER BY ha DESC
LIMIT 10;

-- Rome's recycling rate over time
SELECT anno, Comune, "Percentuale RD (%)"
FROM waste
WHERE Comune = 'ROMA'
ORDER BY anno;

-- Best recyclers among big cities (pop > 100k)
SELECT Comune, Provincia, Popolazione, "Percentuale RD (%)"
FROM waste
WHERE anno = 2024
  AND TRY_CAST(REPLACE(Popolazione, '.', '') AS INT) > 100000
ORDER BY TRY_CAST(REPLACE(REPLACE("Percentuale RD (%)", '%', ''), ',', '.') AS DOUBLE) DESC
LIMIT 20;

-- Worst NO2 stations in 2022
SELECT nome_stazione, Comune, Regione, media_yy AS annual_mean_ug_m3
FROM air_no2
WHERE yy = 2022
ORDER BY TRY_CAST(media_yy AS INT) DESC
LIMIT 20;
```

### Note on Waste Data Formatting

The waste CSVs use Italian number formatting: commas for decimals, dots for thousands (e.g., `1.642.827,258` = 1,642,827.258 tonnes). Values are loaded as strings. To query numerically:

```sql
SELECT Comune,
       TRY_CAST(REPLACE(REPLACE("Totale RU (t)", '.', ''), ',', '.') AS DOUBLE) AS total_waste_tonnes
FROM waste
WHERE anno = 2024
ORDER BY total_waste_tonnes DESC
LIMIT 10;
```

## License

Data: IODL (Italian Open Data License) for ISPRA/Catasto Rifiuti data.
