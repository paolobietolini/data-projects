# Polish Baby Names

Trends in first names given to children in Poland (2000–2025), with national bump charts and regional breakdowns by voivodeship.

## Data Source

All data comes from the [Polish Open Data API](https://api.dane.gov.pl/) (Ministerstwo Cyfryzacji). The notebook fetches CSVs and paginated JSON automatically, caching everything locally.

| Dataset | Years | Granularity | Schema |
|---------|-------|-------------|--------|
| Consolidated national | 2000–2019 | country | `(year, name, sex, count)` |
| Yearly national (M/F) | 2020–2025 | country | `(name, sex, count)` |
| Regional (M/F) | 2025 | voivodeship | `(voivodeship, name, sex, count)` |

> The raw CSVs have inconsistent Polish headers across years (e.g. `IMIĘ_PIERWSZE` vs `IMIĘ PIERWSZE`, a 2022 typo `LICZBA WYSTĄPIENIEŃ`). The notebook normalizes all of this on load.

## Project Structure

```
polish-names/
├── notebook.ipynb          # Main analysis notebook
├── raw-data/               # Cached CSVs (auto-downloaded on first run)
│   ├── data_2000_2019.csv
│   ├── data_2020_M.csv … data_2025_F.csv
│   ├── regional_2025_M.csv
│   └── regional_2025_F.csv
├── reports/                # Generated interactive HTML charts
│   ├── bump_chart_male.html
│   ├── bump_chart_female.html
│   ├── regional_top3_male_2025.html
│   └── regional_top3_female_2025.html
├── pyproject.toml
└── README.md
```

## Visualizations

### Bump Charts — Top 10 Names (2015–2025)

Rank the most popular names nationally per year, then trace how each name's rank moves over the last decade. One chart per sex, interactive (Plotly).

### Regional Top 3 — 2025

Horizontal bar charts showing the top 3 boy and girl names in each of Poland's 16 voivodeships, with name labels and counts. Shows regional variation at a glance.

## Setup

```bash
uv sync

uv run jupyter lab
```

Then open `notebook.ipynb` and run all cells. On first run the notebook downloads ~15 CSVs from the API (takes a minute or two); subsequent runs use the cached files in `raw-data/`.

## License

Data: open data, Polish government ([dane.gov.pl](https://dane.gov.pl/)).
