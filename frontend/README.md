# Frontend

Static dashboard for the NYC taxi mobility and air quality project.

## Run locally

From the project root:

```bash
cd frontend
python3 -m http.server 4173
```

Then open:

```text
http://localhost:4173
```

## Data assets

The frontend reads pre-exported JSON and GeoJSON from:

- `frontend/public/data/meta.json`
- `frontend/public/data/summary.json`
- `frontend/public/data/timeseries/*.json`
- `frontend/public/data/zones.geojson`

## Refresh frontend data

From the project root:

```bash
.venv/bin/python src/analysis/export_frontend_assets.py \
  --summary-input outputs/zone_analysis_summary.csv \
  --timeseries-input outputs/zone_analysis_timeseries.csv \
  --zones-input code-B/data/taxi_zones/taxi_zones.shp \
  --output-dir frontend/public/data
```
