# NYC Taxi Mobility & Air Quality Explorer

Interactive visual analytics project for exploring the relationship between NYC taxi mobility and air quality.

## Project Structure

- `src/data_processing/`
  - raw data download, cleaning, and merge pipeline
- `src/analysis/`
  - zone-hour transport aggregation
  - transport-pollution merge
  - lag analysis and hotspot-style summary export
- `src/modeling/`
  - auxiliary PyTorch short-term forecasting workflow
- `frontend/`
  - static dashboard for map, ranking, detail views, and prediction module
- `presentation_notes.md`
  - English demo notes for group presentation

## Installation

Create a virtual environment and install dependencies:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Data Processing

7-day demo run:

```bash
python3 src/data_processing/main.py --download-taxi --end-date 2026-02-22 --days 7
```

One-year run:

```bash
python3 src/data_processing/main.py --download-taxi --end-date 2026-02-22 --days 365
```

Air-only run:

```bash
python3 src/data_processing/air_handler/main.py --end-date 2026-02-22 --days 365
```

## Analysis Workflow

The main analysis method is statistical and interpretable:

1. Build zone-hour transport features
2. Merge transport and pollution series
3. Compute delayed relationships and priority metrics
4. Export frontend-ready assets

Example run order:

```bash
python3 src/analysis/build_transport_timeseries.py \
  --yellow-input data/taxi_data/<run>/yellow_tripdata_merged.csv \
  --green-input data/taxi_data/<run>/green_tripdata_merged.csv \
  --start 2025-03-01T00:00:00 \
  --end 2026-02-27T23:00:00 \
  --output outputs/zone_transport_timeseries.csv
```

```bash
python3 src/analysis/merge_transport_pollution.py \
  --transport-input outputs/zone_transport_timeseries.csv \
  --pollution-input <member-b-output>/zone_pollution_timeseries.csv \
  --zone-lookup data/taxi_data/original_data/map_table/taxi_zone_lookup.csv \
  --output outputs/zone_analysis_timeseries.csv
```

```bash
python3 src/analysis/analyze_correlation_lag.py \
  --input outputs/zone_analysis_timeseries.csv \
  --output outputs/zone_analysis_summary.csv
```

```bash
python3 src/analysis/export_frontend_assets.py \
  --summary-input outputs/zone_analysis_summary.csv \
  --timeseries-input outputs/zone_analysis_timeseries.csv \
  --zones-input data/taxi_data/original_data/map_table/taxi_zones.shp \
  --output-dir frontend/public/data
```

More detail is in [src/analysis/README.md](/Users/sonnet/Desktop/dda3003/CUHKSZ-DDA3003-transport-air-visualization-main/src/analysis/README.md).

## Frontend

Run the static frontend locally:

```bash
cd frontend
python3 -m http.server 4173
```

Then open:

```text
http://localhost:4173
```

The frontend reads exported assets from `frontend/public/data/`.

These data assets are generated locally and are not committed in this repository because they are large. Rebuild them from the analysis and modeling scripts before running the full dashboard on a fresh machine.

## PyTorch Forecasting Module

The modeling module is an auxiliary component, not the main method.

Current configuration:

- pollutant: `pm25`
- target: next-hour pollution change / anomaly
- input window: previous `6` hours
- model: PyTorch MLP baseline

Recommended run order:

```bash
.venv/bin/python src/modeling/prepare_training_data.py \
  --pollutant pm25 \
  --window-hours 6 \
  --horizon-hours 1 \
  --step-hours 3
```

```bash
.venv/bin/python src/modeling/train_mlp.py \
  --data outputs/model_data/pm25_w6_h1_s3_dataset.parquet \
  --metadata outputs/model_data/pm25_w6_h1_s3_metadata.json
```

```bash
.venv/bin/python src/modeling/export_prediction_assets.py
```

More detail is in [src/modeling/README.md](/Users/sonnet/Desktop/dda3003/CUHKSZ-DDA3003-transport-air-visualization-main/src/modeling/README.md).

## Notes

- Large local artifacts such as `.venv/`, `outputs/`, `frontend/public/data/`, backups, and collaborator dump folders are ignored.
- The repository is intended to store source code, scripts, configuration, and lightweight documentation.
