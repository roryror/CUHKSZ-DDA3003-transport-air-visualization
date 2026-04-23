# Modeling Module

This directory contains the PyTorch-oriented predictive modeling workflow.

Current scope:

- task: one-hour-ahead pollution prediction
- first pollutant: `pm25`
- target: `target_pollution_anomaly`
- input style: previous `N` hours of flattened transport features plus recent pollution context
- model type: baseline `MLP` regressor in PyTorch

## Scripts

`prepare_training_data.py`

Purpose:

- read `zone_analysis_timeseries_1year.csv`
- filter one pollutant
- build zone-hour sliding-window samples
- compute `pollution_anomaly`
- include lagged pollution and anomaly features for short-term forecasting
- split samples into `train / val / test` by time
- save a parquet dataset and a metadata JSON

Key option:

- `--step-hours`: sample every `k` hours instead of every single hour, so year-scale data stays tractable

`train_mlp.py`

Purpose:

- load the prepared parquet dataset
- standardize features using the training split only
- train a PyTorch MLP regressor
- apply early stopping on validation RMSE
- save model weights, scaler stats, and metrics JSON

`export_prediction_assets.py`

Purpose:

- load the trained MLP checkpoint
- run inference on the held-out test split
- export per-zone prediction JSON files for the frontend
- export global and per-zone evaluation summaries

## Default configuration

- pollutant: `pm25`
- window: `6` hours
- horizon: `1` hour
- sample stride: `3` hours
- hidden layers: `128 -> 64`

## Output

Saved under `outputs/model_data/`:

- `pm25_w6_h1_s3_dataset.parquet`
- `pm25_w6_h1_s3_metadata.json`

Saved under `outputs/model_runs/pm25_mlp/` after training:

- `best_model.pt`
- `standardizer.npz`
- `metrics.json`

## Recommended run order

1. Prepare data:

```bash
.venv/bin/python src/modeling/prepare_training_data.py \
  --pollutant pm25 \
  --window-hours 6 \
  --horizon-hours 1 \
  --step-hours 3
```

2. Train baseline MLP:

```bash
.venv/bin/python src/modeling/train_mlp.py \
  --data outputs/model_data/pm25_w6_h1_s3_dataset.parquet \
  --metadata outputs/model_data/pm25_w6_h1_s3_metadata.json
```

3. Export frontend prediction assets:

```bash
.venv/bin/python src/modeling/export_prediction_assets.py
```

## Notes

- This modeling module is an auxiliary component for the project, not the main analysis method.
- The main story remains the interpretable statistical pipeline: zone-hour aggregation, delayed relationship analysis, and hotspot scoring.
