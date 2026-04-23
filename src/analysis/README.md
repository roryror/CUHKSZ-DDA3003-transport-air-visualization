# Analysis Module

This module contains Member C's analysis-layer scripts.

## Phase 1

Build taxi mobility features at the `zone-hour` level.

### Input

* merged yellow taxi CSV
* merged green taxi CSV

### Output

* `zone_transport_timeseries.csv`
* `zone_analysis_timeseries.csv`

### Example

```bash
python src/analysis/build_transport_timeseries.py \
  --yellow-input data/taxi_data/taxi_data_20260215_20260222_20260408_114455/yellow_tripdata_merged.csv \
  --green-input data/taxi_data/taxi_data_20260215_20260222_20260408_114455/green_tripdata_merged.csv \
  --start 2026-02-15T00:00:00 \
  --end 2026-02-22T23:59:59 \
  --output outputs/zone_transport_timeseries.csv
```

```bash
python src/analysis/merge_transport_pollution.py \
  --transport-input outputs/zone_transport_timeseries.csv \
  --pollution-input code-B/output/zone_pollution_timeseries.csv \
  --zone-lookup data/taxi_data/original_data/map_table/taxi_zone_lookup.csv \
  --output outputs/zone_analysis_timeseries.csv
```

```bash
python src/analysis/analyze_correlation_lag.py \
  --input outputs/zone_analysis_timeseries.csv \
  --output outputs/zone_analysis_summary.csv
```
