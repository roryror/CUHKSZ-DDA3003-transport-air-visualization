#!/usr/bin/env python3
"""
Build a PyTorch-friendly tabular dataset for short-term pollution prediction.

First version:
- pollutant-specific
- one-hour-ahead regression
- previous N hours of transport features flattened into one row
- target is pollution anomaly
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


BASE_FEATURES = [
    "pickup_count",
    "dropoff_count",
    "turnover",
    "net_flow",
    "passenger_volume_out",
    "trip_distance_sum_out",
    "trip_duration_sum_out",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare one-hour-ahead modeling dataset.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/zone_analysis_timeseries_1year.csv"),
        help="Path to zone-level transport-pollution timeseries CSV.",
    )
    parser.add_argument(
        "--pollutant",
        type=str,
        default="pm25",
        help="Pollutant to model. Default: pm25",
    )
    parser.add_argument(
        "--window-hours",
        type=int,
        default=6,
        help="Number of previous hours used as features. Default: 6",
    )
    parser.add_argument(
        "--horizon-hours",
        type=int,
        default=1,
        help="Prediction horizon in hours. Default: 1",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/model_data"),
        help="Directory for prepared modeling data.",
    )
    parser.add_argument(
        "--min-zone-rows",
        type=int,
        default=48,
        help="Minimum rows required for a zone to participate. Default: 48",
    )
    parser.add_argument(
        "--step-hours",
        type=int,
        default=3,
        help="Stride between adjacent sliding windows. Default: 3",
    )
    return parser.parse_args()


def load_source_table(path: Path, pollutant: str) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Input file not found: {path}")

    df = pd.read_csv(
        path,
        usecols=[
            "datetime_hour",
            "zone_id",
            "borough",
            "zone_name",
            "parameter",
            "pollution",
            *BASE_FEATURES,
        ],
    )
    df = df[df["parameter"] == pollutant].copy()
    if df.empty:
        raise ValueError(f"No rows found for pollutant '{pollutant}' in {path}")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"])
    df = df.sort_values(["zone_id", "datetime_hour"]).reset_index(drop=True)
    return df


def add_anomaly_target(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["hour_of_day"] = df["datetime_hour"].dt.hour
    df["day_of_week"] = df["datetime_hour"].dt.dayofweek
    df["hour_sin"] = np.sin(2 * np.pi * df["hour_of_day"] / 24.0)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour_of_day"] / 24.0)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7.0)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7.0)

    baseline = (
        df.groupby(["zone_id", "hour_of_day"], observed=True)["pollution"]
        .mean()
        .rename("pollution_hour_baseline")
        .reset_index()
    )
    df = df.merge(baseline, on=["zone_id", "hour_of_day"], how="left")
    df["pollution_anomaly"] = df["pollution"] - df["pollution_hour_baseline"]
    return df


def build_samples(
    df: pd.DataFrame,
    window_hours: int,
    horizon_hours: int,
    min_zone_rows: int,
    step_hours: int,
) -> tuple[pd.DataFrame, list[str]]:
    df = df.copy()
    feature_columns = [
        *BASE_FEATURES,
        "pollution",
        "pollution_anomaly",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
    ]
    flattened_feature_names: list[str] = []
    group = df.groupby("zone_id", sort=False)

    for lag in range(window_hours - 1, -1, -1):
        suffix = f"t_minus_{lag}"
        for feature in feature_columns:
            flattened_feature_names.append(f"{feature}_{suffix}")
    min_required_rows = max(min_zone_rows, window_hours + horizon_hours)
    eligible_zone_ids = group.size()
    eligible_zone_ids = eligible_zone_ids[eligible_zone_ids >= min_required_rows].index
    df = df[df["zone_id"].isin(eligible_zone_ids)].copy()
    if df.empty:
        raise ValueError("No zones satisfied the minimum row threshold for modeling.")

    group = df.groupby("zone_id", sort=False)
    df["zone_row_number"] = group.cumcount()
    df["feature_end"] = group["datetime_hour"].shift(horizon_hours)
    df["feature_start"] = group["datetime_hour"].shift(horizon_hours + window_hours - 1)

    valid_mask = pd.Series(True, index=df.index)
    for lag in range(window_hours - 1, -1, -1):
        offset = horizon_hours + lag
        suffix = f"t_minus_{lag}"

        shifted_time = group["datetime_hour"].shift(offset)
        valid_mask &= (df["datetime_hour"] - shifted_time) == pd.Timedelta(hours=offset)

        for feature in feature_columns:
            df[f"{feature}_{suffix}"] = group[feature].shift(offset)

    sample_anchor = window_hours + horizon_hours - 1
    valid_mask &= ((df["zone_row_number"] - sample_anchor) % step_hours) == 0

    selected_columns = [
        "zone_id",
        "zone_name",
        "borough",
        "feature_start",
        "feature_end",
        "datetime_hour",
        "pollution",
        "pollution_anomaly",
        *flattened_feature_names,
    ]
    dataset = df.loc[valid_mask, selected_columns].copy()
    if dataset.empty:
        raise ValueError("No modeling samples were generated. Check window size or source data.")

    dataset = dataset.rename(
        columns={
            "datetime_hour": "target_datetime",
            "pollution": "target_pollution",
            "pollution_anomaly": "target_pollution_anomaly",
        }
    )
    for column in ["feature_start", "feature_end", "target_datetime"]:
        dataset[column] = pd.to_datetime(dataset[column]).dt.strftime("%Y-%m-%d %H:%M:%S")
    return dataset, flattened_feature_names


def assign_time_splits(df: pd.DataFrame) -> pd.DataFrame:
    unique_targets = np.array(sorted(pd.to_datetime(df["target_datetime"].unique())))
    n = len(unique_targets)
    train_end = unique_targets[min(max(int(n * 0.70), 0), n - 1)]
    val_end = unique_targets[min(max(int(n * 0.85), 0), n - 1)]

    target_ts = pd.to_datetime(df["target_datetime"])
    split = np.where(
        target_ts <= train_end,
        "train",
        np.where(target_ts <= val_end, "val", "test"),
    )
    df = df.copy()
    df["split"] = split
    return df


def write_outputs(
    dataset: pd.DataFrame,
    feature_names: list[str],
    output_dir: Path,
    pollutant: str,
    window_hours: int,
    horizon_hours: int,
    step_hours: int,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{pollutant}_w{window_hours}_h{horizon_hours}_s{step_hours}"

    parquet_path = output_dir / f"{stem}_dataset.parquet"
    metadata_path = output_dir / f"{stem}_metadata.json"
    dataset.to_parquet(parquet_path, index=False)

    split_counts = (
        dataset.groupby("split", observed=True).size().to_dict()
        if "split" in dataset.columns
        else {}
    )
    metadata = {
        "pollutant": pollutant,
        "window_hours": window_hours,
        "horizon_hours": horizon_hours,
        "step_hours": step_hours,
        "row_count": int(len(dataset)),
        "zone_count": int(dataset["zone_id"].nunique()),
        "feature_count": len(feature_names),
        "feature_names": feature_names,
        "target_column": "target_pollution_anomaly",
        "split_counts": {k: int(v) for k, v in split_counts.items()},
        "time_range": {
            "feature_start_min": dataset["feature_start"].min(),
            "target_datetime_max": dataset["target_datetime"].max(),
        },
    }
    metadata_path.write_text(json.dumps(metadata, indent=2))

    print("Prepared modeling dataset")
    print(f"  Rows: {len(dataset):,}")
    print(f"  Zones: {dataset['zone_id'].nunique():,}")
    print(f"  Features per sample: {len(feature_names):,}")
    print(f"  Split counts: {metadata['split_counts']}")
    print(f"  Dataset saved to: {parquet_path}")
    print(f"  Metadata saved to: {metadata_path}")


def main() -> int:
    args = parse_args()
    if args.step_hours < 1:
        raise ValueError("--step-hours must be at least 1")

    print(f"Loading source table from: {args.input}")
    source_df = load_source_table(args.input, args.pollutant)
    print(f"Loaded {len(source_df):,} rows for pollutant '{args.pollutant}'")

    print("Constructing anomaly target and calendar features...")
    source_df = add_anomaly_target(source_df)

    print("Building sliding-window samples...")
    dataset, feature_names = build_samples(
        source_df,
        window_hours=args.window_hours,
        horizon_hours=args.horizon_hours,
        min_zone_rows=args.min_zone_rows,
        step_hours=args.step_hours,
    )

    print("Assigning train/val/test splits...")
    dataset = assign_time_splits(dataset)
    write_outputs(
        dataset,
        feature_names,
        output_dir=args.output_dir,
        pollutant=args.pollutant,
        window_hours=args.window_hours,
        horizon_hours=args.horizon_hours,
        step_hours=args.step_hours,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
