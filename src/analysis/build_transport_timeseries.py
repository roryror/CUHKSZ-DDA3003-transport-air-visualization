#!/usr/bin/env python3
"""
Build zone-hour transport features from NYC taxi trip CSV files.

Supports either:
- one merged CSV per taxi type, or
- a directory containing monthly CSV files per taxi type.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


COMMON_COLUMNS = [
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Aggregate taxi CSV files into zone-hour transport features."
    )
    parser.add_argument(
        "--yellow-input",
        type=Path,
        required=True,
        help="Yellow taxi CSV file or directory containing yellow_tripdata_*.csv files.",
    )
    parser.add_argument(
        "--green-input",
        type=Path,
        required=True,
        help="Green taxi CSV file or directory containing green_tripdata_*.csv files.",
    )
    parser.add_argument(
        "--start",
        type=str,
        required=True,
        help="Inclusive window start, for example 2026-02-15T00:00:00.",
    )
    parser.add_argument(
        "--end",
        type=str,
        required=True,
        help="Inclusive window end, for example 2026-02-22T23:59:59.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/zone_transport_timeseries.csv"),
        help="Output CSV path.",
    )
    parser.add_argument(
        "--max-duration-min",
        type=float,
        default=1440.0,
        help="Discard trips longer than this duration in minutes. Default: 1440.",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=500_000,
        help="CSV chunk size for streaming large monthly files.",
    )
    return parser.parse_args()


def resolve_input_files(path: Path, prefix: str) -> list[Path]:
    if not path.exists():
        raise FileNotFoundError(f"Taxi input not found: {path}")
    if path.is_file():
        return [path]
    files = sorted(path.glob(f"{prefix}_tripdata_*.csv"))
    if not files:
        raise FileNotFoundError(f"No {prefix} CSV files found in {path}")
    return files


def parse_datetime_column(series: pd.Series) -> pd.Series:
    parsed = pd.to_datetime(series, format="%Y%m%d%H%M%S", errors="coerce")
    missing_mask = parsed.isna()
    if missing_mask.any():
        parsed.loc[missing_mask] = pd.to_datetime(series.loc[missing_mask], errors="coerce")
    return parsed


def normalize_chunk(df: pd.DataFrame, pickup_col: str, dropoff_col: str) -> pd.DataFrame:
    rename_map = {
        pickup_col: "pickup_datetime",
        dropoff_col: "dropoff_datetime",
    }
    df = df.rename(columns=rename_map)

    required_columns = set(COMMON_COLUMNS)
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"Chunk is missing required columns: {sorted(missing_columns)}")

    df = df[COMMON_COLUMNS].copy()
    df["pickup_datetime"] = parse_datetime_column(df["pickup_datetime"])
    df["dropoff_datetime"] = parse_datetime_column(df["dropoff_datetime"])
    df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce")
    df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce")
    df["PULocationID"] = pd.to_numeric(df["PULocationID"], errors="coerce")
    df["DOLocationID"] = pd.to_numeric(df["DOLocationID"], errors="coerce")

    df = df.dropna(
        subset=[
            "pickup_datetime",
            "dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
        ]
    ).copy()
    df["PULocationID"] = df["PULocationID"].astype(int)
    df["DOLocationID"] = df["DOLocationID"].astype(int)
    return df


def filter_time_window(
    df: pd.DataFrame, start: pd.Timestamp, end: pd.Timestamp, max_duration_min: float
) -> pd.DataFrame:
    filtered = df[(df["pickup_datetime"] >= start) & (df["pickup_datetime"] <= end)].copy()
    filtered["trip_duration_min"] = (
        filtered["dropoff_datetime"] - filtered["pickup_datetime"]
    ).dt.total_seconds() / 60.0
    filtered = filtered[
        (filtered["trip_duration_min"] >= 0)
        & (filtered["trip_duration_min"] <= max_duration_min)
        & (filtered["trip_distance"] > 0)
        & (filtered["passenger_count"] > 0)
        & (filtered["PULocationID"] > 0)
        & (filtered["DOLocationID"] > 0)
    ].copy()
    filtered["pickup_hour"] = filtered["pickup_datetime"].dt.floor("h")
    filtered["dropoff_hour"] = filtered["dropoff_datetime"].dt.floor("h")
    return filtered


def aggregate_pickup_chunk(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["pickup_hour", "PULocationID"], as_index=False)
        .agg(
            pickup_count=("pickup_datetime", "size"),
            passenger_volume_out=("passenger_count", "sum"),
            trip_distance_sum_out=("trip_distance", "sum"),
            trip_duration_sum_out=("trip_duration_min", "sum"),
        )
        .rename(columns={"pickup_hour": "datetime_hour", "PULocationID": "zone_id"})
    )


def aggregate_dropoff_chunk(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["dropoff_hour", "DOLocationID"], as_index=False)
        .agg(dropoff_count=("dropoff_datetime", "size"))
        .rename(columns={"dropoff_hour": "datetime_hour", "DOLocationID": "zone_id"})
    )


def combine_pickup_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame(
            columns=[
                "datetime_hour",
                "zone_id",
                "pickup_count",
                "passenger_volume_out",
                "trip_distance_sum_out",
                "trip_duration_sum_out",
            ]
        )
    combined = (
        pd.concat(parts, ignore_index=True)
        .groupby(["datetime_hour", "zone_id"], as_index=False)
        .sum()
    )
    combined["avg_trip_distance_out"] = combined["trip_distance_sum_out"] / combined["pickup_count"]
    combined["avg_trip_duration_out"] = combined["trip_duration_sum_out"] / combined["pickup_count"]
    return combined


def combine_dropoff_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    if not parts:
        return pd.DataFrame(columns=["datetime_hour", "zone_id", "dropoff_count"])
    return (
        pd.concat(parts, ignore_index=True)
        .groupby(["datetime_hour", "zone_id"], as_index=False)
        .sum()
    )


def process_input_files(
    input_path: Path,
    prefix: str,
    pickup_col: str,
    dropoff_col: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
    max_duration_min: float,
    chunksize: int,
) -> tuple[list[pd.DataFrame], list[pd.DataFrame], int, int]:
    files = resolve_input_files(input_path, prefix)
    usecols = [
        pickup_col,
        dropoff_col,
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
    ]

    pickup_parts: list[pd.DataFrame] = []
    dropoff_parts: list[pd.DataFrame] = []
    raw_rows = 0
    filtered_rows = 0

    for file_path in files:
        print(f"Reading {file_path.name}")
        for chunk in pd.read_csv(file_path, usecols=usecols, low_memory=False, chunksize=chunksize):
            raw_rows += len(chunk)
            normalized = normalize_chunk(chunk, pickup_col, dropoff_col)
            filtered = filter_time_window(normalized, start, end, max_duration_min)
            filtered_rows += len(filtered)
            if filtered.empty:
                continue
            pickup_parts.append(aggregate_pickup_chunk(filtered))
            dropoff_parts.append(aggregate_dropoff_chunk(filtered))

    return pickup_parts, dropoff_parts, raw_rows, filtered_rows


def combine_aggregations(
    pickup_agg: pd.DataFrame,
    dropoff_agg: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    combined = pickup_agg.merge(
        dropoff_agg,
        how="outer",
        on=["datetime_hour", "zone_id"],
    )

    numeric_columns = [
        "pickup_count",
        "dropoff_count",
        "passenger_volume_out",
        "trip_distance_sum_out",
        "trip_duration_sum_out",
        "avg_trip_distance_out",
        "avg_trip_duration_out",
    ]
    for column in numeric_columns:
        if column not in combined.columns:
            combined[column] = 0.0
    combined[numeric_columns] = combined[numeric_columns].fillna(0)

    combined["pickup_count"] = combined["pickup_count"].astype(int)
    combined["dropoff_count"] = combined["dropoff_count"].astype(int)
    combined["turnover"] = combined["pickup_count"] + combined["dropoff_count"]
    combined["net_flow"] = combined["dropoff_count"] - combined["pickup_count"]

    ordered_columns = [
        "datetime_hour",
        "zone_id",
        "pickup_count",
        "dropoff_count",
        "turnover",
        "net_flow",
        "passenger_volume_out",
        "trip_distance_sum_out",
        "trip_duration_sum_out",
        "avg_trip_distance_out",
        "avg_trip_duration_out",
    ]
    start_hour = start.floor("h")
    end_hour = end.floor("h")
    combined = combined[
        (combined["datetime_hour"] >= start_hour) & (combined["datetime_hour"] <= end_hour)
    ].copy()
    combined = combined[ordered_columns].sort_values(["datetime_hour", "zone_id"]).reset_index(drop=True)
    return combined


def print_summary(raw_rows: int, filtered_rows: int, output_df: pd.DataFrame) -> None:
    print("Transport aggregation completed")
    print(f"  Raw trip rows loaded: {raw_rows:,}")
    print(f"  Rows in target pickup window: {filtered_rows:,}")
    print(f"  Output rows: {len(output_df):,}")
    print(f"  Unique zones: {output_df['zone_id'].nunique():,}")
    if not output_df.empty:
        print(f"  Time range: {output_df['datetime_hour'].min()} -> {output_df['datetime_hour'].max()}")


def main() -> int:
    args = parse_args()
    start = pd.Timestamp(args.start)
    end = pd.Timestamp(args.end)
    if end < start:
        raise ValueError("End time must be greater than or equal to start time.")

    yellow_pickup_parts, yellow_dropoff_parts, yellow_raw_rows, yellow_filtered_rows = process_input_files(
        input_path=args.yellow_input,
        prefix="yellow",
        pickup_col="tpep_pickup_datetime",
        dropoff_col="tpep_dropoff_datetime",
        start=start,
        end=end,
        max_duration_min=args.max_duration_min,
        chunksize=args.chunksize,
    )
    green_pickup_parts, green_dropoff_parts, green_raw_rows, green_filtered_rows = process_input_files(
        input_path=args.green_input,
        prefix="green",
        pickup_col="lpep_pickup_datetime",
        dropoff_col="lpep_dropoff_datetime",
        start=start,
        end=end,
        max_duration_min=args.max_duration_min,
        chunksize=args.chunksize,
    )

    pickup_agg = combine_pickup_parts(yellow_pickup_parts + green_pickup_parts)
    dropoff_agg = combine_dropoff_parts(yellow_dropoff_parts + green_dropoff_parts)
    output_df = combine_aggregations(pickup_agg, dropoff_agg, start, end)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    output_df.to_csv(args.output, index=False)
    print_summary(
        raw_rows=yellow_raw_rows + green_raw_rows,
        filtered_rows=yellow_filtered_rows + green_filtered_rows,
        output_df=output_df,
    )
    print(f"  Saved to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
