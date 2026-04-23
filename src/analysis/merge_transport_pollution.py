#!/usr/bin/env python3
"""
Merge zone-hour transport features with zone-level pollution estimates.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


TRANSPORT_ZERO_FILL_COLUMNS = [
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Merge transport, pollution, and taxi-zone metadata into one analysis table."
    )
    parser.add_argument(
        "--transport-input",
        type=Path,
        default=Path("outputs/zone_transport_timeseries.csv"),
        help="Transport features CSV from build_transport_timeseries.py",
    )
    parser.add_argument(
        "--pollution-input",
        type=Path,
        default=Path("code-B/output/zone_pollution_timeseries.csv"),
        help="Zone-level pollution CSV from Member B's spatial module.",
    )
    parser.add_argument(
        "--zone-lookup",
        type=Path,
        default=Path("data/taxi_data/original_data/map_table/taxi_zone_lookup.csv"),
        help="Taxi zone lookup table.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/zone_analysis_timeseries.csv"),
        help="Output merged analysis CSV.",
    )
    return parser.parse_args()


def load_transport(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Transport input not found: {path}")
    df = pd.read_csv(path)
    required_columns = {"datetime_hour", "zone_id"} | set(TRANSPORT_ZERO_FILL_COLUMNS)
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{path} is missing required columns: {sorted(missing_columns)}")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"], errors="coerce")
    df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce")
    df = df.dropna(subset=["datetime_hour", "zone_id"]).copy()
    df["zone_id"] = df["zone_id"].astype(int)
    return df


def load_pollution(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Pollution input not found: {path}")
    df = pd.read_csv(path)
    required_columns = {"datetime", "zone_id", "parameter", "pollution"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{path} is missing required columns: {sorted(missing_columns)}")

    df["datetime_hour"] = pd.to_datetime(df["datetime"], errors="coerce")
    df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce")
    df["pollution"] = pd.to_numeric(df["pollution"], errors="coerce")
    df = df.dropna(subset=["datetime_hour", "zone_id", "parameter", "pollution"]).copy()
    df["zone_id"] = df["zone_id"].astype(int)
    return df[["datetime_hour", "zone_id", "parameter", "pollution"]]


def load_zone_lookup(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Zone lookup not found: {path}")
    df = pd.read_csv(path)
    rename_map = {
        "LocationID": "zone_id",
        "Borough": "borough",
        "Zone": "zone_name",
        "service_zone": "service_zone",
    }
    df = df.rename(columns=rename_map)
    required_columns = {"zone_id", "borough", "zone_name", "service_zone"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{path} is missing required columns: {sorted(missing_columns)}")

    df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce")
    df = df.dropna(subset=["zone_id"]).copy()
    df["zone_id"] = df["zone_id"].astype(int)
    return df[["zone_id", "borough", "zone_name", "service_zone"]]


def merge_analysis_tables(
    transport_df: pd.DataFrame, pollution_df: pd.DataFrame, zone_lookup_df: pd.DataFrame
) -> pd.DataFrame:
    merged = pollution_df.merge(
        transport_df,
        how="left",
        on=["datetime_hour", "zone_id"],
    )
    for column in TRANSPORT_ZERO_FILL_COLUMNS:
        merged[column] = merged[column].fillna(0)

    merged["pickup_count"] = merged["pickup_count"].astype(int)
    merged["dropoff_count"] = merged["dropoff_count"].astype(int)
    merged["turnover"] = merged["turnover"].astype(int)
    merged["net_flow"] = merged["net_flow"].astype(int)

    merged = merged.merge(zone_lookup_df, how="left", on="zone_id")

    ordered_columns = [
        "datetime_hour",
        "zone_id",
        "borough",
        "zone_name",
        "service_zone",
        "parameter",
        "pollution",
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
    merged = merged[ordered_columns].sort_values(
        ["datetime_hour", "zone_id", "parameter"]
    ).reset_index(drop=True)
    return merged


def print_summary(
    transport_df: pd.DataFrame, pollution_df: pd.DataFrame, merged_df: pd.DataFrame
) -> None:
    print("Transport-pollution merge completed")
    print(f"  Transport rows: {len(transport_df):,}")
    print(f"  Pollution rows: {len(pollution_df):,}")
    print(f"  Output rows: {len(merged_df):,}")
    print(f"  Unique zones: {merged_df['zone_id'].nunique():,}")
    print(f"  Unique parameters: {merged_df['parameter'].nunique():,}")
    print(
        f"  Time range: {merged_df['datetime_hour'].min()} -> "
        f"{merged_df['datetime_hour'].max()}"
    )


def main() -> int:
    args = parse_args()
    transport_df = load_transport(args.transport_input)
    pollution_df = load_pollution(args.pollution_input)
    zone_lookup_df = load_zone_lookup(args.zone_lookup)
    merged_df = merge_analysis_tables(transport_df, pollution_df, zone_lookup_df)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    merged_df.to_csv(args.output, index=False)
    print_summary(transport_df, pollution_df, merged_df)
    print(f"  Saved to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
