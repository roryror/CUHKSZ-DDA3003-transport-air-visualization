#!/usr/bin/env python3
"""
Compute anomaly-based correlation, lag, and hotspot summaries.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze zone-level transport-pollution relationships."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("outputs/zone_analysis_timeseries.csv"),
        help="Merged analysis timeseries CSV.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("outputs/zone_analysis_summary.csv"),
        help="Output summary CSV.",
    )
    parser.add_argument(
        "--lag-max",
        type=int,
        default=24,
        help="Maximum lag in hours to evaluate.",
    )
    parser.add_argument(
        "--min-samples",
        type=int,
        default=24,
        help="Minimum aligned samples required to compute statistics.",
    )
    parser.add_argument(
        "--cooccur-quantile",
        type=float,
        default=0.8,
        help="Quantile threshold used for anomaly co-occurrence.",
    )
    return parser.parse_args()


def load_analysis_table(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Analysis input not found: {path}")

    df = pd.read_csv(path)
    required_columns = {
        "datetime_hour",
        "zone_id",
        "borough",
        "zone_name",
        "parameter",
        "pollution",
        "turnover",
    }
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise ValueError(f"{path} is missing required columns: {sorted(missing_columns)}")

    df["datetime_hour"] = pd.to_datetime(df["datetime_hour"], errors="coerce")
    df["zone_id"] = pd.to_numeric(df["zone_id"], errors="coerce")
    df["pollution"] = pd.to_numeric(df["pollution"], errors="coerce")
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce")
    df = df.dropna(subset=["datetime_hour", "zone_id", "parameter", "pollution", "turnover"]).copy()
    df["zone_id"] = df["zone_id"].astype(int)
    return df.sort_values(["zone_id", "parameter", "datetime_hour"]).reset_index(drop=True)


def add_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    traffic_df = df[["datetime_hour", "zone_id", "turnover"]].drop_duplicates().copy()
    traffic_df["hour_of_day"] = traffic_df["datetime_hour"].dt.hour
    traffic_df["log_turnover"] = np.log1p(traffic_df["turnover"])
    traffic_df["traffic_baseline"] = traffic_df.groupby(["zone_id", "hour_of_day"])[
        "log_turnover"
    ].transform("mean")
    traffic_df["traffic_anomaly"] = traffic_df["log_turnover"] - traffic_df["traffic_baseline"]

    merged = df.merge(
        traffic_df[["datetime_hour", "zone_id", "traffic_anomaly"]],
        on=["datetime_hour", "zone_id"],
        how="left",
    )
    merged["hour_of_day"] = merged["datetime_hour"].dt.hour
    merged["pollution_baseline"] = merged.groupby(["zone_id", "parameter", "hour_of_day"])[
        "pollution"
    ].transform("mean")
    merged["pollution_anomaly"] = merged["pollution"] - merged["pollution_baseline"]
    return merged


def compute_spearman(x: pd.Series, y: pd.Series, min_samples: int) -> float | None:
    paired = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(paired) < min_samples:
        return None
    if paired["x"].nunique() < 2 or paired["y"].nunique() < 2:
        return None
    ranked = paired.rank(method="average")
    return ranked["x"].corr(ranked["y"], method="pearson")


def compute_group_summary(
    group: pd.DataFrame,
    lag_max: int,
    min_samples: int,
    cooccur_quantile: float,
) -> dict:
    group = group.sort_values("datetime_hour").reset_index(drop=True)
    traffic = group["traffic_anomaly"]
    pollution = group["pollution_anomaly"]

    lag_zero_corr = compute_spearman(traffic, pollution, min_samples)

    best_lag_hours: int | None = None
    best_lag_corr: float | None = None
    for lag in range(lag_max + 1):
        shifted_pollution = pollution.shift(-lag)
        lag_corr = compute_spearman(traffic, shifted_pollution, min_samples)
        if lag_corr is None:
            continue
        if best_lag_corr is None or lag_corr > best_lag_corr:
            best_lag_corr = lag_corr
            best_lag_hours = lag

    traffic_threshold = traffic.quantile(cooccur_quantile)
    pollution_threshold = pollution.quantile(cooccur_quantile)
    cooccur_rate = (
        ((traffic >= traffic_threshold) & (pollution >= pollution_threshold)).mean()
        if len(group) > 0
        else 0.0
    )

    return {
        "zone_id": int(group["zone_id"].iloc[0]),
        "borough": group["borough"].iloc[0],
        "zone_name": group["zone_name"].iloc[0],
        "parameter": group["parameter"].iloc[0],
        "sample_count": int(len(group)),
        "mean_pollution": float(group["pollution"].mean()),
        "mean_turnover": float(group["turnover"].mean()),
        "corr_turnover_pollution": lag_zero_corr,
        "best_lag_hours": best_lag_hours,
        "best_lag_corr": best_lag_corr,
        "cooccur_rate": float(cooccur_rate),
    }


def normalize_within_parameter(series: pd.Series) -> pd.Series:
    min_value = series.min()
    max_value = series.max()
    if pd.isna(min_value) or pd.isna(max_value) or min_value == max_value:
        return pd.Series(0.0, index=series.index)
    return (series - min_value) / (max_value - min_value)


def add_hotspot_score(summary_df: pd.DataFrame) -> pd.DataFrame:
    enriched_groups = []
    for parameter, group in summary_df.groupby("parameter", dropna=False):
        group = group.copy()
        group["best_lag_corr_for_score"] = group["best_lag_corr"].fillna(0).clip(lower=0)
        group["norm_best_lag_corr"] = normalize_within_parameter(group["best_lag_corr_for_score"])
        group["norm_cooccur_rate"] = normalize_within_parameter(group["cooccur_rate"].fillna(0))
        group["norm_mean_pollution"] = normalize_within_parameter(group["mean_pollution"])
        group["hotspot_score"] = (
            0.5 * group["norm_best_lag_corr"]
            + 0.3 * group["norm_cooccur_rate"]
            + 0.2 * group["norm_mean_pollution"]
        )
        enriched_groups.append(group)

    result = pd.concat(enriched_groups, ignore_index=True)
    drop_columns = [
        "best_lag_corr_for_score",
        "norm_best_lag_corr",
        "norm_cooccur_rate",
        "norm_mean_pollution",
    ]
    return result.drop(columns=drop_columns)


def build_summary(
    df: pd.DataFrame,
    lag_max: int,
    min_samples: int,
    cooccur_quantile: float,
) -> pd.DataFrame:
    rows = []
    grouped = df.groupby(["zone_id", "parameter"], sort=True, dropna=False)
    for (_, _), group in grouped:
        rows.append(
            compute_group_summary(
                group,
                lag_max=lag_max,
                min_samples=min_samples,
                cooccur_quantile=cooccur_quantile,
            )
        )

    summary_df = pd.DataFrame(rows)
    summary_df = add_hotspot_score(summary_df)
    summary_df = summary_df.sort_values(
        ["parameter", "hotspot_score", "zone_id"],
        ascending=[True, False, True],
    ).reset_index(drop=True)
    return summary_df


def print_summary(input_df: pd.DataFrame, summary_df: pd.DataFrame) -> None:
    print("Correlation and lag analysis completed")
    print(f"  Input rows: {len(input_df):,}")
    print(f"  Output rows: {len(summary_df):,}")
    print(f"  Unique zones: {summary_df['zone_id'].nunique():,}")
    print(f"  Unique parameters: {summary_df['parameter'].nunique():,}")


def main() -> int:
    args = parse_args()
    analysis_df = load_analysis_table(args.input)
    analysis_df = add_anomalies(analysis_df)
    summary_df = build_summary(
        analysis_df,
        lag_max=args.lag_max,
        min_samples=args.min_samples,
        cooccur_quantile=args.cooccur_quantile,
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    summary_df.to_csv(args.output, index=False)
    print_summary(analysis_df, summary_df)
    print(f"  Saved to: {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
