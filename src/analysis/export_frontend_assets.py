#!/usr/bin/env python3
"""Export frontend-friendly assets from analysis outputs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import geopandas as gpd
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export frontend data assets.")
    parser.add_argument(
        "--summary-input",
        required=True,
        help="Path to zone_analysis_summary.csv",
    )
    parser.add_argument(
        "--timeseries-input",
        required=True,
        help="Path to zone_analysis_timeseries.csv",
    )
    parser.add_argument(
        "--zones-input",
        required=True,
        help="Path to taxi_zones.shp",
    )
    parser.add_argument(
        "--output-dir",
        required=True,
        help="Frontend public data directory",
    )
    parser.add_argument(
        "--simplify-tolerance",
        type=float,
        default=0.00008,
        help="GeoJSON simplification tolerance in EPSG:4326 units.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def compact_float(value: float | int | str | None, digits: int = 4):
    if value is None or pd.isna(value):
        return None
    if isinstance(value, (int, str)):
        return value
    return round(float(value), digits)


def export_zones_geojson(zones_input: Path, output_dir: Path, tolerance: float) -> dict:
    gdf = gpd.read_file(zones_input)[["LocationID", "zone", "borough", "geometry"]]
    gdf = gdf.rename(
        columns={
            "LocationID": "zone_id",
            "zone": "zone_name",
            "borough": "borough",
        }
    )
    gdf = gdf.to_crs("EPSG:4326")
    gdf["geometry"] = gdf["geometry"].simplify(tolerance, preserve_topology=True)
    output_path = output_dir / "zones.geojson"
    gdf.to_file(output_path, driver="GeoJSON")

    return {
        "zone_count": int(len(gdf)),
        "bounds": list(gdf.total_bounds),
    }


def export_summary(summary_input: Path, output_dir: Path) -> dict:
    df = pd.read_csv(summary_input)
    df = df.sort_values(["parameter", "hotspot_score"], ascending=[True, False])

    pollutants = sorted(df["parameter"].dropna().unique().tolist())
    metrics = [
        {
            "id": "mean_turnover",
            "label": "Mean Turnover",
            "description": "Average hourly pickup and dropoff activity.",
            "scale": "sequential",
        },
        {
            "id": "mean_pollution",
            "label": "Mean Pollution",
            "description": "Average interpolated pollution concentration.",
            "scale": "sequential",
        },
        {
            "id": "best_lag_corr",
            "label": "Best Lag Correlation",
            "description": "Strongest positive lagged traffic-pollution relationship.",
            "scale": "diverging",
        },
        {
            "id": "hotspot_score",
            "label": "Hotspot Score",
            "description": "Composite score from lag strength, co-occurrence, and mean pollution.",
            "scale": "sequential",
        },
    ]

    summary_records = []
    for row in df.itertuples(index=False):
        summary_records.append(
            {
                "zone_id": int(row.zone_id),
                "borough": row.borough,
                "zone_name": row.zone_name,
                "parameter": row.parameter,
                "sample_count": int(row.sample_count),
                "mean_pollution": compact_float(row.mean_pollution),
                "mean_turnover": compact_float(row.mean_turnover),
                "corr_turnover_pollution": compact_float(row.corr_turnover_pollution),
                "best_lag_hours": int(row.best_lag_hours) if pd.notna(row.best_lag_hours) else None,
                "best_lag_corr": compact_float(row.best_lag_corr),
                "cooccur_rate": compact_float(row.cooccur_rate),
                "hotspot_score": compact_float(row.hotspot_score),
            }
        )

    payload = {
        "pollutants": pollutants,
        "metrics": metrics,
        "records": summary_records,
    }
    output_path = output_dir / "summary.json"
    output_path.write_text(json.dumps(payload, separators=(",", ":")))

    return {
        "pollutant_count": len(pollutants),
        "summary_record_count": len(summary_records),
    }


def export_timeseries(timeseries_input: Path, output_dir: Path) -> dict:
    df = pd.read_csv(
        timeseries_input,
        usecols=[
            "datetime_hour",
            "zone_id",
            "borough",
            "zone_name",
            "parameter",
            "pollution",
            "pickup_count",
            "dropoff_count",
            "turnover",
            "net_flow",
        ],
    )
    timeseries_dir = output_dir / "timeseries"
    ensure_dir(timeseries_dir)

    pollutants = sorted(df["parameter"].dropna().unique().tolist())
    stats = {}
    for parameter in pollutants:
        subset = df[df["parameter"] == parameter].sort_values(["zone_id", "datetime_hour"])
        parameter_dir = timeseries_dir / parameter
        ensure_dir(parameter_dir)

        zone_count = 0
        row_count = 0
        for zone_id, zone_df in subset.groupby("zone_id", sort=True):
            zone_count += 1
            row_count += len(zone_df)
            zone_name = zone_df["zone_name"].iloc[0]
            borough = zone_df["borough"].iloc[0]
            records = []
            for row in zone_df.itertuples(index=False):
                records.append(
                    {
                        "datetime_hour": row.datetime_hour,
                        "pollution": compact_float(row.pollution),
                        "pickup_count": int(row.pickup_count),
                        "dropoff_count": int(row.dropoff_count),
                        "turnover": compact_float(row.turnover),
                        "net_flow": compact_float(row.net_flow),
                    }
                )

            output_path = parameter_dir / f"{int(zone_id)}.json"
            output_path.write_text(
                json.dumps(
                    {
                        "parameter": parameter,
                        "zone_id": int(zone_id),
                        "zone_name": zone_name,
                        "borough": borough,
                        "records": records,
                    },
                    separators=(",", ":"),
                )
            )
        stats[parameter] = {
            "row_count": row_count,
            "zone_file_count": zone_count,
        }

    return {
        "counts_by_parameter": stats,
        "datetime_min": str(df["datetime_hour"].min()),
        "datetime_max": str(df["datetime_hour"].max()),
    }


def export_meta(output_dir: Path, zone_meta: dict, summary_meta: dict, timeseries_meta: dict) -> None:
    meta = {
        "title": "NYC Taxi Mobility and Air Quality Explorer",
        "description": "Interactive dashboard for transport intensity, pollution, lag correlation, and hotspot analysis.",
        "zone_count": zone_meta["zone_count"],
        "bounds": zone_meta["bounds"],
        "pollutant_count": summary_meta["pollutant_count"],
        "summary_record_count": summary_meta["summary_record_count"],
        "timeseries_record_count_by_parameter": timeseries_meta["counts_by_parameter"],
        "timeseries_layout": "per_zone_per_parameter",
        "analysis_window": {
            "start": timeseries_meta["datetime_min"],
            "end": timeseries_meta["datetime_max"],
        },
    }
    (output_dir / "meta.json").write_text(json.dumps(meta, separators=(",", ":")))


def main() -> None:
    args = parse_args()
    output_dir = Path(args.output_dir)
    ensure_dir(output_dir)

    zone_meta = export_zones_geojson(Path(args.zones_input), output_dir, args.simplify_tolerance)
    summary_meta = export_summary(Path(args.summary_input), output_dir)
    timeseries_meta = export_timeseries(Path(args.timeseries_input), output_dir)
    export_meta(output_dir, zone_meta, summary_meta, timeseries_meta)

    print(f"Exported zones.geojson to {output_dir}")
    print(f"Summary records: {summary_meta['summary_record_count']}")
    print(f"Pollutants: {summary_meta['pollutant_count']}")
    print("Timeseries rows by parameter:")
    for parameter, count in timeseries_meta.items():
        print(f"  {parameter}: {count}")


if __name__ == "__main__":
    main()
