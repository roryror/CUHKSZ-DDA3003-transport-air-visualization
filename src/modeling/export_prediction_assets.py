#!/usr/bin/env python3
"""
Export trained prediction results into frontend-friendly static JSON assets.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import torch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.modeling.train_mlp import MLPRegressor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export prediction assets for the frontend.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("outputs/model_data/pm25_w6_h1_s3_dataset.parquet"),
        help="Prepared modeling parquet dataset.",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=Path("outputs/model_data/pm25_w6_h1_s3_metadata.json"),
        help="Prepared dataset metadata JSON.",
    )
    parser.add_argument(
        "--run-dir",
        type=Path,
        default=Path("outputs/model_runs/pm25_mlp"),
        help="Directory containing best_model.pt, standardizer.npz, and metrics.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("frontend/public/data/predictions/pm25"),
        help="Frontend output directory.",
    )
    parser.add_argument(
        "--target-column",
        type=str,
        default="target_pollution_anomaly",
        help="Target column in the prepared dataset.",
    )
    return parser.parse_args()


def load_inputs(args: argparse.Namespace):
    metadata = json.loads(args.metadata.read_text())
    metrics = json.loads((args.run_dir / "metrics.json").read_text())
    feature_names = metadata["feature_names"]

    df = pd.read_parquet(args.data)
    df = df[df["split"] == "test"].copy()
    if df.empty:
        raise ValueError("No test split rows found in prepared dataset.")

    scaler = np.load(args.run_dir / "standardizer.npz")
    mean = scaler["mean"]
    std = scaler["std"]

    model = MLPRegressor(
        input_dim=len(feature_names),
        hidden_dims=metrics["hidden_dims"],
        dropout=float(metrics["dropout"]),
    )
    state_dict = torch.load(args.run_dir / "best_model.pt", map_location="cpu")
    model.load_state_dict(state_dict)
    model.eval()

    return df, metadata, metrics, feature_names, mean, std, model


def run_inference(
    df: pd.DataFrame,
    feature_names: list[str],
    mean: np.ndarray,
    std: np.ndarray,
    model: MLPRegressor,
) -> pd.DataFrame:
    x = df[feature_names].to_numpy(dtype=np.float32)
    x = (x - mean) / std
    with torch.no_grad():
        pred = model(torch.from_numpy(x)).cpu().numpy().reshape(-1)

    result = df[
        [
            "zone_id",
            "zone_name",
            "borough",
            "target_datetime",
            "target_pollution",
            "target_pollution_anomaly",
            "split",
        ]
    ].copy()
    result["predicted_pollution_anomaly"] = pred
    result["absolute_error"] = np.abs(
        result["target_pollution_anomaly"] - result["predicted_pollution_anomaly"]
    )
    result["squared_error"] = (
        result["target_pollution_anomaly"] - result["predicted_pollution_anomaly"]
    ) ** 2
    return result.sort_values(["zone_id", "target_datetime"]).reset_index(drop=True)


def build_zone_summary(pred_df: pd.DataFrame) -> list[dict]:
    summary_rows = []
    for (zone_id, zone_name, borough), zone_df in pred_df.groupby(
        ["zone_id", "zone_name", "borough"],
        sort=True,
        observed=True,
    ):
        y_true = zone_df["target_pollution_anomaly"].to_numpy(dtype=float)
        y_pred = zone_df["predicted_pollution_anomaly"].to_numpy(dtype=float)
        mae = float(np.mean(np.abs(y_true - y_pred)))
        rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
        var = float(np.var(y_true))
        r2 = float(1.0 - np.mean((y_true - y_pred) ** 2) / var) if var > 1e-12 else 0.0
        summary_rows.append(
            {
                "zone_id": int(zone_id),
                "zone_name": zone_name,
                "borough": borough,
                "sample_count": int(len(zone_df)),
                "mae": mae,
                "rmse": rmse,
                "r2": r2,
            }
        )
    return summary_rows


def write_assets(
    pred_df: pd.DataFrame,
    zone_summary: list[dict],
    metadata: dict,
    metrics: dict,
    output_dir: Path,
) -> None:
    zones_dir = output_dir / "zones"
    zones_dir.mkdir(parents=True, exist_ok=True)

    global_meta = {
        "module": "Auxiliary Prediction Module",
        "pollutant": metadata["pollutant"],
        "target_column": metadata["target_column"],
        "window_hours": metadata["window_hours"],
        "horizon_hours": metadata["horizon_hours"],
        "step_hours": metadata["step_hours"],
        "sample_count": int(metrics["dataset_rows"]),
        "test_sample_count": int(len(pred_df)),
        "zone_count": int(pred_df["zone_id"].nunique()),
        "test_metrics": metrics["test_metrics"],
        "test_window": {
            "start": pred_df["target_datetime"].min(),
            "end": pred_df["target_datetime"].max(),
        },
    }
    (output_dir / "meta.json").write_text(json.dumps(global_meta, indent=2))
    (output_dir / "summary.json").write_text(json.dumps({"records": zone_summary}, indent=2))

    for zone_id, zone_df in pred_df.groupby("zone_id", sort=True):
        payload = {
            "zone_id": int(zone_id),
            "zone_name": zone_df["zone_name"].iloc[0],
            "borough": zone_df["borough"].iloc[0],
            "records": zone_df[
                [
                    "target_datetime",
                    "target_pollution",
                    "target_pollution_anomaly",
                    "predicted_pollution_anomaly",
                    "absolute_error",
                ]
            ].to_dict(orient="records"),
        }
        (zones_dir / f"{int(zone_id)}.json").write_text(json.dumps(payload, indent=2))


def main() -> int:
    args = parse_args()
    df, metadata, metrics, feature_names, mean, std, model = load_inputs(args)
    pred_df = run_inference(df, feature_names, mean, std, model)
    zone_summary = build_zone_summary(pred_df)
    write_assets(pred_df, zone_summary, metadata, metrics, args.output_dir)

    print("Exported prediction assets")
    print(f"  Test rows: {len(pred_df):,}")
    print(f"  Zones: {pred_df['zone_id'].nunique():,}")
    print(f"  Output dir: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
