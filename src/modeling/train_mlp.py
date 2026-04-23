#!/usr/bin/env python3
"""
Train a simple PyTorch MLP on the prepared pollution prediction dataset.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

try:
    import torch
    from torch import nn
    from torch.utils.data import DataLoader, TensorDataset
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "PyTorch is not installed in the active environment. "
        "Install it first, for example with `pip install torch`."
    ) from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train an MLP on prepared modeling data.")
    parser.add_argument(
        "--data",
        type=Path,
        default=Path("outputs/model_data/pm25_w6_h1_s3_dataset.parquet"),
        help="Prepared parquet dataset path.",
    )
    parser.add_argument(
        "--metadata",
        type=Path,
        default=Path("outputs/model_data/pm25_w6_h1_s3_metadata.json"),
        help="Metadata JSON path produced by prepare_training_data.py.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/model_runs/pm25_mlp"),
        help="Directory for checkpoints and metrics.",
    )
    parser.add_argument(
        "--target-column",
        type=str,
        default="target_pollution_anomaly",
        help="Regression target column.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=512,
        help="Batch size. Default: 512",
    )
    parser.add_argument(
        "--epochs",
        type=int,
        default=30,
        help="Maximum epochs. Default: 30",
    )
    parser.add_argument(
        "--learning-rate",
        type=float,
        default=1e-3,
        help="Adam learning rate. Default: 1e-3",
    )
    parser.add_argument(
        "--hidden-dims",
        type=int,
        nargs="+",
        default=[128, 64],
        help="Hidden layer sizes. Default: 128 64",
    )
    parser.add_argument(
        "--dropout",
        type=float,
        default=0.15,
        help="Dropout rate. Default: 0.15",
    )
    parser.add_argument(
        "--patience",
        type=int,
        default=5,
        help="Early stopping patience. Default: 5",
    )
    parser.add_argument(
        "--device",
        type=str,
        default="auto",
        choices=["auto", "cpu", "mps", "cuda"],
        help="Training device. Default: auto",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed. Default: 42",
    )
    return parser.parse_args()


def set_seed(seed: int) -> None:
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def resolve_device(device_arg: str) -> torch.device:
    if device_arg == "cpu":
        return torch.device("cpu")
    if device_arg == "cuda":
        return torch.device("cuda" if torch.cuda.is_available() else "cpu")
    if device_arg == "mps":
        return torch.device("mps" if torch.backends.mps.is_available() else "cpu")
    if torch.cuda.is_available():
        return torch.device("cuda")
    if torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


def load_training_inputs(data_path: Path, metadata_path: Path, target_column: str):
    if not data_path.exists():
        raise FileNotFoundError(f"Dataset not found: {data_path}")
    if not metadata_path.exists():
        raise FileNotFoundError(f"Metadata not found: {metadata_path}")

    metadata = json.loads(metadata_path.read_text())
    feature_names = metadata["feature_names"]

    df = pd.read_parquet(data_path)
    required_cols = {"split", target_column, *feature_names}
    missing = required_cols.difference(df.columns)
    if missing:
        raise ValueError(f"Dataset missing required columns: {sorted(missing)}")

    return df, metadata, feature_names


def fit_standardizer(train_x: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    mean = train_x.mean(axis=0)
    std = train_x.std(axis=0)
    std[std < 1e-8] = 1.0
    return mean, std


def apply_standardizer(x: np.ndarray, mean: np.ndarray, std: np.ndarray) -> np.ndarray:
    return (x - mean) / std


def split_arrays(
    df: pd.DataFrame,
    feature_names: list[str],
    target_column: str,
) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    splits: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for split_name in ["train", "val", "test"]:
        split_df = df[df["split"] == split_name]
        if split_df.empty:
            raise ValueError(f"No rows found for split '{split_name}'")
        x = split_df[feature_names].to_numpy(dtype=np.float32)
        y = split_df[target_column].to_numpy(dtype=np.float32).reshape(-1, 1)
        splits[split_name] = (x, y)
    return splits


def make_loader(x: np.ndarray, y: np.ndarray, batch_size: int, shuffle: bool) -> DataLoader:
    dataset = TensorDataset(torch.from_numpy(x), torch.from_numpy(y))
    return DataLoader(dataset, batch_size=batch_size, shuffle=shuffle)


class MLPRegressor(nn.Module):
    def __init__(self, input_dim: int, hidden_dims: list[int], dropout: float) -> None:
        super().__init__()
        layers: list[nn.Module] = []
        prev_dim = input_dim
        for hidden_dim in hidden_dims:
            layers.append(nn.Linear(prev_dim, hidden_dim))
            layers.append(nn.ReLU())
            if dropout > 0:
                layers.append(nn.Dropout(dropout))
            prev_dim = hidden_dim
        layers.append(nn.Linear(prev_dim, 1))
        self.network = nn.Sequential(*layers)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.network(x)


def evaluate_model(model: nn.Module, loader: DataLoader, device: torch.device) -> dict[str, float]:
    model.eval()
    predictions: list[np.ndarray] = []
    targets: list[np.ndarray] = []

    with torch.no_grad():
        for batch_x, batch_y in loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)
            batch_pred = model(batch_x)
            predictions.append(batch_pred.cpu().numpy())
            targets.append(batch_y.cpu().numpy())

    y_pred = np.vstack(predictions).reshape(-1)
    y_true = np.vstack(targets).reshape(-1)

    mae = float(np.mean(np.abs(y_true - y_pred)))
    rmse = float(np.sqrt(np.mean((y_true - y_pred) ** 2)))
    target_var = float(np.var(y_true))
    r2 = float(1.0 - np.mean((y_true - y_pred) ** 2) / target_var) if target_var > 1e-12 else 0.0
    return {"mae": mae, "rmse": rmse, "r2": r2}


def train_model(
    model: nn.Module,
    train_loader: DataLoader,
    val_loader: DataLoader,
    device: torch.device,
    epochs: int,
    learning_rate: float,
    patience: int,
):
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=learning_rate)

    best_state = None
    best_val_rmse = float("inf")
    epochs_without_improvement = 0
    history: list[dict[str, float]] = []

    for epoch in range(1, epochs + 1):
        model.train()
        epoch_losses: list[float] = []

        for batch_x, batch_y in train_loader:
            batch_x = batch_x.to(device)
            batch_y = batch_y.to(device)

            optimizer.zero_grad()
            pred = model(batch_x)
            loss = criterion(pred, batch_y)
            loss.backward()
            optimizer.step()
            epoch_losses.append(float(loss.item()))

        train_loss = float(np.mean(epoch_losses))
        val_metrics = evaluate_model(model, val_loader, device)
        history.append(
            {
                "epoch": epoch,
                "train_mse": train_loss,
                "val_mae": val_metrics["mae"],
                "val_rmse": val_metrics["rmse"],
                "val_r2": val_metrics["r2"],
            }
        )

        print(
            f"Epoch {epoch:02d} | train_mse={train_loss:.4f} "
            f"| val_rmse={val_metrics['rmse']:.4f} | val_mae={val_metrics['mae']:.4f}"
        )

        if val_metrics["rmse"] < best_val_rmse:
            best_val_rmse = val_metrics["rmse"]
            best_state = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            epochs_without_improvement = 0
        else:
            epochs_without_improvement += 1
            if epochs_without_improvement >= patience:
                print(f"Early stopping triggered after {epoch} epochs.")
                break

    if best_state is None:
        raise RuntimeError("Training ended without a valid best checkpoint.")

    model.load_state_dict(best_state)
    return history


def main() -> int:
    args = parse_args()
    set_seed(args.seed)
    device = resolve_device(args.device)

    df, metadata, feature_names = load_training_inputs(
        args.data,
        args.metadata,
        args.target_column,
    )
    split_arrays_map = split_arrays(df, feature_names, args.target_column)
    train_x, train_y = split_arrays_map["train"]
    val_x, val_y = split_arrays_map["val"]
    test_x, test_y = split_arrays_map["test"]

    mean, std = fit_standardizer(train_x)
    train_x = apply_standardizer(train_x, mean, std)
    val_x = apply_standardizer(val_x, mean, std)
    test_x = apply_standardizer(test_x, mean, std)

    train_loader = make_loader(train_x, train_y, args.batch_size, shuffle=True)
    val_loader = make_loader(val_x, val_y, args.batch_size, shuffle=False)
    test_loader = make_loader(test_x, test_y, args.batch_size, shuffle=False)

    model = MLPRegressor(
        input_dim=len(feature_names),
        hidden_dims=args.hidden_dims,
        dropout=args.dropout,
    ).to(device)

    history = train_model(
        model,
        train_loader,
        val_loader,
        device=device,
        epochs=args.epochs,
        learning_rate=args.learning_rate,
        patience=args.patience,
    )

    test_metrics = evaluate_model(model, test_loader, device)
    args.output_dir.mkdir(parents=True, exist_ok=True)

    torch.save(model.state_dict(), args.output_dir / "best_model.pt")
    np.savez(
        args.output_dir / "standardizer.npz",
        mean=mean,
        std=std,
    )

    run_summary = {
        "data_path": str(args.data),
        "metadata_path": str(args.metadata),
        "device": str(device),
        "target_column": args.target_column,
        "feature_count": len(feature_names),
        "hidden_dims": args.hidden_dims,
        "dropout": args.dropout,
        "batch_size": args.batch_size,
        "epochs_requested": args.epochs,
        "epochs_ran": len(history),
        "learning_rate": args.learning_rate,
        "patience": args.patience,
        "dataset_rows": int(len(df)),
        "split_counts": metadata.get("split_counts", {}),
        "test_metrics": test_metrics,
        "history": history,
    }
    (args.output_dir / "metrics.json").write_text(json.dumps(run_summary, indent=2))

    print("Training complete")
    print(f"  Device: {device}")
    print(f"  Test RMSE: {test_metrics['rmse']:.4f}")
    print(f"  Test MAE: {test_metrics['mae']:.4f}")
    print(f"  Test R2: {test_metrics['r2']:.4f}")
    print(f"  Outputs saved to: {args.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
