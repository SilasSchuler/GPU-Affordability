"""Train, compare and register the Buy/Wait model.

Steps:
1. build features + labels from staging.gpu_daily_prices
2. time-based split: train | gap | validation | gap | test
   (the gaps are as long as the label horizon, so no label leaks across splits)
3. fit every candidate on train, tune its decision threshold on validation
4. pick the candidate with the best validation PR-AUC (baselines excluded)
5. refit it on train+validation, report test metrics (honest estimate)
6. quality gate: test PR-AUC must beat the "always Wait" base rate and the
   momentum baseline; then refit on all labelled data and register it in
   MLflow as `gpu-buy-wait` with alias `champion`

Usage:
    uv run python -m src.training.train            # logs to MLflow
    uv run python -m src.training.train --no-mlflow
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

from .features import (
    DROP_THRESHOLD,
    HORIZON_DAYS,
    LABEL,
    NUMERIC_FEATURES,
    TARGET_RETURN,
    build_features,
)
from .models import Candidate, candidates

EXPERIMENT = "gpu-buy-wait"
REGISTERED_MODEL = "gpu-buy-wait"
GAP_DAYS = HORIZON_DAYS + 2
VAL_FRAC = 0.2
TEST_FRAC = 0.2


# --------------------------------------------------------------------- split
def time_split(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dates = np.sort(df["snapshot_date"].unique())
    n = len(dates)
    test_start = dates[int(n * (1 - TEST_FRAC))]
    val_start = dates[int(n * (1 - TEST_FRAC - VAL_FRAC))]
    gap = pd.Timedelta(days=GAP_DAYS)
    d = df["snapshot_date"]
    train = df[d < val_start - gap]
    val = df[(d >= val_start) & (d < test_start - gap)]
    test = df[d >= test_start]
    for name, part in (("train", train), ("val", val), ("test", test)):
        if part.empty or part[LABEL].nunique() < 2:
            raise RuntimeError(f"{name} split is empty or has one class — need more history")
    return train, val, test


# ------------------------------------------------------------------- metrics
def tune_threshold(y: np.ndarray, score: np.ndarray) -> float:
    """Threshold that maximises F1 for the Wait class."""
    qs = np.unique(np.quantile(score, np.linspace(0.02, 0.98, 97)))
    f1s = [f1_score(y, score >= t, zero_division=0) for t in qs]
    return float(qs[int(np.argmax(f1s))])


def evaluate(y: np.ndarray, ret: np.ndarray, score: np.ndarray, thr: float) -> dict:
    pred = score >= thr
    m = {
        "roc_auc": roc_auc_score(y, score),
        "pr_auc": average_precision_score(y, score),
        "f1": f1_score(y, pred, zero_division=0),
        "precision": precision_score(y, pred, zero_division=0),
        "recall": recall_score(y, pred, zero_division=0),
        "balanced_accuracy": balanced_accuracy_score(y, pred),
        "wait_rate_pred": float(pred.mean()),
        "wait_rate_true": float(y.mean()),
    }
    # business view: average price change ~14 days later for each advice
    # (negative after "Wait" = money saved by waiting)
    m["avg_return_when_wait"] = float(ret[pred].mean()) if pred.any() else 0.0
    m["avg_return_when_buy"] = float(ret[~pred].mean()) if (~pred).any() else 0.0
    return {k: round(float(v), 4) for k, v in m.items()}


def _xy(df: pd.DataFrame):
    return df[NUMERIC_FEATURES], df[LABEL], df[TARGET_RETURN]


# ---------------------------------------------------------------------- main
def run(use_mlflow: bool = True) -> dict:
    feats = build_features()
    labelled = feats[feats[LABEL].notna()].copy()
    train, val, test = time_split(labelled)
    data_info = {
        "rows_labelled": len(labelled),
        "series": int(labelled.groupby(["chipset", "condition_group"]).ngroups),
        "train_rows": len(train), "val_rows": len(val), "test_rows": len(test),
        "train_period": f"{train.snapshot_date.min():%F}..{train.snapshot_date.max():%F}",
        "val_period": f"{val.snapshot_date.min():%F}..{val.snapshot_date.max():%F}",
        "test_period": f"{test.snapshot_date.min():%F}..{test.snapshot_date.max():%F}",
        "wait_rate_train": round(float(train[LABEL].mean()), 4),
    }
    print(json.dumps(data_info, indent=2))

    Xtr, ytr, rtr = _xy(train)
    Xva, yva, rva = _xy(val)
    results: list[dict] = []
    fitted: dict[str, tuple[Candidate, float]] = {}
    for cand in candidates():
        cand.fit(Xtr, ytr, rtr)
        s_va = cand.score(Xva)
        thr = tune_threshold(yva.to_numpy(), s_va)
        metrics = evaluate(yva.to_numpy(), rva.to_numpy(), s_va, thr)
        fitted[cand.name] = (cand, thr)
        results.append({"model": cand.name, "kind": cand.kind, "threshold": round(thr, 4),
                        **{f"val_{k}": v for k, v in metrics.items()}})
        print(f"{cand.name:22s} val PR-AUC={metrics['pr_auc']:.3f} "
              f"ROC-AUC={metrics['roc_auc']:.3f} F1={metrics['f1']:.3f}")

    table = pd.DataFrame(results).sort_values("val_pr_auc", ascending=False)
    best_name = table[table["kind"] != "baseline"].iloc[0]["model"]
    thr = fitted[best_name][1]
    print(f"\nselected: {best_name} (threshold {thr:.4f})")

    # honest test estimate: refit on train+val, threshold from validation
    trval = pd.concat([train, val])
    best = next(c for c in candidates() if c.name == best_name)
    best.fit(*_xy(trval))
    Xte, yte, rte = _xy(test)
    test_metrics = evaluate(yte.to_numpy(), rte.to_numpy(), best.score(Xte), thr)
    momentum = next(c for c in candidates() if c.kind == "baseline")
    base_metrics = evaluate(yte.to_numpy(), rte.to_numpy(), momentum.score(Xte),
                            fitted["baseline_momentum"][1])
    print("test:", json.dumps(test_metrics))
    print("test baseline_momentum:", json.dumps(base_metrics))

    passed = (test_metrics["pr_auc"] > test_metrics["wait_rate_true"]
              and test_metrics["pr_auc"] > base_metrics["pr_auc"])
    print(f"quality gate: {'PASSED' if passed else 'FAILED'}")

    # final model: all labelled data, same hyper-parameters and threshold
    final = next(c for c in candidates() if c.name == best_name)
    final.fit(*_xy(labelled))

    summary = {"selected_model": best_name, "threshold": thr, "quality_gate": passed,
               "test": test_metrics, "test_baseline_momentum": base_metrics,
               "data": data_info, "comparison": table.to_dict(orient="records")}
    if use_mlflow:
        _log_mlflow(summary, table, fitted, final, labelled)
    return summary


def _log_mlflow(summary: dict, table: pd.DataFrame,
                fitted: dict[str, tuple[Candidate, float]],
                final: Candidate, labelled: pd.DataFrame) -> None:
    import mlflow
    import mlflow.sklearn
    import skops.io as sio
    from mlflow import MlflowClient

    mlflow.set_tracking_uri(os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5001"))
    mlflow.set_experiment(EXPERIMENT)
    common = {"horizon_days": HORIZON_DAYS, "drop_threshold": DROP_THRESHOLD,
              "gap_days": GAP_DAYS, "n_features": len(NUMERIC_FEATURES)}

    with mlflow.start_run(run_name="train-compare") as parent:
        mlflow.log_params({**common, "selected_model": summary["selected_model"]})
        mlflow.log_params({f"data_{k}": v for k, v in summary["data"].items()})
        for row in table.to_dict(orient="records"):
            cand, _ = fitted[row["model"]]
            with mlflow.start_run(run_name=row["model"], nested=True):
                mlflow.log_params({"kind": row["kind"], "threshold": row["threshold"],
                                   **{k: v for k, v in cand.params.items()}})
                mlflow.log_metrics({k: v for k, v in row.items() if k.startswith("val_")})
        mlflow.log_metrics({f"test_{k}": v for k, v in summary["test"].items()})
        mlflow.log_metrics({f"test_baseline_{k}": v
                            for k, v in summary["test_baseline_momentum"].items()})
        mlflow.log_metric("quality_gate_passed", int(summary["quality_gate"]))

        with tempfile.TemporaryDirectory() as tmp:
            table.to_csv(Path(tmp) / "model_comparison.csv", index=False)
            (Path(tmp) / "summary.json").write_text(json.dumps(summary, indent=2, default=str))
            imp = _importance(final)
            if imp is not None:
                imp.to_csv(Path(tmp) / "feature_importance.csv", index=False)
            mlflow.log_artifacts(tmp, artifact_path="report")

        # MLflow saves sklearn models with skops (no arbitrary pickle code on load).
        # skops only loads types on an allow-list; the types our own freshly
        # trained estimator uses (e.g. numpy.dtype, LightGBM classes) are trusted.
        trusted = sio.get_untrusted_types(data=sio.dumps(final.estimator))
        info = mlflow.sklearn.log_model(
            final.estimator, name="model",
            input_example=labelled[NUMERIC_FEATURES].head(5),
            registered_model_name=REGISTERED_MODEL,
            skops_trusted_types=trusted,
        )
        client = MlflowClient()
        version = info.registered_model_version
        tags = {"model_name": final.name, "kind": final.kind,
                "threshold": str(summary["threshold"]),
                "score": "predict_proba[:,1]" if final.kind == "classifier"
                         else "-predict (predicted 14d return)",
                "features": ",".join(NUMERIC_FEATURES),
                "quality_gate": str(summary["quality_gate"])}
        for k, v in tags.items():
            client.set_model_version_tag(REGISTERED_MODEL, version, k, v)
        if summary["quality_gate"]:
            client.set_registered_model_alias(REGISTERED_MODEL, "champion", version)
            print(f"registered {REGISTERED_MODEL} v{version} -> alias 'champion'")
        else:
            print(f"registered {REGISTERED_MODEL} v{version} (gate failed, champion unchanged)")
        print(f"mlflow run: {parent.info.run_id}")


def _importance(c: Candidate) -> pd.DataFrame | None:
    est = c.estimator.steps[-1][1] if hasattr(c.estimator, "steps") else c.estimator
    vals = getattr(est, "feature_importances_", None)
    if vals is None and hasattr(est, "coef_"):
        vals = np.abs(est.coef_[0])
    if vals is None:
        return None
    return (pd.DataFrame({"feature": NUMERIC_FEATURES, "importance": vals})
            .sort_values("importance", ascending=False))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--no-mlflow", action="store_true", help="skip MLflow logging/registry")
    args = p.parse_args()
    summary = run(use_mlflow=not args.no_mlflow)
    sys.exit(0 if summary["quality_gate"] else 2)


if __name__ == "__main__":
    main()
