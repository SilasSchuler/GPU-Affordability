"""Candidate models for the Buy/Wait decision.

Every candidate exposes the same interface: fit(X, y_class, y_return) and
score(X) -> higher means "more likely Wait". That lets classifiers and the
regressor (which predicts the 14-day return) be compared with the same
metrics and the same threshold tuning.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

SEED = 42


@dataclass
class Candidate:
    name: str
    kind: str  # "baseline" | "classifier" | "regressor"
    estimator: Any = None
    params: dict = field(default_factory=dict)

    def fit(self, X: pd.DataFrame, y_class: pd.Series, y_return: pd.Series) -> "Candidate":
        if self.kind == "classifier":
            self.estimator.fit(X, y_class.astype(int))
        elif self.kind == "regressor":
            self.estimator.fit(X, y_return)
        return self

    def score(self, X: pd.DataFrame) -> np.ndarray:
        if self.kind == "classifier":
            return self.estimator.predict_proba(X)[:, 1]
        if self.kind == "regressor":
            return -self.estimator.predict(X)  # bigger predicted drop = more "Wait"
        # momentum baseline: prices that fell last week keep falling
        return -X["ret_7d"].fillna(0).to_numpy()


def candidates() -> list[Candidate]:
    lgbm_common = dict(
        n_estimators=400, learning_rate=0.03, num_leaves=31,
        min_child_samples=50, subsample=0.8, subsample_freq=1,
        colsample_bytree=0.8, reg_lambda=1.0, random_state=SEED, verbose=-1,
    )
    return [
        Candidate("baseline_momentum", "baseline", params={"rule": "-ret_7d"}),
        Candidate(
            "logistic_regression", "classifier",
            Pipeline([
                ("impute", SimpleImputer(strategy="median")),
                ("scale", StandardScaler()),
                ("model", LogisticRegression(class_weight="balanced", max_iter=2000)),
            ]),
            {"class_weight": "balanced", "C": 1.0},
        ),
        Candidate(
            "random_forest", "classifier",
            Pipeline([
                ("impute", SimpleImputer(strategy="median")),
                ("model", RandomForestClassifier(
                    n_estimators=300, min_samples_leaf=20, max_features="sqrt",
                    class_weight="balanced_subsample", n_jobs=-1, random_state=SEED)),
            ]),
            {"n_estimators": 300, "min_samples_leaf": 20},
        ),
        Candidate(
            "lightgbm_classifier", "classifier",
            LGBMClassifier(class_weight="balanced", **lgbm_common),
            {**lgbm_common, "class_weight": "balanced"},
        ),
        Candidate(
            "lightgbm_regressor", "regressor",
            LGBMRegressor(objective="huber", alpha=0.1, **lgbm_common),
            {**lgbm_common, "objective": "huber"},
        ),
    ]
