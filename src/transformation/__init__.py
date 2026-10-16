"""Transformation layer (first installment of the W14 transform phase).

Consumes the raw schema, produces the staging/curated schema. Currently
holds the single midterm deliverable transformation: the AI model release
calendar (see model_releases.py for justification).

Boundary: raw.* is never modified here — transformations are derived,
rerunnable projections of the raw layer.
"""
