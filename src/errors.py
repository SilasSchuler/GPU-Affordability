"""Named pipeline failures shared by src/ingestion and src/transformation."""

from __future__ import annotations

from datetime import date


class TooFewRowsError(ValueError):
    """A step produced fewer rows than its minimum.

    Subclasses ValueError so callers that already catch ValueError keep working.
    """

    def __init__(self, step: str, count: int, min_rows: int, day: date | None = None) -> None:
        self.step = step
        self.count = count
        self.min_rows = min_rows
        self.day = day
        scope = f" for {day.isoformat()}" if day else ""
        super().__init__(f"{step}{scope}: only {count} rows (< min_rows {min_rows})")
