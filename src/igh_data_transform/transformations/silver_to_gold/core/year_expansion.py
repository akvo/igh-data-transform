"""Expand SCD2 pipeline snapshot rows across intermediate reporting years.

SCD2 records may span multiple reporting years (e.g. valid_from=2023,
valid_to=2025).  The temporal chart groups by ``dt.year``, so a candidate
only appears in the year of its ``date_key``.  This module creates
carry-forward copies for every intermediate reporting year the record spans.
"""

import logging
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)

# Minimum length of a timestamp string to extract a 4-digit year
MIN_YEAR_LENGTH = 4


def _parse_year(timestamp: str | None) -> int | None:
    """Extract the 4-digit year from an ISO timestamp, or None."""
    if not timestamp or len(timestamp) < MIN_YEAR_LENGTH:
        return None
    try:
        return int(timestamp[:MIN_YEAR_LENGTH])
    except ValueError:
        return None


def _collect_reporting_years(
    source_valid_ranges: list[tuple[str | None, str | None]],
) -> set[int]:
    """Collect the set of reporting years from valid_from/valid_to timestamps."""
    years: set[int] = set()
    for vf, vt in source_valid_ranges:
        for ts in (vf, vt):
            year = _parse_year(ts)
            if year is not None:
                years.add(year)
    return years


def _infill_for_row(
    row: dict,
    vf: str | None,
    vt: str | None,
    sorted_years: list[int],
    max_reporting_year: int,
    lookup_date_key: Callable[[str], Any],
) -> list[dict]:
    """Return [original + infill rows] for a single pipeline snapshot row."""
    from_year = _parse_year(vf)
    if from_year is None:
        return [row]

    is_active = vt is None
    to_year = _parse_year(vt) if not is_active else None
    if to_year is None:
        to_year = max_reporting_year + 1

    infill_years = [y for y in sorted_years if from_year < y < to_year]
    if not infill_years:
        return [row]

    # For active records, flip original to inactive; last infill gets active
    if is_active:
        row = {**row, "is_active_flag": 0}

    rows = [row]
    last_infill_year = infill_years[-1] if is_active else None
    for year in infill_years:
        date_key = lookup_date_key(f"{year}-01-01")
        active_flag = 1 if year == last_infill_year else 0
        rows.append({**row, "date_key": date_key, "is_active_flag": active_flag})
    return rows


def expand_pipeline_years(
    transformed: list[dict],
    source_valid_ranges: list[tuple[str | None, str | None]],
    lookup_date_key: Callable[[str], Any],
    reporting_years: set[int] | None = None,
) -> list[dict]:
    """Expand pipeline snapshot rows to cover all intermediate reporting years.

    Args:
        transformed: The fact rows produced by ``transform_fact``.
        source_valid_ranges: Parallel list of ``(valid_from, valid_to)``
            strings from the source rows.
        lookup_date_key: Callable that maps a date string (e.g.
            ``"2024-01-01"``) to the corresponding ``dim_date`` surrogate key.
        reporting_years: Optional explicit set; auto-detected from
            valid_from/valid_to dates when not provided.
    """
    if reporting_years is None:
        reporting_years = _collect_reporting_years(source_valid_ranges)
    if not reporting_years:
        return transformed

    sorted_years = sorted(reporting_years)
    max_reporting_year = sorted_years[-1]

    result: list[dict] = []
    for i, original_row in enumerate(transformed):
        vf, vt = source_valid_ranges[i]
        infill = _infill_for_row(original_row, vf, vt, sorted_years, max_reporting_year, lookup_date_key)
        result.extend(infill)

    logger.info(f"Year expansion: {len(transformed)} → {len(result)} rows (+{len(result) - len(transformed)} infill)")
    return result
