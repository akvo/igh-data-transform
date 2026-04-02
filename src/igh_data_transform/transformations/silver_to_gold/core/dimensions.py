"""Standalone dimension-transformation helpers extracted from Transformer.

These functions don't depend on Transformer state, so they live here to
keep the main transformer module under the pylint line-count limit.
"""

import logging
from datetime import date, timedelta

from igh_data_transform.transformations.silver_to_gold.config.phase_sort_order import (
    collect_referenced_phase_names,
    inject_synthetic_phases,
)

logger = logging.getLogger(__name__)


def generate_date_dimension(special: dict) -> list[dict]:
    """Generate date dimension programmatically."""
    start_year = special.get("start_year", 2015)
    end_year = special.get("end_year", 2030)

    rows = []
    current = date(start_year, 1, 1)
    end = date(end_year + 1, 1, 1)

    while current < end:
        rows.append({
            "full_date": current.isoformat(),
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
        })
        current += timedelta(days=1)

    logger.info(f"Generated {len(rows)} rows for dim_date ({start_year}-{end_year})")
    return rows


def postprocess_dim_phase(transformed: list[dict], extractor: object) -> list[dict]:
    """Deduplicate, filter unreferenced, and inject synthetic phases."""
    referenced = collect_referenced_phase_names(extractor)
    # Deduplicate by phase_name (keep first occurrence)
    seen_names: set[str] = set()
    deduped: list[dict] = []
    for row in transformed:
        name = row.get("phase_name")
        if name and name not in seen_names:
            seen_names.add(name)
            deduped.append(row)
    # Filter to only phases referenced by candidates
    filtered = [r for r in deduped if r.get("phase_name") in referenced]
    return inject_synthetic_phases(filtered, referenced)
