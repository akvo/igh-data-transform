"""
R&D phase configuration: sort order, aliases, and helper functions.

Provides a logical ordering for stacked bar charts and pipelines,
from earliest stages (discovery) to latest (post-marketing).

Phases are matched by their vin_name from vin_rdstages.
"""

import logging

logger = logging.getLogger(__name__)

PHASE_SORT_ORDER = {
    "Discovery & Preclinical": 10,
    "Early development": 20,
    "Phase I": 30,
    "Phase II": 40,
    "Phase III": 50,
    "Late development": 60,
    "Regulatory filing": 70,
    "Approved": 80,
    "Post-marketing surveillance": 90,
    "Human safety & efficacy": 95,
    # Special cases
    "Not applicable": 999,
    "Unclear": 998,
}

# Default sort order for phases not in the lookup
DEFAULT_SORT_ORDER = 500

# Normalize non-standard phase names to their canonical dim_phase equivalents
PHASE_ALIASES = {
    "Approved product": "Approved",
    "Discovery and Preclinical": "Discovery & Preclinical",
    "Discovery and preclinical": "Discovery & Preclinical",
    "N/A": "Not applicable",
}


def collect_referenced_phase_names(extractor) -> set[str]:
    """Scan vin_candidates.new_currentrdstage to find all referenced phase names."""
    referenced: set[str] = set()
    for row in extractor.extract_table("vin_candidates", ["new_currentrdstage"]):
        raw = row.get("new_currentrdstage")
        if not raw:
            continue
        phase = raw.split(" - ")[0] if " - " in raw else raw
        phase = PHASE_ALIASES.get(phase, phase)
        referenced.add(phase)
    return referenced


def inject_synthetic_phases(
    transformed: list[dict],
    referenced_phases: set[str] | None = None,
) -> list[dict]:
    """Add phases from PHASE_SORT_ORDER that aren't already in the source data."""
    existing_names = {row["phase_name"] for row in transformed if row.get("phase_name")}
    for phase_name, sort_order in PHASE_SORT_ORDER.items():
        if phase_name not in existing_names:
            if referenced_phases is not None and phase_name not in referenced_phases:
                continue
            transformed.append({
                "vin_rdstageid": None,
                "phase_name": phase_name,
                "sort_order": sort_order,
            })
            logger.info(f"Injected synthetic phase: {phase_name}")
    return transformed
