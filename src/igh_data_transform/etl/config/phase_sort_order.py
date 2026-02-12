"""
Manual sort order for R&D phases.

This provides a logical ordering for stacked bar charts and pipelines,
from earliest stages (discovery) to latest (post-marketing).

Phases are matched by their vin_name from vin_rdstages.
"""

PHASE_SORT_ORDER = {
    # Discovery/Early stages
    "Discovery": 10,
    "Discovery and preclinical": 15,
    "Primary and secondary screening and optimisation": 20,
    "Preclinical": 25,
    # Early Development
    "Development": 30,
    "Early development": 35,
    "Early development (concept and research)": 36,
    "Early development (feasibility and planning)": 37,
    # Clinical Phases
    "Phase I": 40,
    "Phase II": 50,
    "Phase III": 60,
    "Clinical evaluation": 65,
    # Late Development
    "Late development": 70,
    "Late development (design and development)": 72,
    "Late development (clinical validation and launch readiness)": 75,
    # Regulatory/Approval
    "Regulatory filing": 80,
    "PQ listing and regulatory approval": 85,
    # Approved
    "Approved": 88,
    # Post-approval
    "Phase IV": 90,
    "Post-marketing surveillance": 92,
    "Post-marketing human safety/efficacy studies (without prior clinical studies)": 93,
    "Human safety & efficacy": 94,
    "Operational research for diagnostics": 95,
    # Special cases
    "Not applicable": 999,
    "Unclear": 998,
}

# Default sort order for phases not in the lookup
DEFAULT_SORT_ORDER = 500
