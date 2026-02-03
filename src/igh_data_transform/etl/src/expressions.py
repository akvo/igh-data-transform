"""
Expression parsing functions for transformer.

Handles evaluation of:
- COALESCE expressions
- CASE WHEN expressions
- LOOKUP expressions
"""

import logging
import re
from typing import Any

from config.phase_sort_order import DEFAULT_SORT_ORDER, PHASE_SORT_ORDER

logger = logging.getLogger(__name__)


def parse_coalesce(expr: str, row: dict) -> Any:
    """
    Parse and evaluate COALESCE(col, 'default') expression.

    Args:
        expr: Expression like "COALESCE(col_name, 'default')"
        row: Source row dict

    Returns:
        Column value or default
    """
    match = re.match(r"COALESCE\((\w+),\s*'([^']*)'\)", expr)
    if not match:
        match = re.match(r"COALESCE\((\w+),\s*(\d+)\)", expr)
        if match:
            col_name, default = match.groups()
            val = row.get(col_name)
            return val if val is not None else int(default)
        # Handle COALESCE(NULL, 'default')
        match = re.match(r"COALESCE\(NULL,\s*'([^']*)'\)", expr)
        if match:
            return match.group(1)
        return None

    col_name, default = match.groups()
    val = row.get(col_name)
    return val if val is not None else default


def parse_case_when(expr: str, row: dict) -> Any:
    """
    Parse and evaluate CASE WHEN expression.

    Args:
        expr: Expression like "CASE WHEN col = val THEN result ELSE other END"
        row: Source row dict

    Returns:
        Result based on condition
    """
    # Simple pattern: CASE WHEN col = val THEN result ELSE other END
    match = re.match(r"CASE\s+WHEN\s+(\w+)\s*=\s*(\d+)\s+THEN\s+(\d+)\s+ELSE\s+(\d+)\s+END", expr, re.IGNORECASE)
    if match:
        col_name, check_val, then_val, else_val = match.groups()
        actual_val = row.get(col_name)
        if actual_val is not None and int(actual_val) == int(check_val):
            return int(then_val)
        return int(else_val)

    logger.warning(f"Could not parse CASE WHEN: {expr}")
    return None


def evaluate_lookup(expr: str, row: dict) -> Any:
    """
    Evaluate LOOKUP: expressions.

    Args:
        expr: Expression like "LOOKUP:PHASE_SORT_ORDER"
        row: Source row dict

    Returns:
        Looked up value or None
    """
    lookup_type = expr[len("LOOKUP:") :]
    if lookup_type == "PHASE_SORT_ORDER":
        phase_name = row.get("vin_name", "")
        return PHASE_SORT_ORDER.get(phase_name, DEFAULT_SORT_ORDER)
    return None
