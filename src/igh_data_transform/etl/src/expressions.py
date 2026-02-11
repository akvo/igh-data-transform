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


def _eval_single_branch_case_when(expr: str, row: dict) -> tuple[bool, Any]:
    """Try to evaluate single-branch CASE WHEN (integer or string results).

    Returns (matched, value) where matched=True if the pattern was recognized.
    """
    # Pattern 1: Integer results - CASE WHEN col = val THEN 1 ELSE 0 END
    match = re.match(r"CASE\s+WHEN\s+(\w+)\s*=\s*(\d+)\s+THEN\s+(\d+)\s+ELSE\s+(\d+)\s+END", expr, re.IGNORECASE)
    if match:
        col_name, check_val, then_val, else_val = match.groups()
        actual_val = row.get(col_name)
        condition = actual_val is not None and int(actual_val) == int(check_val)
        return True, int(then_val) if condition else int(else_val)

    # Pattern 2: String results - CASE WHEN col = val THEN 'str' ELSE 'str' END
    match = re.match(
        r"CASE\s+WHEN\s+(\w+)\s*=\s*(\d+)\s+THEN\s+'([^']+)'\s+ELSE\s+'([^']+)'\s+END", expr, re.IGNORECASE
    )
    if match:
        col_name, check_val, then_val, else_val = match.groups()
        actual_val = row.get(col_name)
        condition = actual_val is not None and int(actual_val) == int(check_val)
        return True, then_val if condition else else_val

    return False, None


def parse_case_when(expr: str, row: dict) -> Any:
    """
    Parse and evaluate CASE WHEN expression.

    Args:
        expr: Expression like "CASE WHEN col = val THEN result ELSE other END"
        row: Source row dict

    Returns:
        Result based on condition
    """
    matched, value = _eval_single_branch_case_when(expr, row)
    if matched:
        return value

    # Pattern 3: Multi-branch CASE WHEN with string comparison values
    # e.g. CASE WHEN col = 'guid1' THEN 'Label1' WHEN col = 'guid2' THEN 'Label2' ELSE 'Other' END
    branches = re.findall(
        r"WHEN\s+(\w+)\s*=\s*'([^']+)'\s+THEN\s+'([^']+)'",
        expr,
        re.IGNORECASE,
    )
    if branches:
        for col_name, check_val, then_val in branches:
            actual_val = row.get(col_name)
            if actual_val is not None and str(actual_val) == check_val:
                return then_val
        else_match = re.search(r"ELSE\s+'([^']+)'\s+END", expr, re.IGNORECASE)
        return else_match.group(1) if else_match else None

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
