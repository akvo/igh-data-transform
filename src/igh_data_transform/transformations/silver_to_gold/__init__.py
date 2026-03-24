"""Silver to Gold layer transformation.

Transforms normalized Silver-layer tables into a star schema (Gold layer)
optimised for OLAP analytics.  The heavy lifting is done by the ETL modules
in ``core/``; this file is a thin facade that preserves the public API
expected by ``cli.py`` and the ``transformations`` package.
"""

from pathlib import Path

from igh_data_transform.transformations.silver_to_gold.core.main import run_etl, setup_logging


def silver_to_gold(silver_db_path: str, gold_db_path: str) -> bool:
    """Transform Silver layer to Gold layer.

    Args:
        silver_db_path: Path to the Silver layer SQLite database.
        gold_db_path: Path to the Gold layer SQLite database to create.

    Returns:
        True if transformation succeeded, False otherwise.
    """
    setup_logging()
    return run_etl(Path(silver_db_path), Path(gold_db_path))
