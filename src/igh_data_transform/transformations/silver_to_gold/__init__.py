"""Silver to Gold layer transformation.

Transforms Silver-layer normalized tables into an OLAP star schema
(dimensions, facts, and bridges) suitable for analytics.
"""

from pathlib import Path

from igh_data_transform.transformations.silver_to_gold.core.main import run_etl


def silver_to_gold(silver_db_path: str, gold_db_path: str | None = None) -> bool:
    """Transform Silver layer to Gold layer (star schema).

    Args:
        silver_db_path: Path to the Silver layer SQLite database.
        gold_db_path: Path to the Gold layer SQLite database output.
            Defaults to ``star_schema.db`` next to the silver DB.

    Returns:
        True if transformation succeeded, False otherwise.
    """
    source = Path(silver_db_path)
    if gold_db_path is None:
        output = source.parent / "star_schema.db"
    else:
        output = Path(gold_db_path)

    return run_etl(source, output)
