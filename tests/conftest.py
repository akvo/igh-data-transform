"""Shared pytest fixtures for igh-data-transform tests."""

import tempfile
from pathlib import Path

import pytest


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Create a temporary database path."""
    return tmp_path / "test.db"


@pytest.fixture
def temp_bronze_db(tmp_path: Path) -> Path:
    """Create a temporary Bronze database path."""
    return tmp_path / "bronze.db"


@pytest.fixture
def temp_silver_db(tmp_path: Path) -> Path:
    """Create a temporary Silver database path."""
    return tmp_path / "silver.db"


@pytest.fixture
def temp_dir() -> Path:
    """Create a temporary directory that is cleaned up after the test."""
    with tempfile.TemporaryDirectory() as tmpdir:
        yield Path(tmpdir)
