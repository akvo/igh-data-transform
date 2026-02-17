"""Shared pytest fixtures for igh-data-transform tests."""

import tempfile
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption("--e2e", action="store_true", default=False, help="Run only e2e tests")
    parser.addoption("--all", action="store_true", default=False, help="Run all tests including e2e")


def pytest_collection_modifyitems(config, items):
    run_e2e = config.getoption("--e2e")
    run_all = config.getoption("--all")

    if run_all:
        return  # no filtering

    if run_e2e:
        # Keep only e2e-marked tests
        items[:] = [item for item in items if item.get_closest_marker("e2e")]
    else:
        # Default: exclude e2e-marked tests
        items[:] = [item for item in items if not item.get_closest_marker("e2e")]


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
