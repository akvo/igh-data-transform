"""Tests for database utilities."""

import sqlite3
from pathlib import Path

import pytest

from igh_data_transform.utils.database import DatabaseManager


class TestDatabaseManager:
    """Tests for DatabaseManager class."""

    def test_context_manager_protocol(self, tmp_path: Path) -> None:
        """Test that DatabaseManager implements context manager protocol."""
        db_path = tmp_path / "test.db"
        with DatabaseManager(str(db_path)) as db:
            assert db.connection is not None
        assert db.connection is None

    def test_connection_established_on_enter(self, tmp_path: Path) -> None:
        """Test that connection is established on __enter__."""
        db_path = tmp_path / "test.db"
        manager = DatabaseManager(str(db_path))
        assert manager.connection is None
        with manager:
            assert manager.connection is not None
            assert isinstance(manager.connection, sqlite3.Connection)

    def test_connection_closed_on_exit(self, tmp_path: Path) -> None:
        """Test that connection is closed on __exit__."""
        db_path = tmp_path / "test.db"
        with DatabaseManager(str(db_path)) as db:
            connection = db.connection
            assert connection is not None
        # Connection should be closed and set to None
        assert db.connection is None

    def test_execute_query(self, tmp_path: Path) -> None:
        """Test executing a SQL query."""
        db_path = tmp_path / "test.db"
        with DatabaseManager(str(db_path)) as db:
            cursor = db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            assert cursor is not None
            db.commit()

            # Verify table was created
            cursor = db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='test'")
            result = cursor.fetchone()
            assert result is not None
            assert result["name"] == "test"

    def test_execute_with_params(self, tmp_path: Path) -> None:
        """Test executing a SQL query with parameters."""
        db_path = tmp_path / "test.db"
        with DatabaseManager(str(db_path)) as db:
            db.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
            db.execute("INSERT INTO test (id, name) VALUES (?, ?)", (1, "Alice"))
            db.commit()

            cursor = db.execute("SELECT * FROM test WHERE id = ?", (1,))
            result = cursor.fetchone()
            assert result["id"] == 1
            assert result["name"] == "Alice"

    def test_execute_outside_context_raises(self, tmp_path: Path) -> None:
        """Test that execute raises RuntimeError outside context manager."""
        db_path = tmp_path / "test.db"
        manager = DatabaseManager(str(db_path))
        with pytest.raises(RuntimeError, match="Database connection not established"):
            manager.execute("SELECT 1")

    def test_commit_outside_context_raises(self, tmp_path: Path) -> None:
        """Test that commit raises RuntimeError outside context manager."""
        db_path = tmp_path / "test.db"
        manager = DatabaseManager(str(db_path))
        with pytest.raises(RuntimeError, match="Database connection not established"):
            manager.commit()

    def test_row_factory_is_row(self, tmp_path: Path) -> None:
        """Test that row_factory is set to sqlite3.Row."""
        db_path = tmp_path / "test.db"
        with DatabaseManager(str(db_path)) as db:
            assert db.connection is not None
            assert db.connection.row_factory == sqlite3.Row

    def test_db_path_stored(self, tmp_path: Path) -> None:
        """Test that db_path is stored correctly."""
        db_path = tmp_path / "test.db"
        manager = DatabaseManager(str(db_path))
        assert manager.db_path == str(db_path)

    def test_exception_in_context_closes_connection(self, tmp_path: Path) -> None:
        """Test that connection is closed even when exception occurs."""
        db_path = tmp_path / "test.db"
        manager = DatabaseManager(str(db_path))
        with pytest.raises(ValueError):
            with manager:
                assert manager.connection is not None
                raise ValueError("Test exception")
        assert manager.connection is None
