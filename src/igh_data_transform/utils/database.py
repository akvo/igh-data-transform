"""SQLite database connection utilities."""

import sqlite3
from types import TracebackType


class DatabaseManager:
    """SQLite database connection manager with context manager support."""

    def __init__(self, db_path: str) -> None:
        """Initialize the database manager.

        Args:
            db_path: Path to the SQLite database file.
        """
        self.db_path = db_path
        self.connection: sqlite3.Connection | None = None

    def __enter__(self) -> "DatabaseManager":
        """Enter the context manager and establish database connection."""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit the context manager and close database connection."""
        if self.connection:
            self.connection.close()
            self.connection = None

    def execute(self, query: str, params: tuple | None = None) -> sqlite3.Cursor:
        """Execute a SQL query.

        Args:
            query: SQL query to execute.
            params: Optional parameters for the query.

        Returns:
            Cursor with query results.

        Raises:
            RuntimeError: If called outside of context manager.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Use context manager.")
        if params:
            return self.connection.execute(query, params)
        return self.connection.execute(query)

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            RuntimeError: If called outside of context manager.
        """
        if self.connection is None:
            raise RuntimeError("Database connection not established. Use context manager.")
        self.connection.commit()
