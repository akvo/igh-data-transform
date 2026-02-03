"""Tests for CLI commands."""

import subprocess
import sys
from pathlib import Path

import pytest

from igh_data_transform.cli import create_parser, main


class TestCLIParser:
    """Tests for CLI argument parsing."""

    def test_create_parser_returns_parser(self) -> None:
        """Test that create_parser returns an ArgumentParser."""
        parser = create_parser()
        assert parser is not None
        assert parser.prog == "igh-transform"

    def test_help_flag(self) -> None:
        """Test that --help works without error."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--help"])
        assert exc_info.value.code == 0

    def test_bronze_to_silver_help(self) -> None:
        """Test that bronze-to-silver --help works."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["bronze-to-silver", "--help"])
        assert exc_info.value.code == 0

    def test_silver_to_gold_help(self) -> None:
        """Test that silver-to-gold --help works."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["silver-to-gold", "--help"])
        assert exc_info.value.code == 0



class TestBronzeToSilverCommand:
    """Tests for bronze-to-silver command."""

    def test_bronze_to_silver_parses_args(self) -> None:
        """Test that bronze-to-silver parses arguments correctly."""
        parser = create_parser()
        args = parser.parse_args(
            ["bronze-to-silver", "--bronze-db", "/path/bronze.db", "--silver-db", "/path/silver.db"]
        )
        assert args.command == "bronze-to-silver"
        assert args.bronze_db == "/path/bronze.db"
        assert args.silver_db == "/path/silver.db"

    def test_bronze_to_silver_missing_bronze_db(self) -> None:
        """Test that bronze-to-silver requires --bronze-db."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["bronze-to-silver", "--silver-db", "/path/silver.db"])
        assert exc_info.value.code == 2

    def test_bronze_to_silver_missing_silver_db(self) -> None:
        """Test that bronze-to-silver requires --silver-db."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["bronze-to-silver", "--bronze-db", "/path/bronze.db"])
        assert exc_info.value.code == 2

    def test_bronze_to_silver_command_execution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that bronze-to-silver command executes successfully."""
        bronze_db = tmp_path / "bronze.db"
        silver_db = tmp_path / "silver.db"
        monkeypatch.setattr(
            sys,
            "argv",
            ["igh-transform", "bronze-to-silver", "--bronze-db", str(bronze_db), "--silver-db", str(silver_db)],
        )
        result = main()
        assert result == 0


class TestSilverToGoldCommand:
    """Tests for silver-to-gold command."""

    def test_silver_to_gold_parses_args(self) -> None:
        """Test that silver-to-gold parses arguments correctly."""
        parser = create_parser()
        args = parser.parse_args(["silver-to-gold", "--silver-db", "/path/silver.db"])
        assert args.command == "silver-to-gold"
        assert args.silver_db == "/path/silver.db"

    def test_silver_to_gold_missing_silver_db(self) -> None:
        """Test that silver-to-gold requires --silver-db."""
        parser = create_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["silver-to-gold"])
        assert exc_info.value.code == 2

    def test_silver_to_gold_command_execution(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that silver-to-gold command executes successfully."""
        silver_db = tmp_path / "silver.db"
        monkeypatch.setattr(
            sys,
            "argv",
            ["igh-transform", "silver-to-gold", "--silver-db", str(silver_db)],
        )
        result = main()
        assert result == 0


class TestMainFunction:
    """Tests for main entry point."""

    def test_no_command_shows_help(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that running without a command shows help and returns 0."""
        monkeypatch.setattr(sys, "argv", ["igh-transform"])
        result = main()
        assert result == 0


class TestCLIIntegration:
    """Integration tests for CLI using subprocess."""

    def test_cli_help_via_subprocess(self) -> None:
        """Test CLI help via subprocess."""
        result = subprocess.run(
            [sys.executable, "-m", "igh_data_transform.cli", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "bronze-to-silver" in result.stdout
        assert "silver-to-gold" in result.stdout
