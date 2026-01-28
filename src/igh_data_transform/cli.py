"""CLI entry point for igh-data-transform."""

import argparse
import sys


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser for the CLI."""
    parser = argparse.ArgumentParser(
        prog="igh-transform",
        description="IGH Data Transform - Bronze to Silver to Gold pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # bronze-to-silver command
    bronze_parser = subparsers.add_parser(
        "bronze-to-silver",
        help="Transform Bronze layer to Silver layer",
    )
    bronze_parser.add_argument(
        "--bronze-db",
        required=True,
        help="Path to Bronze layer SQLite database",
    )
    bronze_parser.add_argument(
        "--silver-db",
        required=True,
        help="Path to Silver layer SQLite database",
    )

    # silver-to-gold command
    silver_parser = subparsers.add_parser(
        "silver-to-gold",
        help="Transform Silver layer to Gold layer",
    )
    silver_parser.add_argument(
        "--silver-db",
        required=True,
        help="Path to Silver layer SQLite database",
    )

    # validate command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate data quality in database",
    )
    validate_parser.add_argument(
        "--db",
        required=True,
        help="Path to SQLite database to validate",
    )

    return parser


def main() -> int:
    """Main entry point for the CLI."""
    parser = create_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "bronze-to-silver":
        from igh_data_transform.transformations.bronze_to_silver import bronze_to_silver

        print(f"Transforming Bronze to Silver: {args.bronze_db} -> {args.silver_db}")
        success = bronze_to_silver(args.bronze_db, args.silver_db)
        return 0 if success else 1

    if args.command == "silver-to-gold":
        from igh_data_transform.transformations.silver_to_gold import silver_to_gold

        print(f"Transforming Silver to Gold: {args.silver_db}")
        success = silver_to_gold(args.silver_db)
        return 0 if success else 1

    if args.command == "validate":
        from igh_data_transform.utils.validators import validate_data_quality

        print(f"Validating data quality: {args.db}")
        errors = validate_data_quality(args.db)
        if errors:
            print(f"Found {len(errors)} validation errors")
            return 1
        print("Validation passed")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
