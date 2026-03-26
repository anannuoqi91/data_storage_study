#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json

import duckdb

from schema_versioning import (
    DATASET_DEFINITIONS,
    ROOT_DATASET_VERSION,
    WRITER_DB_PATH,
    ensure_writer_schema_latest,
    get_dataset_status,
    get_writer_status,
    rollback_writer_schema,
    switch_dataset_version,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage writer.duckdb migrations and active lake dataset versions.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("status", help="Show writer schema status and active dataset versions.")

    migrate_parser = subparsers.add_parser("writer-migrate", help="Apply pending writer.duckdb migrations.")
    migrate_parser.add_argument("--note", default="", help="Optional note stored in migration history.")

    rollback_parser = subparsers.add_parser("writer-rollback", help="Restore writer.duckdb to a snapshotted version.")
    rollback_parser.add_argument("--to", required=True, help="Target writer schema version, for example 0001_init")
    rollback_parser.add_argument("--note", default="", help="Optional note stored in migration history.")

    activate_parser = subparsers.add_parser("lake-activate", help="Activate a lake dataset version for reads and writes.")
    activate_parser.add_argument("--dataset", choices=sorted(DATASET_DEFINITIONS), required=True)
    activate_parser.add_argument("--version", required=True, help=f"Version directory name or {ROOT_DATASET_VERSION}")
    activate_parser.add_argument("--create", action="store_true", help="Create the target version directory if missing.")
    activate_parser.add_argument("--schema-version", default="", help="Optional schema version written into the manifest.")
    activate_parser.add_argument("--note", default="", help="Optional note stored in activation history.")

    lake_rollback_parser = subparsers.add_parser("lake-rollback", help="Switch the active lake dataset pointer back to an existing version.")
    lake_rollback_parser.add_argument("--dataset", choices=sorted(DATASET_DEFINITIONS), required=True)
    lake_rollback_parser.add_argument("--to", required=True, help=f"Existing version directory name or {ROOT_DATASET_VERSION}")
    lake_rollback_parser.add_argument("--note", default="", help="Optional note stored in activation history.")

    return parser.parse_args()


def print_json(payload: dict) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def main() -> None:
    args = parse_args()

    if args.command == "status":
        print_json(
            {
                "writer": get_writer_status(),
                "datasets": {dataset: get_dataset_status(dataset) for dataset in sorted(DATASET_DEFINITIONS)},
            }
        )
        return

    if args.command == "writer-migrate":
        WRITER_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(WRITER_DB_PATH))
        try:
            version = ensure_writer_schema_latest(con, note=args.note or None, db_path=WRITER_DB_PATH)
        finally:
            con.close()
        print_json({"db_path": str(WRITER_DB_PATH), "current_version": version})
        return

    if args.command == "writer-rollback":
        print_json(rollback_writer_schema(target_version=args.to, note=args.note or None, db_path=WRITER_DB_PATH))
        return

    if args.command == "lake-activate":
        print_json(
            switch_dataset_version(
                args.dataset,
                version=args.version,
                create=args.create,
                schema_version=args.schema_version or None,
                note=args.note or None,
                action="activate",
            )
        )
        return

    if args.command == "lake-rollback":
        print_json(
            switch_dataset_version(
                args.dataset,
                version=args.to,
                create=False,
                schema_version=None,
                note=args.note or None,
                action="rollback",
            )
        )
        return

    raise RuntimeError(f"Unhandled command: {args.command}")


if __name__ == "__main__":
    main()
