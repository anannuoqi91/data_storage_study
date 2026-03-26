from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DUCKDB_DIR = DATA_DIR / "duckdb"
WRITER_DB_PATH = DUCKDB_DIR / "writer.duckdb"
LAKE_ROOT = DATA_DIR / "lake"
BACKUP_ROOT = DATA_DIR / "backups" / "schema"
WRITER_MIGRATIONS_ROOT = BASE_DIR / "schema" / "duckdb"

META_SCHEMA = "traj_meta"
WRITER_SCOPE = "writer.duckdb"
ROOT_DATASET_VERSION = "__root__"
CURRENT_FILE_NAME = "_current.json"
MANIFEST_FILE_NAME = "_manifest.json"
HISTORY_FILE_NAME = "_history.jsonl"

DATASET_DEFINITIONS: dict[str, dict[str, Any]] = {
    "box_info": {
        "default_schema_version": "box_info.v2",
        "partitioning": ["date", "hour"],
        "compression": "zstd",
    },
    "events": {
        "default_schema_version": "events.v1",
        "partitioning": ["event_type", "date", "hour"],
        "compression": "zstd",
    },
}

WRITER_METADATA_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {META_SCHEMA};

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.schema_version (
    scope VARCHAR PRIMARY KEY,
    current_version VARCHAR NOT NULL,
    release_id VARCHAR,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS {META_SCHEMA}.schema_migration_history (
    history_id VARCHAR PRIMARY KEY,
    scope VARCHAR NOT NULL,
    direction VARCHAR NOT NULL,
    from_version VARCHAR,
    to_version VARCHAR NOT NULL,
    release_id VARCHAR NOT NULL,
    checksum VARCHAR,
    git_commit VARCHAR,
    snapshot_path VARCHAR,
    status VARCHAR NOT NULL,
    note VARCHAR,
    started_at TIMESTAMP NOT NULL DEFAULT current_timestamp,
    finished_at TIMESTAMP
);
"""

EXPECTED_0001_TABLES = (
    ("od", "box_info_ingest"),
    ("od", "box_info_staging"),
    ("od", "flush_checkpoint"),
    ("od", "flush_log"),
    ("od", "event_materialization_checkpoint"),
)
EXPECTED_0001_TABLE_SET = set(EXPECTED_0001_TABLES)


@dataclass(frozen=True)
class MigrationSpec:
    version: str
    manifest: dict[str, Any]
    up_sql_path: Path
    down_sql_path: Path | None
    checksum: str


def utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat()


def make_release_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(tz=UTC).strftime('%Y%m%dT%H%M%SZ')}"


def read_json_file(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json_file(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def current_git_commit() -> str | None:
    try:
        completed = subprocess.run(
            ["git", "-C", str(BASE_DIR.parent), "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None
    return completed.stdout.strip() or None


def dataset_root(dataset: str) -> Path:
    if dataset not in DATASET_DEFINITIONS:
        raise ValueError(f"Unsupported dataset: {dataset}")
    return LAKE_ROOT / dataset


def dataset_current_path(dataset: str) -> Path:
    return dataset_root(dataset) / CURRENT_FILE_NAME


def dataset_history_path(dataset: str) -> Path:
    return dataset_root(dataset) / HISTORY_FILE_NAME


def dataset_manifest_path_for_resolved_path(resolved_path: Path) -> Path:
    return resolved_path / MANIFEST_FILE_NAME


def list_dataset_versions(dataset: str) -> list[str]:
    root = dataset_root(dataset)
    if not root.exists():
        return []
    versions = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        if child.name.startswith(".") or "=" in child.name:
            continue
        versions.append(child.name)
    return sorted(versions)


def dataset_has_root_level_data(dataset: str) -> bool:
    root = dataset_root(dataset)
    if not root.exists():
        return False
    for child in root.iterdir():
        if child.name in {CURRENT_FILE_NAME, MANIFEST_FILE_NAME, HISTORY_FILE_NAME, ".gitkeep"}:
            continue
        if child.is_file() and child.suffix == ".parquet":
            return True
        if child.is_dir() and "=" in child.name:
            return True
    return False


def read_dataset_pointer(dataset: str) -> dict[str, Any] | None:
    current_path = dataset_current_path(dataset)
    if not current_path.exists():
        return None
    return read_json_file(current_path)


def resolve_dataset_version_and_path(dataset: str, *, create_if_missing: bool = False) -> tuple[str, Path]:
    root = dataset_root(dataset)
    root.mkdir(parents=True, exist_ok=True)

    pointer = read_dataset_pointer(dataset)
    known_versions = list_dataset_versions(dataset)
    has_root_data = dataset_has_root_level_data(dataset)

    if pointer is not None:
        active_version = pointer["active_version"]
        if active_version == ROOT_DATASET_VERSION:
            if known_versions:
                raise RuntimeError(
                    f"{dataset} is pinned to {ROOT_DATASET_VERSION} but version directories also exist. "
                    "Move the root-level dataset under a version or switch the active pointer first."
                )
            return active_version, root

        resolved_path = root / active_version
        if not resolved_path.exists():
            if not create_if_missing:
                raise FileNotFoundError(f"Active version path does not exist: {resolved_path}")
            resolved_path.mkdir(parents=True, exist_ok=True)
        return active_version, resolved_path

    if known_versions:
        raise RuntimeError(
            f"{dataset} has version directories {known_versions} but no {CURRENT_FILE_NAME}. "
            "Use schema_ctl.py lake-activate to select the active version."
        )

    if create_if_missing or has_root_data or not root.exists():
        return ROOT_DATASET_VERSION, root

    return ROOT_DATASET_VERSION, root


def resolve_dataset_scan_glob(dataset: str) -> str:
    _active_version, resolved_path = resolve_dataset_version_and_path(dataset)
    return (resolved_path / "**" / "*.parquet").as_posix()


def load_dataset_manifest(dataset: str, version: str | None = None) -> dict[str, Any] | None:
    if version is None:
        version, resolved_path = resolve_dataset_version_and_path(dataset, create_if_missing=True)
    else:
        resolved_path = dataset_root(dataset) if version == ROOT_DATASET_VERSION else dataset_root(dataset) / version

    manifest_path = dataset_manifest_path_for_resolved_path(resolved_path)
    if not manifest_path.exists():
        return None
    return read_json_file(manifest_path)


def append_dataset_history(
    dataset: str,
    *,
    action: str,
    from_version: str | None,
    to_version: str,
    release_id: str,
    note: str | None = None,
) -> None:
    entry = {
        "action": action,
        "dataset": dataset,
        "from_version": from_version,
        "to_version": to_version,
        "release_id": release_id,
        "git_commit": current_git_commit(),
        "noted_at_utc": utc_now_iso(),
        "note": note,
    }
    history_path = dataset_history_path(dataset)
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with history_path.open("a", encoding="utf-8") as history_file:
        history_file.write(json.dumps(entry, sort_keys=True) + "\n")


def ensure_dataset_manifest(
    dataset: str,
    *,
    schema_version: str,
    producer_script: str,
    source_dependencies: dict[str, Any] | None = None,
    note: str | None = None,
    extra: dict[str, Any] | None = None,
) -> Path:
    active_version, resolved_path = resolve_dataset_version_and_path(dataset, create_if_missing=True)
    resolved_path.mkdir(parents=True, exist_ok=True)

    manifest_path = dataset_manifest_path_for_resolved_path(resolved_path)
    existing = read_json_file(manifest_path) if manifest_path.exists() else {}
    definition = DATASET_DEFINITIONS[dataset]

    manifest = {
        "dataset": dataset,
        "version": active_version,
        "schema_version": schema_version,
        "storage_format": "parquet",
        "partitioning": definition["partitioning"],
        "compression": definition["compression"],
        "resolved_path": str(resolved_path),
        "created_at_utc": existing.get("created_at_utc", utc_now_iso()),
        "updated_at_utc": utc_now_iso(),
        "producer_script": producer_script,
        "git_commit": current_git_commit(),
        "note": note,
    }
    if source_dependencies:
        manifest["source_dependencies"] = source_dependencies
    if extra:
        manifest.update(extra)

    write_json_file(manifest_path, manifest)
    return manifest_path


def switch_dataset_version(
    dataset: str,
    *,
    version: str,
    create: bool = False,
    schema_version: str | None = None,
    note: str | None = None,
    action: str = "activate",
) -> dict[str, Any]:
    root = dataset_root(dataset)
    root.mkdir(parents=True, exist_ok=True)

    previous_pointer = read_dataset_pointer(dataset)
    previous_version = previous_pointer["active_version"] if previous_pointer is not None else None
    if previous_version is None and not list_dataset_versions(dataset):
        previous_version = ROOT_DATASET_VERSION

    if version == ROOT_DATASET_VERSION:
        if list_dataset_versions(dataset):
            raise RuntimeError(
                f"Cannot activate {ROOT_DATASET_VERSION} for {dataset} while version directories exist."
            )
        resolved_path = root
    else:
        resolved_path = root / version
        if not resolved_path.exists():
            if not create:
                raise FileNotFoundError(f"Dataset version does not exist: {resolved_path}")
            resolved_path.mkdir(parents=True, exist_ok=True)

    write_json_file(
        dataset_current_path(dataset),
        {
            "active_version": version,
            "activated_at_utc": utc_now_iso(),
            "git_commit": current_git_commit(),
            "note": note,
            "resolved_path": str(resolved_path),
        },
    )

    manifest_schema_version = schema_version or DATASET_DEFINITIONS[dataset]["default_schema_version"]
    ensure_dataset_manifest(
        dataset,
        schema_version=manifest_schema_version,
        producer_script="10_schema_ctl.py",
        note=note,
    )

    append_dataset_history(
        dataset,
        action=action,
        from_version=previous_version,
        to_version=version,
        release_id=make_release_id(f"{dataset}_{action}"),
        note=note,
    )

    manifest = load_dataset_manifest(dataset)
    return {
        "dataset": dataset,
        "active_version": version,
        "resolved_path": str(resolved_path),
        "manifest_path": str(dataset_manifest_path_for_resolved_path(resolved_path)),
        "manifest": manifest,
    }


def get_dataset_status(dataset: str) -> dict[str, Any]:
    root = dataset_root(dataset)
    pointer = read_dataset_pointer(dataset)
    version_dirs = list_dataset_versions(dataset)
    has_root_data = dataset_has_root_level_data(dataset)
    active_version: str | None = None
    resolved_path: Path | None = None
    error: str | None = None

    try:
        active_version, resolved_path = resolve_dataset_version_and_path(dataset, create_if_missing=True)
    except Exception as exc:  # pragma: no cover - defensive status path
        error = str(exc)

    available_versions = version_dirs[:]
    if has_root_data or pointer is None and not version_dirs:
        available_versions = [ROOT_DATASET_VERSION, *available_versions]

    manifest = None
    manifest_path = None
    if resolved_path is not None:
        manifest_path = dataset_manifest_path_for_resolved_path(resolved_path)
        manifest = read_json_file(manifest_path) if manifest_path.exists() else None

    return {
        "dataset": dataset,
        "root": str(root),
        "active_version": active_version,
        "resolved_path": str(resolved_path) if resolved_path is not None else None,
        "available_versions": available_versions,
        "has_root_level_data": has_root_data,
        "pointer_path": str(dataset_current_path(dataset)),
        "pointer": pointer,
        "manifest_path": str(manifest_path) if manifest_path is not None else None,
        "manifest": manifest,
        "error": error,
    }


def writer_snapshot_root(version: str) -> Path:
    return BACKUP_ROOT / "writer_duckdb" / version


def writer_snapshot_manifest_path(version: str, release_id: str) -> Path:
    return writer_snapshot_root(version) / release_id / "snapshot.json"


def writer_snapshot_db_path(version: str, release_id: str) -> Path:
    return writer_snapshot_root(version) / release_id / "writer.duckdb"


def writer_snapshot_wal_path(version: str, release_id: str) -> Path:
    return writer_snapshot_root(version) / release_id / "writer.duckdb.wal"


def ensure_writer_metadata_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(WRITER_METADATA_SQL)


def writer_table_exists(con: duckdb.DuckDBPyConnection, *, schema_name: str, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT count(*)
        FROM information_schema.tables
        WHERE table_schema = ?
          AND table_name = ?
        """,
        [schema_name, table_name],
    ).fetchone()
    return bool(row and row[0])


def writer_user_tables(con: duckdb.DuckDBPyConnection) -> set[tuple[str, str]]:
    rows = con.execute(
        f"""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema NOT IN ('information_schema', 'pg_catalog', '{META_SCHEMA}')
        """
    ).fetchall()
    return {(row[0], row[1]) for row in rows}


def writer_has_known_0001_schema(con: duckdb.DuckDBPyConnection) -> bool:
    return EXPECTED_0001_TABLE_SET.issubset(writer_user_tables(con))


def writer_schema_is_subset_of_0001(con: duckdb.DuckDBPyConnection) -> bool:
    user_tables = writer_user_tables(con)
    return bool(user_tables) and user_tables.issubset(EXPECTED_0001_TABLE_SET)


def writer_has_unmanaged_user_objects(con: duckdb.DuckDBPyConnection) -> bool:
    return bool(writer_user_tables(con) - EXPECTED_0001_TABLE_SET)


def get_current_writer_version(con: duckdb.DuckDBPyConnection) -> str | None:
    ensure_writer_metadata_tables(con)
    row = con.execute(
        f"""
        SELECT current_version
        FROM {META_SCHEMA}.schema_version
        WHERE scope = ?
        """,
        [WRITER_SCOPE],
    ).fetchone()
    return row[0] if row else None


def set_current_writer_version(con: duckdb.DuckDBPyConnection, *, version: str, release_id: str) -> None:
    updated_at = utc_now_iso()
    exists = con.execute(
        f"""
        SELECT count(*)
        FROM {META_SCHEMA}.schema_version
        WHERE scope = ?
        """,
        [WRITER_SCOPE],
    ).fetchone()[0]

    if exists:
        con.execute(
            f"""
            UPDATE {META_SCHEMA}.schema_version
            SET current_version = ?,
                release_id = ?,
                updated_at = ?
            WHERE scope = ?
            """,
            [version, release_id, updated_at, WRITER_SCOPE],
        )
        return

    con.execute(
        f"""
        INSERT INTO {META_SCHEMA}.schema_version (scope, current_version, release_id, updated_at)
        VALUES (?, ?, ?, ?)
        """,
        [WRITER_SCOPE, version, release_id, updated_at],
    )


def insert_writer_history(
    con: duckdb.DuckDBPyConnection,
    *,
    direction: str,
    from_version: str | None,
    to_version: str,
    release_id: str,
    checksum: str | None,
    snapshot_path: str | None,
    status: str,
    note: str | None,
) -> None:
    ensure_writer_metadata_tables(con)
    history_id = f"{release_id}:{direction}:{to_version}"
    con.execute(
        f"""
        INSERT INTO {META_SCHEMA}.schema_migration_history
        (history_id, scope, direction, from_version, to_version, release_id, checksum, git_commit, snapshot_path, status, note, started_at, finished_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)
        """,
        [
            history_id,
            WRITER_SCOPE,
            direction,
            from_version,
            to_version,
            release_id,
            checksum,
            current_git_commit(),
            snapshot_path,
            status,
            note,
        ],
    )


def load_writer_migrations() -> list[MigrationSpec]:
    migrations: list[MigrationSpec] = []
    if not WRITER_MIGRATIONS_ROOT.exists():
        return migrations

    for version_dir in sorted(path for path in WRITER_MIGRATIONS_ROOT.iterdir() if path.is_dir()):
        manifest_path = version_dir / "manifest.json"
        up_sql_path = version_dir / "up.sql"
        down_sql_path = version_dir / "down.sql"
        if not manifest_path.exists() or not up_sql_path.exists():
            continue
        manifest = read_json_file(manifest_path)
        checksum = hashlib.sha256(up_sql_path.read_bytes()).hexdigest()
        migrations.append(
            MigrationSpec(
                version=version_dir.name,
                manifest=manifest,
                up_sql_path=up_sql_path,
                down_sql_path=down_sql_path if down_sql_path.exists() else None,
                checksum=checksum,
            )
        )
    return migrations


def latest_writer_migration_version() -> str | None:
    migrations = load_writer_migrations()
    return migrations[-1].version if migrations else None


def create_writer_snapshot(
    con: duckdb.DuckDBPyConnection,
    *,
    version: str,
    release_id: str,
    note: str | None = None,
    db_path: Path = WRITER_DB_PATH,
) -> Path:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    BACKUP_ROOT.mkdir(parents=True, exist_ok=True)
    con.execute("CHECKPOINT")

    snapshot_dir = writer_snapshot_root(version) / release_id
    snapshot_dir.mkdir(parents=True, exist_ok=True)

    db_snapshot_path = writer_snapshot_db_path(version, release_id)
    shutil.copy2(db_path, db_snapshot_path)

    live_wal_path = Path(f"{db_path}.wal")
    wal_snapshot_path = writer_snapshot_wal_path(version, release_id)
    if live_wal_path.exists():
        shutil.copy2(live_wal_path, wal_snapshot_path)
    elif wal_snapshot_path.exists():
        wal_snapshot_path.unlink()

    manifest = {
        "scope": WRITER_SCOPE,
        "version": version,
        "release_id": release_id,
        "db_snapshot_path": str(db_snapshot_path),
        "wal_snapshot_path": str(wal_snapshot_path) if wal_snapshot_path.exists() else None,
        "git_commit": current_git_commit(),
        "created_at_utc": utc_now_iso(),
        "note": note,
    }
    write_json_file(writer_snapshot_manifest_path(version, release_id), manifest)
    return db_snapshot_path


def find_writer_snapshot(version: str) -> dict[str, Any] | None:
    version_root = writer_snapshot_root(version)
    if not version_root.exists():
        return None

    manifests = sorted(version_root.glob("*/snapshot.json"))
    if not manifests:
        return None
    return read_json_file(manifests[-1])


def bootstrap_writer_schema(con: duckdb.DuckDBPyConnection, *, db_path: Path = WRITER_DB_PATH) -> str | None:
    ensure_writer_metadata_tables(con)
    current_version = get_current_writer_version(con)
    if current_version is not None:
        return current_version

    if writer_has_known_0001_schema(con):
        release_id = make_release_id("writer_bootstrap")
        set_current_writer_version(con, version="0001_init", release_id=release_id)
        snapshot_path = create_writer_snapshot(con, version="0001_init", release_id=release_id, note="bootstrap legacy writer schema", db_path=db_path)
        insert_writer_history(
            con,
            direction="bootstrap",
            from_version=None,
            to_version="0001_init",
            release_id=release_id,
            checksum=None,
            snapshot_path=str(snapshot_path),
            status="applied",
            note="Adopted pre-existing writer schema as 0001_init",
        )
        return "0001_init"

    if writer_schema_is_subset_of_0001(con):
        return None

    if writer_has_unmanaged_user_objects(con):
        raise RuntimeError(
            "writer.duckdb contains user tables but no managed schema version metadata. "
            "Manual adoption is required before migrations can continue."
        )

    return None


def apply_writer_migration(
    con: duckdb.DuckDBPyConnection,
    migration: MigrationSpec,
    *,
    from_version: str | None,
    note: str | None = None,
    db_path: Path = WRITER_DB_PATH,
) -> str:
    release_id = make_release_id("writer_migrate")
    sql_text = migration.up_sql_path.read_text(encoding="utf-8")

    con.execute("BEGIN")
    try:
        con.execute(sql_text)
        set_current_writer_version(con, version=migration.version, release_id=release_id)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        insert_writer_history(
            con,
            direction="up",
            from_version=from_version,
            to_version=migration.version,
            release_id=release_id,
            checksum=migration.checksum,
            snapshot_path=None,
            status="failed",
            note=note,
        )
        raise

    snapshot_path = create_writer_snapshot(
        con,
        version=migration.version,
        release_id=release_id,
        note=note or migration.manifest.get("description"),
        db_path=db_path,
    )
    insert_writer_history(
        con,
        direction="up",
        from_version=from_version,
        to_version=migration.version,
        release_id=release_id,
        checksum=migration.checksum,
        snapshot_path=str(snapshot_path),
        status="applied",
        note=note or migration.manifest.get("description"),
    )
    return migration.version


def ensure_writer_schema_latest(
    con: duckdb.DuckDBPyConnection,
    *,
    note: str | None = None,
    db_path: Path = WRITER_DB_PATH,
) -> str | None:
    ensure_writer_metadata_tables(con)
    current_version = bootstrap_writer_schema(con, db_path=db_path)
    migrations = load_writer_migrations()
    if not migrations:
        return current_version

    version_order = {migration.version: index for index, migration in enumerate(migrations)}
    if current_version is not None and current_version not in version_order:
        raise RuntimeError(f"Current writer version {current_version} is not present in local migration files.")

    current_index = version_order[current_version] if current_version is not None else -1
    for migration in migrations[current_index + 1 :]:
        current_version = apply_writer_migration(
            con,
            migration,
            from_version=current_version,
            note=note,
            db_path=db_path,
        )

    return current_version


def rollback_writer_schema(*, target_version: str, db_path: Path = WRITER_DB_PATH, note: str | None = None) -> dict[str, Any]:
    if target_version == "":
        raise ValueError("target_version must not be empty")

    snapshot = find_writer_snapshot(target_version)
    if snapshot is None:
        raise FileNotFoundError(f"No writer snapshot found for version {target_version}")

    db_path.parent.mkdir(parents=True, exist_ok=True)

    current_version = None
    if db_path.exists():
        current_con = duckdb.connect(str(db_path))
        try:
            ensure_writer_metadata_tables(current_con)
            current_version = bootstrap_writer_schema(current_con, db_path=db_path)
            if current_version is None:
                current_version = get_current_writer_version(current_con)
            if current_version is not None:
                create_writer_snapshot(
                    current_con,
                    version=current_version,
                    release_id=make_release_id("writer_pre_rollback"),
                    note=f"Pre-rollback snapshot before restoring {target_version}",
                    db_path=db_path,
                )
        finally:
            current_con.close()

    shutil.copy2(Path(snapshot["db_snapshot_path"]), db_path)
    live_wal_path = Path(f"{db_path}.wal")
    snapshot_wal = snapshot.get("wal_snapshot_path")
    if snapshot_wal:
        shutil.copy2(Path(snapshot_wal), live_wal_path)
    elif live_wal_path.exists():
        live_wal_path.unlink()

    restored_con = duckdb.connect(str(db_path))
    try:
        ensure_writer_metadata_tables(restored_con)
        release_id = make_release_id("writer_rollback")
        set_current_writer_version(restored_con, version=target_version, release_id=release_id)
        restored_snapshot_path = create_writer_snapshot(
            restored_con,
            version=target_version,
            release_id=release_id,
            note=note or f"Rollback to {target_version}",
            db_path=db_path,
        )
        insert_writer_history(
            restored_con,
            direction="rollback",
            from_version=current_version,
            to_version=target_version,
            release_id=release_id,
            checksum=None,
            snapshot_path=str(restored_snapshot_path),
            status="applied",
            note=note or f"Restored snapshot {snapshot['release_id']}",
        )
    finally:
        restored_con.close()

    return {
        "db_path": str(db_path),
        "from_version": current_version,
        "to_version": target_version,
        "restored_snapshot_release_id": snapshot["release_id"],
        "restored_snapshot_path": snapshot["db_snapshot_path"],
    }


def get_writer_status(*, db_path: Path = WRITER_DB_PATH) -> dict[str, Any]:
    migrations = load_writer_migrations()
    status = {
        "db_path": str(db_path),
        "db_exists": db_path.exists(),
        "latest_migration_version": migrations[-1].version if migrations else None,
        "known_migrations": [migration.version for migration in migrations],
        "current_version": None,
        "bootstrap_candidate": None,
        "adoption_error": None,
        "history": [],
        "snapshots": {},
    }

    for migration in migrations:
        snapshot = find_writer_snapshot(migration.version)
        if snapshot is not None:
            status["snapshots"][migration.version] = snapshot

    if not db_path.exists():
        return status

    con = duckdb.connect(str(db_path))
    try:
        ensure_writer_metadata_tables(con)
        current_version = get_current_writer_version(con)
        status["current_version"] = current_version
        if current_version is None:
            if writer_has_known_0001_schema(con) or writer_schema_is_subset_of_0001(con):
                status["bootstrap_candidate"] = "0001_init"
            elif writer_has_unmanaged_user_objects(con):
                status["adoption_error"] = (
                    "writer.duckdb contains unmanaged user tables; inspect the file and adopt it manually before migration."
                )

        history_rows = con.execute(
            f"""
            SELECT
                direction,
                from_version,
                to_version,
                release_id,
                checksum,
                git_commit,
                snapshot_path,
                status,
                note,
                started_at,
                finished_at
            FROM {META_SCHEMA}.schema_migration_history
            WHERE scope = ?
            ORDER BY started_at
            """,
            [WRITER_SCOPE],
        ).fetchall()
        status["history"] = [
            {
                "direction": row[0],
                "from_version": row[1],
                "to_version": row[2],
                "release_id": row[3],
                "checksum": row[4],
                "git_commit": row[5],
                "snapshot_path": row[6],
                "status": row[7],
                "note": row[8],
                "started_at": str(row[9]),
                "finished_at": str(row[10]),
            }
            for row in history_rows
        ]
    finally:
        con.close()

    return status
