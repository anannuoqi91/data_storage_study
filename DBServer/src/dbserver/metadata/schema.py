SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS schema_versions (
        schema_version_id VARCHAR PRIMARY KEY,
        created_at_ms BIGINT NOT NULL,
        table_name VARCHAR NOT NULL,
        schema_json VARCHAR NOT NULL,
        partition_rule VARCHAR NOT NULL,
        sort_rule VARCHAR NOT NULL,
        compression_codec VARCHAR NOT NULL,
        is_current BOOLEAN NOT NULL DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS database_releases (
        release_id VARCHAR PRIMARY KEY,
        parent_release_id VARCHAR,
        schema_version_id VARCHAR NOT NULL,
        created_at_ms BIGINT NOT NULL,
        event_type VARCHAR NOT NULL,
        retention_floor_ms BIGINT NOT NULL,
        note VARCHAR NOT NULL DEFAULT ''
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS file_groups (
        file_group_id VARCHAR PRIMARY KEY,
        dataset_kind VARCHAR NOT NULL,
        device_id INTEGER,
        date_utc VARCHAR,
        hour_utc INTEGER,
        window_start_ms BIGINT,
        window_end_ms BIGINT,
        min_event_time_ms BIGINT,
        max_event_time_ms BIGINT,
        row_count BIGINT NOT NULL DEFAULT 0,
        file_count BIGINT NOT NULL DEFAULT 0,
        total_bytes BIGINT NOT NULL DEFAULT 0,
        state VARCHAR NOT NULL,
        path_list_json VARCHAR NOT NULL,
        created_at_ms BIGINT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS database_release_items (
        release_id VARCHAR NOT NULL,
        file_group_id VARCHAR NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS partition_states (
        date_utc VARCHAR NOT NULL,
        hour_utc INTEGER NOT NULL,
        device_id INTEGER NOT NULL,
        state VARCHAR NOT NULL,
        last_event_time_ms BIGINT,
        watermark_ms BIGINT,
        last_publish_release_id VARCHAR,
        last_seal_release_id VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS active_queries (
        query_id VARCHAR PRIMARY KEY,
        release_id VARCHAR NOT NULL,
        query_kind VARCHAR NOT NULL,
        started_at_ms BIGINT NOT NULL,
        status VARCHAR NOT NULL,
        owner_pid INTEGER,
        target_range_start_ms BIGINT,
        target_range_end_ms BIGINT
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS current_release (
        singleton BOOLEAN PRIMARY KEY,
        release_id VARCHAR,
        updated_at_ms BIGINT NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_releases_created
    ON database_releases (created_at_ms)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_file_groups_dataset_device_time
    ON file_groups (dataset_kind, device_id, min_event_time_ms, max_event_time_ms)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_release_items_release
    ON database_release_items (release_id)
    """,
]
