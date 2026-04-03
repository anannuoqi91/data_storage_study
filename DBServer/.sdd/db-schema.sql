-- DBServer 第一版数据库设计
-- 说明：
-- 1. 元数据保存在 metadata.db
-- 2. 明细和派生数据保存在 Parquet 文件中
-- 3. 以下 DDL 既描述现有 metadata.db，也描述受控查询的逻辑契约

CREATE TABLE schema_versions (
    schema_version_id VARCHAR PRIMARY KEY,
    created_at_ms BIGINT NOT NULL,
    table_name VARCHAR NOT NULL,
    schema_json VARCHAR NOT NULL,
    partition_rule VARCHAR NOT NULL,
    sort_rule VARCHAR NOT NULL,
    compression_codec VARCHAR NOT NULL,
    is_current BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE TABLE database_releases (
    release_id VARCHAR PRIMARY KEY,
    parent_release_id VARCHAR,
    schema_version_id VARCHAR NOT NULL,
    created_at_ms BIGINT NOT NULL,
    event_type VARCHAR NOT NULL,
    retention_floor_ms BIGINT NOT NULL,
    note VARCHAR NOT NULL DEFAULT ''
);

CREATE TABLE file_groups (
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
);

CREATE TABLE database_release_items (
    release_id VARCHAR NOT NULL,
    file_group_id VARCHAR NOT NULL
);

CREATE TABLE partition_states (
    date_utc VARCHAR NOT NULL,
    hour_utc INTEGER NOT NULL,
    device_id INTEGER NOT NULL,
    state VARCHAR NOT NULL,
    last_event_time_ms BIGINT,
    watermark_ms BIGINT,
    last_publish_release_id VARCHAR,
    last_seal_release_id VARCHAR
);

CREATE TABLE active_queries (
    query_id VARCHAR PRIMARY KEY,
    release_id VARCHAR NOT NULL,
    query_kind VARCHAR NOT NULL,
    started_at_ms BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    owner_pid INTEGER,
    target_range_start_ms BIGINT,
    target_range_end_ms BIGINT
);

CREATE TABLE current_release (
    singleton BOOLEAN PRIMARY KEY,
    release_id VARCHAR,
    updated_at_ms BIGINT NOT NULL
);

-- 逻辑数据集契约：box_info
-- 排序键：event_time_ms, frame_id, trace_id
-- 分区：date_utc / hour_utc / device_id

-- 逻辑数据集契约：trace_index_hot / trace_index_cold
-- 字段：
-- device_id, trace_id, segment_id, min_event_time_ms, max_event_time_ms,
-- start_frame_id, end_frame_id, row_count, state

-- 逻辑数据集契约：obj_type_rollup
-- 字段：
-- device_id, lane_id, bucket_start_ms, obj_type, box_count, trace_count
