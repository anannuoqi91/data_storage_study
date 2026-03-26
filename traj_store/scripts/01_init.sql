CREATE SCHEMA IF NOT EXISTS od;

CREATE TABLE IF NOT EXISTS od.box_info_ingest (
    box_id BIGINT,
    trace_id BIGINT NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x REAL NOT NULL,
    position_y REAL NOT NULL,
    position_z REAL NOT NULL,
    length REAL NOT NULL,
    width REAL NOT NULL,
    height REAL NOT NULL,
    speed_kmh REAL NOT NULL,
    spindle REAL NOT NULL,
    lane_id INTEGER NOT NULL,
    frame_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS od.box_info_staging (
    box_id BIGINT,
    trace_id BIGINT NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type SMALLINT NOT NULL,
    position_x REAL NOT NULL,
    position_y REAL NOT NULL,
    position_z REAL NOT NULL,
    length REAL NOT NULL,
    width REAL NOT NULL,
    height REAL NOT NULL,
    speed_kmh REAL NOT NULL,
    spindle REAL NOT NULL,
    lane_id INTEGER NOT NULL,
    frame_id BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS od.flush_checkpoint (
    id INTEGER,
    flushed_before_ts BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

INSERT INTO od.flush_checkpoint (id, flushed_before_ts, updated_at)
SELECT 1, 0, current_timestamp
WHERE NOT EXISTS (
    SELECT 1
    FROM od.flush_checkpoint
    WHERE id = 1
);

CREATE TABLE IF NOT EXISTS od.flush_log (
    range_start_ts BIGINT NOT NULL,
    range_end_ts BIGINT NOT NULL,
    row_count BIGINT NOT NULL,
    flushed_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

CREATE TABLE IF NOT EXISTS od.event_materialization_checkpoint (
    event_type VARCHAR,
    processed_before_ts BIGINT NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT current_timestamp
);

INSERT INTO od.event_materialization_checkpoint (event_type, processed_before_ts, updated_at)
SELECT 'overspeed', 0, current_timestamp
WHERE NOT EXISTS (
    SELECT 1
    FROM od.event_materialization_checkpoint
    WHERE event_type = 'overspeed'
);
