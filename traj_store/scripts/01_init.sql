CREATE SCHEMA IF NOT EXISTS od;

CREATE TABLE IF NOT EXISTS od.box_info_ingest (
    trace_id INTEGER NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type UTINYINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm USMALLINT NOT NULL,
    width_mm USMALLINT NOT NULL,
    height_mm USMALLINT NOT NULL,
    speed_centi_kmh USMALLINT NOT NULL,
    spindle_centi_deg USMALLINT NOT NULL,
    lane_id UTINYINT NOT NULL,
    frame_id INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS od.box_info_staging (
    trace_id INTEGER NOT NULL,
    sample_timestamp BIGINT NOT NULL,
    obj_type UTINYINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm USMALLINT NOT NULL,
    width_mm USMALLINT NOT NULL,
    height_mm USMALLINT NOT NULL,
    speed_centi_kmh USMALLINT NOT NULL,
    spindle_centi_deg USMALLINT NOT NULL,
    lane_id UTINYINT NOT NULL,
    frame_id INTEGER NOT NULL
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
