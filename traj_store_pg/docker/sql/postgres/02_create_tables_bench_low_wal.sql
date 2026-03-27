DROP TABLE IF EXISTS ingest_batches;
DROP TABLE IF EXISTS trace_latest_state;

CREATE UNLOGGED TABLE ingest_batches (
    batch_id BIGINT PRIMARY KEY,
    storage_layout TEXT NOT NULL,
    row_count INTEGER NOT NULL,
    min_trace_id INTEGER NOT NULL,
    max_trace_id INTEGER NOT NULL,
    min_sample_timestamp TIMESTAMPTZ NOT NULL,
    max_sample_timestamp TIMESTAMPTZ NOT NULL,
    min_frame_id INTEGER NOT NULL,
    max_frame_id INTEGER NOT NULL,
    received_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (autovacuum_enabled = false);

CREATE UNLOGGED TABLE trace_latest_state (
    trace_id INTEGER PRIMARY KEY,
    last_sample_timestamp TIMESTAMPTZ NOT NULL,
    last_frame_id INTEGER NOT NULL,
    obj_type SMALLINT NOT NULL,
    lane_id SMALLINT NOT NULL,
    position_x_mm INTEGER NOT NULL,
    position_y_mm INTEGER NOT NULL,
    position_z_mm INTEGER NOT NULL,
    length_mm INTEGER NOT NULL,
    width_mm INTEGER NOT NULL,
    height_mm INTEGER NOT NULL,
    speed_centi_kmh INTEGER NOT NULL,
    spindle_centi_deg INTEGER NOT NULL,
    updated_at_utc TIMESTAMPTZ NOT NULL DEFAULT now()
) WITH (autovacuum_enabled = false);

ALTER TABLE ingest_batches ALTER COLUMN storage_layout SET STORAGE EXTENDED;
ALTER TABLE ingest_batches ALTER COLUMN storage_layout SET COMPRESSION __TOAST_COMPRESSION__;
