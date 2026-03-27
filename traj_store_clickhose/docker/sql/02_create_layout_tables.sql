CREATE DATABASE IF NOT EXISTS __DB_NAME__;
USE __DB_NAME__;

CREATE TABLE IF NOT EXISTS box_info_raw_lz4 (
    trace_id UInt32,
    sample_timestamp DateTime64(3, 'UTC'),
    obj_type UInt8,
    position_x Float32,
    position_y Float32,
    position_z Float32,
    length Float32,
    width Float32,
    height Float32,
    speed_kmh Float32,
    spindle Float32,
    lane_id UInt8,
    frame_id UInt32
)
ENGINE = MergeTree
PARTITION BY toDate(sample_timestamp)
ORDER BY (sample_timestamp, trace_id, frame_id)
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS box_info_compact_zstd (
    trace_id UInt32 CODEC(ZSTD(3)),
    sample_date Date CODEC(ZSTD(1)),
    sample_hour UInt8 CODEC(ZSTD(1)),
    sample_offset_ms UInt32 CODEC(ZSTD(3)),
    obj_type UInt8 CODEC(ZSTD(1)),
    position_x_mm Int32 CODEC(ZSTD(3)),
    position_y_mm Int32 CODEC(ZSTD(3)),
    position_z_mm Int32 CODEC(ZSTD(3)),
    length_mm UInt16 CODEC(ZSTD(1)),
    width_mm UInt16 CODEC(ZSTD(1)),
    height_mm UInt16 CODEC(ZSTD(1)),
    speed_centi_kmh UInt16 CODEC(ZSTD(1)),
    spindle_centi_deg UInt16 CODEC(ZSTD(1)),
    lane_id UInt8 CODEC(ZSTD(1)),
    frame_id UInt32 CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY (sample_date, sample_hour)
ORDER BY (sample_date, sample_hour, sample_offset_ms, trace_id, frame_id)
SETTINGS index_granularity = 8192;
