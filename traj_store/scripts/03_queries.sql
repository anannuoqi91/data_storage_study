-- Resolve the active dataset globs first:
--   python3 traj_store/scripts/10_schema_ctl.py status
-- Then replace <box_info_active_glob> and <events_active_glob> below.

CREATE OR REPLACE TEMP VIEW box_info_active AS
SELECT
    trace_id,
    epoch_ms(strptime(date || ' ' || hour || ':00:00', '%Y-%m-%d %H:%M:%S')) + sample_offset_ms AS sample_timestamp,
    obj_type,
    position_x_mm / 1000.0 AS position_x,
    position_y_mm / 1000.0 AS position_y,
    position_z_mm / 1000.0 AS position_z,
    length_mm / 1000.0 AS length,
    width_mm / 1000.0 AS width,
    height_mm / 1000.0 AS height,
    speed_centi_kmh / 100.0 AS speed_kmh,
    spindle_centi_deg / 100.0 AS spindle,
    lane_id,
    frame_id,
    date,
    hour
FROM read_parquet(
    '<box_info_active_glob>',
    hive_partitioning = true
);

-- 1. Query one day of box_info facts.
SELECT
    trace_id,
    sample_timestamp,
    obj_type,
    position_x,
    position_y,
    position_z,
    length,
    width,
    height,
    speed_kmh,
    spindle,
    lane_id,
    frame_id
FROM box_info_active
WHERE date = '2025-03-14'
ORDER BY sample_timestamp, trace_id;

-- 2. Count rows and distinct trajectories for the day.
SELECT
    count(*) AS row_cnt,
    count(DISTINCT trace_id) AS trace_cnt
FROM box_info_active
WHERE date = '2025-03-14';

-- 3. Point lookup by trace + frame.
SELECT *
FROM box_info_active
WHERE date = '2025-03-14'
  AND trace_id = 100123
  AND frame_id = 45;

-- 4. Point lookup by trace + timestamp.
SELECT *
FROM box_info_active
WHERE date = '2025-03-14'
  AND trace_id = 100125
  AND sample_timestamp = 1741910405000;

-- 5. Read derived overspeed events.
SELECT
    event_id,
    event_type,
    trace_id,
    event_timestamp,
    speed_kmh,
    source_frame_id
FROM read_parquet(
    '<events_active_glob>',
    hive_partitioning = true
)
WHERE event_type = 'overspeed'
  AND date = '2025-03-14'
ORDER BY event_timestamp, trace_id;

-- 6. Event back-reference into box_info.
SELECT
    e.event_id,
    e.event_type,
    e.event_timestamp,
    b.*
FROM read_parquet(
    '<events_active_glob>',
    hive_partitioning = true
) AS e
JOIN box_info_active AS b
  ON b.trace_id = e.source_trace_id
 AND b.sample_timestamp = e.source_sample_timestamp
 AND b.frame_id = e.source_frame_id
WHERE e.event_type = 'overspeed'
  AND e.date = '2025-03-14'
ORDER BY e.trace_id;

-- 7. Build a one-day hot cache and indexes directly in DuckDB.
ATTACH 'traj_store/data/tmp/hot_day.duckdb' AS hot;

CREATE OR REPLACE TABLE hot.box_info_20250314 AS
SELECT
    trace_id,
    sample_timestamp,
    obj_type,
    position_x,
    position_y,
    position_z,
    length,
    width,
    height,
    speed_kmh,
    spindle,
    lane_id,
    frame_id
FROM box_info_active
WHERE date = '2025-03-14';

CREATE INDEX IF NOT EXISTS idx_trace_ts
ON hot.box_info_20250314(trace_id, sample_timestamp);

CREATE INDEX IF NOT EXISTS idx_trace_frame
ON hot.box_info_20250314(trace_id, frame_id);

SELECT *
FROM hot.box_info_20250314
WHERE trace_id = 100125
  AND sample_timestamp = 1741910405000;
