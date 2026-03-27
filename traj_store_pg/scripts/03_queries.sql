-- PostgreSQL queries.
-- Example:
--   PGPASSWORD=benchpass psql -h 127.0.0.1 -p 15432 -U bench -d traj_store_pg

-- 1. Inspect ingest batches.
SELECT batch_id, row_count, min_sample_timestamp, max_sample_timestamp
FROM ingest_batches
ORDER BY batch_id
LIMIT 20;

-- 2. Count tracked batches and hot traces.
SELECT count(*) AS batch_count FROM ingest_batches;
SELECT count(*) AS trace_count FROM trace_latest_state;

-- 3. Inspect latest state for one trace.
SELECT *
FROM trace_latest_state
WHERE trace_id = 100123;

-- TDengine queries.
-- Example:
--   curl -u root:taosdata \
--     -d "SELECT COUNT(*) FROM traj_store_pg_td.box_info_compact;" \
--     http://127.0.0.1:16041/rest/sql

-- 4. Count total facts in TDengine.
SELECT COUNT(*) FROM traj_store_pg_td.box_info_compact;

-- 5. Count rows for one trace by tag.
SELECT trace_id, COUNT(*)
FROM traj_store_pg_td.box_info_compact
WHERE trace_id = 100123
GROUP BY trace_id;

-- 6. Inspect one trace over a time range.
SELECT ts, frame_id, lane_id, position_x_mm, speed_centi_kmh
FROM traj_store_pg_td.box_info_compact
WHERE trace_id = 100123
  AND ts >= '2025-03-14 00:00:00.000'
  AND ts < '2025-03-14 00:05:00.000'
ORDER BY ts;
