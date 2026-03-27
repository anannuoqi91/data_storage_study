SELECT count()
FROM box_info_compact_zstd;

SELECT
    table,
    sum(rows) AS rows,
    sum(bytes_on_disk) AS bytes_on_disk,
    sum(data_compressed_bytes) AS data_compressed_bytes,
    sum(data_uncompressed_bytes) AS data_uncompressed_bytes
FROM system.parts
WHERE database = currentDatabase()
  AND active
GROUP BY table
ORDER BY table;

SELECT
    sample_date,
    sample_hour,
    count() AS rows
FROM box_info_compact_zstd
GROUP BY sample_date, sample_hour
ORDER BY sample_date, sample_hour;
