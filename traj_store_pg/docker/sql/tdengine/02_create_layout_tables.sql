CREATE STABLE IF NOT EXISTS __DB_NAME__.box_info_raw (
    ts TIMESTAMP,
    obj_type TINYINT,
    position_x FLOAT,
    position_y FLOAT,
    position_z FLOAT,
    length FLOAT,
    width FLOAT,
    height FLOAT,
    speed_kmh FLOAT,
    spindle FLOAT,
    lane_id TINYINT,
    frame_id INT
) TAGS (trace_id INT);

CREATE STABLE IF NOT EXISTS __DB_NAME__.box_info_compact (
    ts TIMESTAMP,
    obj_type TINYINT,
    lane_id TINYINT,
    frame_id INT,
    position_x_mm INT,
    position_y_mm INT,
    position_z_mm INT,
    length_mm INT,
    width_mm INT,
    height_mm INT,
    speed_centi_kmh INT,
    spindle_centi_deg INT
) TAGS (trace_id INT);
