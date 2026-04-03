# 相关约束

- `trace_index` 和 `rollup` 是可重建派生数据，不是唯一真相。
- 热索引窗口 30 天，超出后应归入 cold。
- `trace_id` 分段规则为 `frame_id` 回退或时间间隔大于 150ms。
- `rollup` 粒度是 `device_id + lane_id + 10分钟桶 + obj_type`。
