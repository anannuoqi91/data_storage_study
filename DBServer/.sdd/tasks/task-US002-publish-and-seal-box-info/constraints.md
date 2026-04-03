# 相关约束

- 发布窗口固定 10 分钟。
- 分区封口时机为 `hour_end + 2 分钟`。
- `current_release` 只能指向一个 release。
- 只有元数据提交成功的数据才算正式生效。
