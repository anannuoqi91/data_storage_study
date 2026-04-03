# 相关约束

- release 切换必须写入 journal 和 checkpoint。
- stale query handle 必须可恢复清理。
- rollback 只能回到仍存在的历史快照。
- 删除后的数据不允许通过 rollback 恢复。
