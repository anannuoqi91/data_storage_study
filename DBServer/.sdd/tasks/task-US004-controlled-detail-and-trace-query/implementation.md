# 实施指令

## 你需要做的
1. 为明细查询和轨迹段查询生成固定 SQL 计划
2. 在执行前后维护 `active_queries`
3. 让轨迹段查询优先走 `trace_index`

## 禁止事项
- 不要让查询跨 release 漂移
- 不要直接 `glob` 扫全目录
