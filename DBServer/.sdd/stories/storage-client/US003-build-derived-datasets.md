# US003 - 生成并重建派生数据

- **角色**：R-O01 查询值班工程师
- **客户端**：存储发布核心 / 运维控制端
- **优先级**：P0

## 故事
作为一个查询值班工程师，我想在发布后自动拿到 `trace_index` 和 `obj_type rollup`，并在需要时手动重建它们，以便轨迹查询和聚合查询不会每次都直扫明细文件。

## 验收场景

### 场景1：发布即生成派生数据
假设一个新的 `box_info` 批次发布成功，
系统应该同时产出对应的 `trace_index_hot/cold` 和 `obj_type_rollup` 文件组。

### 场景2：手动重建 trace_index
假设我怀疑 `trace_index` 损坏或需要重分类冷热数据，
当我执行 `rebuild-trace-index`，
系统应该基于当前 `box_info` release 重建 `trace_index` 并切换 release。

### 场景3：手动重建 rollup
假设聚合统计口径更新或派生文件损坏，
当我执行 `rebuild-rollup`，
系统应该基于当前 `box_info` release 重建 `obj_type_rollup` 并切换 release。
