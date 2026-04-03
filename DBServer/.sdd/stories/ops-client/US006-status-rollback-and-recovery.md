# US006 - 查看状态、回滚和恢复

- **角色**：R-O02 存储运维工程师
- **客户端**：运维控制端
- **优先级**：P1

## 故事
作为一个存储运维工程师，我想查看当前 release、分区状态和查询占用，并在必要时回滚到历史 release，以便在发布、封口、重建或删数异常后快速止损。

## 验收场景

### 场景1：查看状态
假设我在值班排障，
当我执行 `current-release`、`list-releases`、`list-partitions`、`list-active-queries`，
系统应该给我足够的信息判断当前库状态。

### 场景2：手动回滚
假设最新 release 行为异常但历史 release 仍可用，
当我执行 `rollback-release`，
系统应该生成新的 release switch 事件并把当前可见集切回目标快照。

### 场景3：崩溃恢复
假设进程异常退出留下 staging 文件或陈旧查询句柄，
系统下次启动时应该清理 staging/orphan 并恢复活跃查询状态。
