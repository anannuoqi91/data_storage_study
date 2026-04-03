# 实施指令

## 你需要做的
1. 提供 release 列表和状态查看命令
2. 提供 rollback 命令
3. 在启动时清理 staging orphan 和 stale query

## 禁止事项
- 不要回滚到已经被 retention 裁掉的数据
- 不要跳过 journal/checkpoint 更新
