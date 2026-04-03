# 任务：US005 - 执行聚合查询与删数治理

## 做什么
实现 rollup 查询、删数规划、水位触发和删除时的派生数据联动清理。

## 为什么做
磁盘治理和聚合统计是 README 第一版里明确要求的运维能力。

## 做完什么样
可以从 rollup 拿统计结果；可以按保留边界删数；删数后旧派生数据不会遗留。

## 依赖
- 前置：US002、US003、US004
- 共享实体：ObjTypeRollupBucket、DeletePlan、PartitionState

## 验收标准
见 `tests/acceptance.md`。
