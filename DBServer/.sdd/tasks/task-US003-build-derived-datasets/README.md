# 任务：US003 - 生成并重建派生数据

## 做什么
实现 `trace_index_hot/cold` 与 `obj_type_rollup` 的生成、查询支持和手动重建。

## 为什么做
没有派生数据时，轨迹段查询和聚合查询只能反复直扫明细文件，成本过高。

## 做完什么样
发布后有派生文件可查；支持 `rebuild-trace-index`、`rebuild-rollup`；冷热分类可按时间重算。

## 依赖
- 前置：US001、US002
- 共享实体：TraceIndexSegment、ObjTypeRollupBucket、FileGroup

## 验收标准
见 `tests/acceptance.md`。
