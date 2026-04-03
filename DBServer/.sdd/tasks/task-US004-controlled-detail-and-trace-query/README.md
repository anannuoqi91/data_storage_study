# 任务：US004 - 执行受控明细与轨迹查询

## 做什么
实现 detail query 和 trace lookup query 的固定入口、release 绑定和查询句柄追踪。

## 为什么做
查询侧必须在固定快照上工作，否则删数和回滚时会读到不一致结果。

## 做完什么样
明细查询和轨迹段查询都能在固定 release 上执行，并在删数时被识别为活跃查询。

## 依赖
- 前置：US002、US003
- 共享实体：DatabaseRelease、ActiveQuery、TraceIndexSegment

## 验收标准
见 `tests/acceptance.md`。
