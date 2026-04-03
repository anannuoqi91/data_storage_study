# 任务：US006 - 查看状态、回滚和恢复

## 做什么
补齐 release 列表、分区状态、活跃查询查看、回滚入口和恢复清理能力。

## 为什么做
没有这些入口，发布异常、删数异常和派生数据重建异常都只能手工探库排障。

## 做完什么样
值班人员能列出 release、执行回滚、查看查询占用，并在重启时清理 orphan/stale 状态。

## 依赖
- 前置：US002、US003、US004、US005
- 共享实体：DatabaseRelease、ActiveQuery、ReleaseJournal

## 验收标准
见 `tests/acceptance.md`。
