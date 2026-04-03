# 任务：US002 - 发布和封口 box_info 数据

## 做什么
实现 10 分钟发布、小时封口、release 切换和 superseded 文件清理。

## 为什么做
这是查询侧看到稳定可见集的核心，也是 retention 和 rollback 的基础。

## 做完什么样
发布创建新 release；封口只在 `hour_end + late_window` 后触发；旧批次文件在无引用后删除。

## 依赖
- 前置：US001
- 共享实体：FileGroup、DatabaseRelease、PartitionState

## 验收标准
见 `tests/acceptance.md`。
