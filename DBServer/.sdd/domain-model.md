# 领域模型

## 核心实体

### 帧批次（FrameBatch）
- 字段：`device_id`、`frame_id`、`event_time_ms`、`records`
- 代表上游的一帧快照
- 一帧里的所有 box 必须共享同一个时间、设备和帧号

### 盒子记录（BoxRecord）
- 字段：`trace_id`、`device_id`、`event_time_ms`、`obj_type`、位置尺寸、速度航向、`lane_id`、`frame_id`
- 是 `box_info` 的最小明细行

### 文件组（FileGroup）
- 字段：`file_group_id`、`dataset_kind`、`partition`、`window_start_ms`、`window_end_ms`、`row_count`、`state`、`path_list`
- 代表一次 release 中可见的一组物理文件
- `dataset_kind` 包含 `box_info`、`trace_index_hot`、`trace_index_cold`、`obj_type_rollup`

### 分区状态（PartitionState）
- 字段：`date_utc`、`hour_utc`、`device_id`、`state`、`last_event_time_ms`、`watermark_ms`
- 描述某个 `date/hour/device` 分区目前处于 `open/publishable/sealing/sealed/deleting/deleted` 哪个状态

### 数据库发布版本（DatabaseRelease）
- 字段：`release_id`、`parent_release_id`、`schema_version_id`、`event_type`、`retention_floor_ms`
- 代表对外可见数据集合的快照
- 当前 release 通过 `current_release` 单例指针暴露

### schema 版本（SchemaVersion）
- 字段：`schema_version_id`、`schema_json`、`partition_rule`、`sort_rule`、`compression_codec`
- 定义 `box_info` 存储契约

### 活跃查询（ActiveQuery）
- 字段：`query_id`、`release_id`、`query_kind`、`started_at_ms`、`target_range`
- 用于在删数时识别冲突查询

### 轨迹段索引（TraceIndexSegment）
- 字段：`device_id`、`trace_id`、`segment_id`、`min_event_time_ms`、`max_event_time_ms`、`start_frame_id`、`end_frame_id`、`state`
- 是由 `box_info` 明细派生出的轨迹段列表

### obj_type 聚合桶（ObjTypeRollupBucket）
- 字段：`device_id`、`lane_id`、`bucket_start_ms`、`obj_type`、`box_count`、`trace_count`
- 用于 10 分钟粒度聚合查询

## 实体关系总览

- `FrameBatch` --1:N--> `BoxRecord`
- `BoxRecord` --N:1--> `FileGroup(box_info)`
- `FileGroup` --N:1--> `DatabaseRelease`
- `DatabaseRelease` --N:1--> `SchemaVersion`
- `PartitionState` --1:1--> `date/hour/device` 物理分区
- `BoxRecord` --派生--> `TraceIndexSegment`
- `BoxRecord` --派生--> `ObjTypeRollupBucket`
- `ActiveQuery` --N:1--> `DatabaseRelease`
