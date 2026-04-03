# DBServer 技术方案设计

使用者可直接参考 [用户使用手册](./docs/user-manual.md)。
如果要把当前原型推进成可部署长稳工程，可参考 [可部署化改造方案](./docs/productionization-plan.md)。

## 1. 目标与范围

本文档定义 DBServer 第一版的存储与发布方案，目标聚焦于以下四件事：

- 接收多设备 `box_info` 数据并稳定入库
- 在磁盘空间受限场景下执行可控的数据删除
- 提供最小必要的版本、回滚、索引、聚合与运维能力
- 基于 `Parquet + DuckDB` 管理离线查询数据面

第一版明确只围绕 `box_info` 一张主表设计，不扩展为通用多表发布框架。

## 2. 已确定边界

### 2.1 技术边界

- 存储格式：`Parquet`
- 查询与元数据存储：`DuckDB`
- 压缩算法：`ZSTD`
- 部署形态：单机、单写入实例、本地磁盘
- 运行形态：接收/发布核心 与 查询执行 同机独立进程
- 设备规模上限：`4` 台
- 上游接入：以 `DDS/cybernode` 为例，但接入协议不是存储核心的一部分
- 数据可靠性边界：不承担机器级或磁盘级容灾，由外部基础设施、备份或上游重放保障

### 2.2 业务边界

- 数据以追加写为主
- 支持有限晚到，晚到上限不超过 `2` 分钟
- 不支持 `2` 分钟窗口之外的历史回灌/补录
- 不支持业务纠正/撤回
- 第一版不做严格去重，若上游重复投递，库内结果也可能重复
- 查询以离线查询为主，查询新鲜度不强求秒级

### 2.3 数据保留边界

- 目标保留期：`180` 天
- 期望尽量将空间压缩在 `200GB` 以内
- 外部模块可下发按时间范围删除指令
- DBServer 也可在空间紧张时自主删数
- 删除不可回滚
- 全库保留边界只允许单向前进

## 3. 总体架构

系统拆为两层：

- 接入适配层
  - 负责订阅 `DDS/cybernode` 或未来其他协议
  - 将原始帧数据转换为统一内部模型
  - 不保留原始 payload
- 存储发布核心
  - 负责内存缓冲、排序、分批发布、小时封口
  - 负责生成 `Parquet`、`trace_index`、`obj_type rollup`
  - 负责维护 `metadata.db`、`release journal`
  - 负责版本切换、回滚、删数、冷热索引转换

查询侧通过受控查询入口访问当前可见数据，不允许外部直接 `glob` 扫描全部 `Parquet` 目录。

## 4. 时间模型

- 源数据提供真实事件时间，单位为 `ns`
- 入库统一使用 `event_time_ms BIGINT`
- 所有物理分区均基于 `event_time_ms` 派生
- 分区时区统一使用 `UTC`
- 设备本地时区转换放在数据库外围处理

以下字段均视为 `event_time_ms` 的派生信息，不作为正式 Parquet 物理列长期保存：

- `date`
- `hour`
- `sample_offset_ms`

其中：

- `date/hour` 仅体现在目录分区中
- `sample_offset_ms` 在查询视图或导出阶段派生

## 5. 主表模型

### 5.1 box_info 字段

第一版保留的主表字段如下：

| 字段名 | 类型 | 说明 |
| --- | --- | --- |
| `event_time_ms` | `BIGINT` | 事件时间，毫秒 |
| `trace_id` | `INTEGER` | 轨迹 ID |
| `device_id` | `USMALLINT` | 设备编号 |
| `obj_type` | `UTINYINT` | box 类型 |
| `position_x_mm` | `INTEGER` | 中心点 X，毫米 |
| `position_y_mm` | `INTEGER` | 中心点 Y，毫米 |
| `position_z_mm` | `INTEGER` | 中心点 Z，毫米 |
| `length_mm` | `USMALLINT` | 长，毫米 |
| `width_mm` | `USMALLINT` | 宽，毫米 |
| `height_mm` | `USMALLINT` | 高，毫米 |
| `speed_centi_kmh_100` | `USMALLINT` | 速度，`km/h * 100` |
| `spindle_centi_deg_100` | `USMALLINT` | 航向角，`0~360 * 100` |
| `lane_id` | `UTINYINT` | 车道 ID，空值统一映射为 `255` |
| `frame_id` | `INTEGER` | 上游帧 ID |

### 5.2 排序与组织

- 目录分区：`date/hour`
- 设备级组织：`date/hour/device_id`
- 正式文件排序键：`event_time_ms, frame_id, trace_id`

推荐目录结构如下：

```text
data/
  box_info/
    date=2026-04-01/
      hour=10/
        device_id=1/
          part-000.parquet
        device_id=2/
          part-000.parquet
```

第一版按 `device_id` 组织文件，而不是多设备混写到同一文件内。原因：

- 核心查询包含 `device_id + time_range`
- 发布与封口按 `device_id` 独立进行
- 回滚、删除与空间治理更直接
- 当前只有 `4` 台设备，小文件风险可控

## 6. 写入、发布与封口

### 6.1 输入特征

当前规模估算：

- 设备数：`4`
- 单设备：`10 fps`
- 单帧 box 数：`0~300`，平均约 `50`
- 全局平均写入量约：`2000 box/s`
- 理论峰值约：`12000 box/s`

### 6.2 写入链路

第一版采用：

- 内存缓冲
- 有界缓冲，过载时优先反压/阻塞消费，不主动丢数据
- 允许进程崩溃时丢失最近 `1~5` 秒的内存缓冲数据
- 不引入 WAL

同一设备输入整体有序，但需要容忍少量乱序。正式写出前统一按以下顺序排序：

- `event_time_ms`
- `frame_id`
- `trace_id`

### 6.3 批次发布

- 数据可见周期：`10` 分钟
- 使用 `watermark = now_utc - 2min` 控制发布边界
- 已发布批次一旦可见即不可变
- 发布按 `device_id` 独立进行，不做跨设备 barrier

示例：

- `10:12` 之后才允许发布 `10:00~10:10` 的切片

### 6.4 小时封口

- 分区封口时机：`hour_end + 2分钟晚到窗口`
- 同一小时开放期间可生成多个 `10分钟` 批次文件
- 小时封口时，将该 `hour/device_id` 下的已发布批次合并为最终小时文件
- 封口后，旧 `10分钟` 批次文件在无引用后删除

第一版不保留“开放小时文件 + 最终小时文件”双副本，避免空间放大。

## 7. 版本模型

### 7.1 两层版本

第一版只管理两层版本：

- `schema_version`
  - 记录字段、类型、分区、排序等存储契约
- `database_release`
  - 记录对外当前可见的数据集合

此外，在实现上还会有更细的发布单元：

- `batch_file_group`
  - 某次批处理生成的一组明细文件、索引增量和 rollup 增量

### 7.2 核心原则

- 查询只读 `current_release`
- `database_release` 不允许混用多个 `schema_version`
- 发生 `schema` 变更时，必须先离线重写旧数据，再整库切换
- 变更 `schema` 时接受维护窗口
- 状态修改操作必须串行执行，共享同一把发布锁

### 7.3 当前版本切换

以下操作都会触发状态变更：

- `10分钟` 批次发布
- 小时封口
- 回滚
- 删除
- 冷热索引转换
- 运维手动触发的发布、封口、删除、重建

其中：

- 发布与封口生成新的 `database_release`
- 回滚记录为新的 `release switch` 事件
- 删除也生成新的当前 `database_release`
- 删除事件本身不可回滚
- 所有仍保留的历史 `release` 共享同一全库保留边界
- 只有保留边界之后仍然完整存在的数据，才允许作为回滚目标

## 8. 删除与空间治理

### 8.1 删除来源

删除有两类来源：

- 外部容量治理模块按时间范围下发删除请求
- DBServer 自主触发删除

### 8.2 删除语义

删除策略统一为：

- 按数据时间范围删除
- 按完整 `UTC date/hour` 分区边界删除
- 仅删除已封口历史分区
- 全库统一保留边界
- 保留边界只允许单向前进
- 删除后数据彻底消失，不支持回滚
- 删除会同步裁剪受影响的历史 `release` 元数据

空间紧张时，DBServer 可直接删除 `current_release` 中最老的已封口分区，并记录：

- 删除范围
- 删除文件数
- 删除数量或行数估计
- 释放空间

### 8.3 双阈值自动回收

自主删数采用双阈值水位线模型：

- 低水位触发删除
- 高水位停止删除

删除优先级：

- 永远优先删除最老明细分区
- 不删除 `metadata.db`
- 不删除 `release journal`

### 8.4 删除与查询并发

删除时：

- 获取全局状态锁
- 阻止新查询进入目标分区
- 等待旧查询退场
- 再执行物理删除

删除前快照不作为可回滚版本保留，但为了保持版本模型稳定，删除后的可见数据集合仍映射为新的 `database_release`。

## 9. 查询能力

### 9.1 第一版支持的固定查询

第一版只支持固定查询能力：

- `device_id + time_range`
- `device_id + trace_id`
- `device_id + trace_id + time_range`
- `obj_type` 聚合查询

不支持对外直接执行任意 SQL。未来若开放任意 SQL，也仅限只读 `SELECT`，且允许分钟级响应。

### 9.2 查询一致性

- 每个查询在开始时绑定固定 `database_release`
- 查询全过程只读该快照
- 异步导出也绑定创建时的 `database_release`

### 9.3 查询资源治理

- 查询必须让位于接收、发布、删除主链路
- 重查询强限流
- 异步导出可串行或极低并发执行
- 大结果集查询走异步导出
- 异步导出第一版仅支持 `Parquet`

## 10. trace_index 设计

### 10.1 目标

`trace_index` 是为以下查询提供加速的轻量派生数据：

- `device_id + trace_id`
- `device_id + trace_id + time_range`

### 10.2 唯一性与返回语义

- `trace_id + event_time_ms` 仅在 `device_id` 范围内唯一
- 同一 `device_id + trace_id` 查询需返回全部命中的轨迹段列表
- 不带时间范围查询也必须支持
- 最近 `30天` 查询目标为秒级
- `30~180天` 查询允许 `10s` 级

### 10.3 轨迹段规则

同一 `device_id + trace_id` 下，按 `(event_time_ms, frame_id)` 排序处理。

判定为新轨迹段的条件：

- `frame_id` 回退或重置
- 相邻记录的 `event_time_ms` 间隔 `>150ms`

连续轨迹可跨：

- `10分钟` 批次
- `hour`
- `date`

查询结果必须自动拼接跨批次、跨小时的连续轨迹。

### 10.4 open / closed 状态

轨迹段允许出现 `open/incomplete` 状态。

闭合规则：

- 若出现新记录并满足分段条件，则前一段立即闭合
- 若没有后续记录，则只有当 `watermark > last_event_time_ms + 150ms` 时才闭合

### 10.5 热冷索引

- 热索引窗口：最近 `30天`
- 热索引目标：秒级查询
- 冷索引窗口：`30~180天`
- 冷索引形式：粗粒度候选 `file_group` 索引
- 热转冷方式：每日后台整批转换

`trace_index` 明确为可重建派生数据，不是唯一真相。

## 11. obj_type rollup 设计

### 11.1 目标

为了降低 `obj_type` 聚合查询对明细文件的直接扫描压力，第一版维护轻量 `rollup`。

### 11.2 粒度

第一版 rollup 的最小粒度为：

- `device_id`
- `lane_id`
- `10分钟 time_bucket`
- `obj_type`

约定：

- 空 `lane_id` 统一映射为 `255`

### 11.3 指标口径

rollup 同时维护：

- `box_count`
- `trace_count`

其中 `trace_count` 定义为：

- 一个 `10分钟` 桶内出现过的轨迹段计数
- 同一轨迹跨多个桶时，可在多个桶重复计数
- 同一轨迹跨多个 `lane_id` 时，可在多个 `lane` 桶重复计数

因此：

- `trace_count` 适合作为桶内活跃轨迹统计
- 不适合作为跨桶直接相加后的全局唯一轨迹数

`rollup` 明确为可重建派生数据。

## 12. 元数据与恢复

### 12.1 元数据存储

第一版接受使用持久化 DuckDB 文件保存元数据，例如：

- `metadata.db`

建议至少包含：

- `schema_versions`
- `batch_file_groups`
- `database_releases`
- `database_release_items`
- `current_release`

### 12.2 release journal

除 `metadata.db` 外，还需保留一份更小的追加写恢复记录：

- `release journal`

用途：

- 在 `metadata.db` 损坏时重建当前可见数据集合
- 重建时不会将已删除数据错误恢复

这份最小记录：

- 仅用于恢复
- 不作为审计系统
- 不作为对外可查询历史

### 12.3 崩溃恢复

若系统在发布、封口、删除过程中崩溃：

- 只有已提交到 `metadata.db` 且完成版本切换的数据才算正式生效
- 未提交的临时结果视为未发布
- 重启后清理 `staging/orphan` 文件并重做

## 13. 数据质量与异常处理

### 13.1 时间异常

若 `event_time_ms` 明显异常，例如：

- 落到异常年代
- 明显超前当前时间
- 相对接收时间偏差超出合理范围

则：

- 不进入正式 `release`
- 进入隔离区或记日志

### 13.2 帧内局部错误

若一帧内只有部分 `box` 解析失败或字段非法：

- 整帧拒收
- 不做“部分 box 继续入库”

这样可避免同一帧被拆成不完整快照。

## 14. 监控与运维

### 14.1 第一版监控范围

第一版只做硬指标监控，建议覆盖：

- 接收速率：帧/秒、box/秒、按 `device_id`
- 入库速率：每 `10分钟` 发布量、每小时封口量
- 积压：内存缓冲大小、待发布批次数、待封口小时数
- 空间：总占用、按 `box_info/trace_index/rollup/staging/metadata` 分项占用
- 删数：删除范围、删除文件数、删除行数估计、释放空间
- 异常：坏帧数、坏 box 数、异常时间戳数、发布失败次数、封口失败次数、删除失败次数

### 14.2 暴露方式

第一版只提供：

- 结构化日志
- 简单状态接口
- 简单指标接口

告警规则由外部系统完成。

### 14.3 运维入口

第一版需要受控运维入口，用于：

- 手动发布待发布批次
- 手动封口某小时
- 手动回滚到指定历史 `release`
- 手动重建 `trace_index`
- 手动重建 `rollup`
- 手动执行按分区删除

## 15. schema 演进策略

第一版不做通用 schema 演化框架。

发生 schema 变更时，采用以下策略：

- 离线重写旧数据
- 进入维护窗口
- 生成新的 `schema_version`
- 一次性切换到新版本

不允许：

- 在同一个 `database_release` 中混用多个 `schema_version`
- 新旧 schema 并存对外查询

## 16. 第一版非目标

以下内容明确不属于第一版目标：

- 多设备规模超过 `4` 台后的横向扩展
- 分布式写入与多实例并发发布
- 机器级或磁盘级高可用容灾
- 原始 payload 长期保存
- 行级更新、行级删除、业务纠正/撤回
- `2分钟` 窗口之外的历史回灌
- 严格入库去重
- 对外开放任意写 SQL
- 通用多表发布框架
- 复杂可观测平台

## 17. 推荐默认实现补充

以下是基于已确认边界收敛出的默认实现建议：

- 接收层按 `device_id` 建立独立缓冲队列
- 发布层按 `device_id + 10分钟窗口` 形成 `batch_file_group`
- 小时封口后，每个 `device_id/hour` 尽量收敛为单个最终 `Parquet`
- 查询入口内部先解析 `current_release`，再决定读明细、`trace_index` 还是 `rollup`
- 冷热索引转换、空间回收、重建任务统一放在后台串行调度器中执行
- 所有状态变化都写入结构化日志和 `release journal`

## 18. 总结

第一版 DBServer 的核心原则是：

- 明细数据不可变
- 对外数据面由 `current_release` 控制
- 查询只读受控快照
- 删除按分区、按边界、不可回滚
- 空间治理优先于历史完整保留
- 查询能力最小化，稳定入库与删数优先

在当前 `4台设备`、离线查询为主、`Parquet + DuckDB` 的约束下，上述方案能以较低复杂度满足数据接收、可见性发布、版本切换、历史回滚、空间保护和有限查询能力的目标。

## 附录 A. 实现级补充

本附录将上文的约束进一步收敛成可实现的模块、目录、元数据表和状态流，便于直接进入开发。

### A.1 推荐模块划分

建议将 DBServer 拆成以下模块：

- `adapters`
  - 协议适配层
  - 负责从 `DDS/cybernode` 或未来其他来源接收数据
  - 输出统一内部帧模型
- `validator`
  - 字段校验、时间戳校验、坏帧判定
  - 帧级拒收与隔离记录
- `ingest_buffer`
  - 按 `device_id` 管理内存缓冲
  - 维护开放中的 `10分钟` 批次窗口
- `batch_builder`
  - 将缓冲数据按 `(event_time_ms, frame_id, trace_id)` 排序
  - 生成待发布批次
- `publisher`
  - 写出正式 `Parquet`
  - 生成 `trace_index` 增量与 `rollup` 增量
  - 创建新的 `database_release`
- `hour_sealer`
  - 在小时封口时将多个 `10分钟` 批次合并为最终小时文件
- `metadata_store`
  - 维护 `metadata.db`
  - 维护 `current_release`
  - 持有全局发布锁
- `journal_store`
  - 追加写最小恢复记录
  - 定期 compact 成当前存活状态 checkpoint
- `query_service`
  - 受控查询入口
  - 解析 `current_release`
  - 执行明细查询、轨迹查询、聚合查询和异步导出
- `retention_manager`
  - 处理外部删除指令
  - 执行自主删数
  - 推进全库保留边界
- `rebuild_worker`
  - 重建 `trace_index`
  - 重建 `rollup`
  - 执行热索引转冷索引
- `ops_service`
  - 暴露受控运维接口

### A.2 推荐磁盘目录布局

建议采用如下磁盘布局：

```text
storage/
  live/
    box_info/
      date=YYYY-MM-DD/
        hour=HH/
          device_id=N/
            part-*.parquet
    trace_index_hot/
      date=YYYY-MM-DD/
        device_id=N/
          part-*.parquet
    trace_index_cold/
      date=YYYY-MM-DD/
        device_id=N/
          part-*.parquet
    obj_type_rollup/
      date=YYYY-MM-DD/
        hour=HH/
          device_id=N/
            part-*.parquet
  staging/
    box_info/
    trace_index/
    obj_type_rollup/
  metadata/
    metadata.db
    checkpoints/
    journal/
  quarantine/
    bad_frames/
    bad_timestamps/
  exports/
    pending/
    finished/
```

约定：

- `live/` 只放当前或历史 `release` 可能引用的正式文件
- `staging/` 只放尚未提交的临时结果
- `quarantine/` 用于隔离坏帧或异常时间数据
- `exports/` 仅用于异步导出结果，不参与 `database_release`

### A.3 推荐元数据表

建议 `metadata.db` 至少包含以下表。

#### A.3.1 schema_versions

记录 schema 契约。

建议字段：

- `schema_version_id`
- `created_at_ms`
- `table_name`
- `schema_json`
- `partition_rule`
- `sort_rule`
- `compression_codec`
- `is_current`

#### A.3.2 database_releases

记录对外可见的数据集合。

建议字段：

- `release_id`
- `parent_release_id`
- `schema_version_id`
- `created_at_ms`
- `event_type`
  - 可取值：`publish`、`seal`、`rollback`、`delete`
- `retention_floor_ms`
- `note`

#### A.3.3 file_groups

记录物理文件组。

建议字段：

- `file_group_id`
- `dataset_kind`
  - 可取值：`box_info`、`trace_index_hot`、`trace_index_cold`、`obj_type_rollup`
- `device_id`
- `date_utc`
- `hour_utc`
- `window_start_ms`
- `window_end_ms`
- `min_event_time_ms`
- `max_event_time_ms`
- `row_count`
- `file_count`
- `total_bytes`
- `state`
  - 可取值：`staging`、`published`、`sealed`、`garbage_pending`、`deleted`
- `path_list_json`
- `created_at_ms`

#### A.3.4 database_release_items

记录某个 `release` 引用了哪些 `file_group`。

建议字段：

- `release_id`
- `file_group_id`

#### A.3.5 partition_states

记录物理分区状态。

建议字段：

- `date_utc`
- `hour_utc`
- `device_id`
- `state`
  - 可取值：`open`、`publishable`、`sealing`、`sealed`、`deleting`、`deleted`
- `last_event_time_ms`
- `watermark_ms`
- `last_publish_release_id`
- `last_seal_release_id`

#### A.3.6 active_queries

记录活跃查询，辅助删除等待旧查询退场。

建议字段：

- `query_id`
- `release_id`
- `query_kind`
- `started_at_ms`
- `status`
- `target_range_start_ms`
- `target_range_end_ms`

说明：

- 该表允许作为运行时状态表存在
- 进程异常退出后可通过启动自检清理僵尸记录

### A.4 推荐 release journal 形式

`release journal` 不需要保留完整审计历史，但必须足够支撑恢复。

推荐采用：

- 追加写事件日志
- 定期 compact checkpoint

事件最小集合：

- `release_created`
- `current_release_switched`
- `retention_floor_advanced`
- `checkpoint_written`

恢复策略：

- 优先加载最近 checkpoint
- 再回放 checkpoint 之后的 journal 事件
- 恢复当前存活 `release`、`file_group` 与保留边界

被删除数据不要求在 journal 中保留完整可查询历史，只要求不会在恢复时被错误复活。

### A.5 状态机建议

#### A.5.1 file_group 状态

```text
staging -> published -> sealed -> garbage_pending -> deleted
```

约定：

- `published` 用于开放小时内的 `10分钟` 文件组
- `sealed` 用于小时封口后的最终文件组
- `garbage_pending` 表示逻辑上已不再被当前版本使用，但仍需等待查询退场或后台清理

#### A.5.2 分区状态

```text
open -> publishable -> sealing -> sealed -> deleting -> deleted
```

关键条件：

- `publishable`：`watermark` 已越过某个 `10分钟` 窗口尾部
- `sealing`：`hour_end + 2min` 后开始小时封口
- `deleting`：删除锁已拿到，且不再允许新查询进入该分区

### A.6 关键流程

#### A.6.1 接收与发布流程

建议流程：

1. 接入层收到一帧数据
2. `validator` 完成字段校验与时间校验
3. 若校验失败，整帧写隔离区并记日志
4. 若校验成功，按 `device_id` 写入 `ingest_buffer`
5. `batch_builder` 定时检查 `watermark`
6. 对已可发布的 `10分钟` 窗口，排序并写 `staging`
7. `publisher` 写出以下正式文件：
   - `box_info` 明细
   - `trace_index` 增量
   - `obj_type rollup` 增量
8. `metadata_store` 在事务中：
   - 创建 `file_group`
   - 创建新的 `database_release`
   - 更新 `current_release`
9. `journal_store` 追加最小恢复事件
10. 后台异步回收不再引用的旧 `file_group`

#### A.6.2 小时封口流程

建议流程：

1. 到达 `hour_end + 2min`
2. 锁住目标 `hour/device_id`
3. 收集该小时所有 `published` 的 `10分钟 file_group`
4. 合并生成最终小时 `Parquet`
5. 重算该小时对应的 `trace_index` 与 `rollup` 最终文件
6. 生成新的 `seal` 类型 `database_release`
7. 将旧 `10分钟 file_group` 标记为 `garbage_pending`

#### A.6.3 回滚流程

建议流程：

1. 运维指定目标 `release_id`
2. 校验目标 `release` 仍在当前保留边界之后且完整可用
3. 获取全局发布锁
4. 创建一条新的 `rollback` 事件
5. 将 `current_release` 切换到目标数据集合
6. 写入 journal

说明：

- 回滚本身是新的版本切换事件
- 不直接修改历史 `release` 定义

#### A.6.4 删除流程

建议流程：

1. 收到外部删除指令，或空间达到低水位
2. 计算新的全库 `retention_floor_ms`
3. 获取全局发布锁
4. 阻止新查询进入目标分区
5. 等待旧查询退出
6. 删除所有早于保留边界、且已封口的历史分区
7. 生成新的 `delete` 类型 `database_release`
8. 裁剪不再可用的历史 `release` 元数据
9. 推进 journal/checkpoint 中的保留边界
10. 记录删除日志并释放锁

### A.7 查询执行建议

#### A.7.1 明细查询

`device_id + time_range` 查询建议流程：

1. 固定 `current_release`
2. 从 release manifest 获取命中的 `box_info file_group`
3. 只拼接目标时间范围与 `device_id` 对应文件
4. 用 DuckDB 执行只读查询

#### A.7.2 trace 查询

`device_id + trace_id` 查询建议流程：

1. 固定 `current_release`
2. 优先查热索引
3. 若未命中，再查冷索引
4. 根据候选 `file_group` 回查明细
5. 自动拼接跨批次、跨小时、跨天的连续轨迹
6. 对尾段标记 `open/incomplete`

#### A.7.3 obj_type 聚合查询

建议流程：

1. 固定 `current_release`
2. 直接读取对应时间桶与 `device_id/lane_id` 的 `rollup`
3. 若查询粒度大于 `10分钟`，在查询层二次聚合
4. 不对跨桶 `trace_count` 做“唯一轨迹数”误导性解释

#### A.7.4 异步导出

建议流程：

1. 创建导出任务时固定 `release_id`
2. 计算目标文件列表
3. 排队执行，限制并发
4. 输出 `Parquet` 到 `exports/finished/`
5. 结果文件不纳入 release 生命周期

### A.8 保护模式建议

建议定义以下保护模式：

- `disk_guard_mode`
  - 触发条件：空间低于安全阈值
  - 动作：停止新的重查询与导出；优先执行自主删数
- `metadata_guard_mode`
  - 触发条件：`metadata.db` 或 `journal` 不可写
  - 动作：停止新发布与封口，只保留只读查询或直接降级只读状态
- `backpressure_mode`
  - 触发条件：缓冲区持续高水位
  - 动作：对接入层施加反压，暂停消费

保护模式目标不是保证业务连续可用，而是优先保证数据面不被破坏。

### A.9 开发里程碑建议

建议按以下顺序落地：

#### M1. 基础接收与正式入库

- 接入适配层
- 帧级校验
- 内存缓冲
- `10分钟` 发布
- `date/hour/device_id` 正式 `Parquet`

#### M2. 元数据与版本切换

- `metadata.db`
- `current_release`
- `batch_file_group`
- `release journal`
- 小时封口

#### M3. 查询最小闭环

- `device_id + time_range`
- `device_id + trace_id`
- `device_id + trace_id + time_range`
- `obj_type rollup`

#### M4. 空间治理与恢复

- 外部删除指令
- 自主删数
- 双阈值水位
- 活跃查询等待删除
- journal/checkpoint 恢复

#### M5. 运维能力

- 手动发布
- 手动封口
- 手动回滚
- 手动重建索引
- 手动删除

## B. 当前代码骨架与试跑方式

当前仓库已经落下的重点是第一版主链路：

- `jsonl -> 帧级校验 -> 内存缓冲 -> box_info parquet 发布`
- `staging -> live` 的正式文件提升流程
- `metadata.db + current_release + release journal`
- `disk-usage` 空间统计
- `delete-before` 按时间边界删数
- `enforce-watermarks` 按双阈值自动删数
- 坏帧隔离到 `quarantine/`
- 结构化运行日志输出到 `metadata/runtime_events.jsonl`

### B.1 CLI 入口

主要入口位于：

- `src/dbserver/cli.py`

当前可用命令：

- `show-layout`
- `init-metadata`
- `current-release`
- `list-partitions`
- `disk-usage`
- `ingest-jsonl`
- `run-ingest-loop`
- `serve`
- `plan-detail-query`
- `exec-detail-query`
- `plan-trace-query`
- `exec-trace-query`
- `seal-ready-hours`
- `seal-hour`
- `plan-delete`
- `delete-before`
- `enforce-watermarks`
- `run-maintenance`

### B.2 JSONL 输入格式

可参考样例文件：

- `examples/sample_box_frames.jsonl`

每一行代表一帧，格式如下：

```json
{
  "device_id": 1,
  "frame_id": 1001,
  "event_time_ms": 1711922400100,
  "boxes": [
    {
      "trace_id": 42,
      "obj_type": 1,
      "position_x_mm": 1200,
      "position_y_mm": 3400,
      "position_z_mm": 0,
      "length_mm": 4500,
      "width_mm": 1800,
      "height_mm": 1600,
      "speed_centi_kmh_100": 1250,
      "spindle_centi_deg_100": 9050,
      "lane_id": 2
    }
  ]
}
```

说明：

- `lane_id` 允许缺失或为 `null`，入库前会被统一映射为 `255`
- `event_time_ms` 必须是毫秒时间戳
- 同一帧内的 `boxes` 会继承顶层的 `device_id / frame_id / event_time_ms`

### B.3 运行示例

当前代码依赖 `duckdb` Python 包来完成：

- `metadata.db` 初始化
- Parquet 写出

安装依赖后，可按以下顺序进行 smoke test：

```bash
python3 -m pip install -e '.[duckdb]'
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test init-metadata
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test ingest-jsonl --input examples/sample_box_frames.jsonl
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test serve --input-dir ./storage_test/inbox --poll-interval-seconds 1 --max-runtime-seconds 5
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test exec-detail-query --device-id 1 --start-ms 1711922400000 --end-ms 1711923600000
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test exec-trace-query --device-id 1 --trace-id 42 --start-ms 1711922400000 --end-ms 1711923600000
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test list-partitions
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test seal-ready-hours
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test current-release
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test disk-usage --low-watermark-bytes 104857600 --high-watermark-bytes 52428800
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test plan-delete --before-ms 1711926000000
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test enforce-watermarks --low-watermark-bytes 104857600 --high-watermark-bytes 52428800
PYTHONPATH=src python3 -m dbserver.cli --root ./storage_test run-maintenance --low-watermark-bytes 104857600 --high-watermark-bytes 52428800
```

### B.4 当前实现边界

当前已实现：

- `box_info` 正式文件发布
- 发布与封口先写 `staging/`，再提升到 `live/`
- 分区状态跟踪：`OPEN -> PUBLISHABLE -> SEALED -> DELETED`
- `ready hour` 封口与单小时手动封口
- 当前 release 切换
- 最小长生命周期服务进程 `serve`
- `device_id + time_range` 明细查询执行
- `device_id + trace_id (+ 可选 time_range)` 明细回查执行
- 运行日志
- 坏帧隔离
- 启动时清理 `staging/` 孤儿文件
- 目录轮询式批处理入口 `run-ingest-loop`
- 双阈值删数框架
- 基于 `active_queries` 的删数阻塞保护
- `seal + retention` 串行维护入口

当前尚未实现：

- `trace_index` 生成与冷热转换
- `obj_type rollup` 生成
- 服务级 supervisor / 多线程模型 / 反压调度
- 接入 `DDS/cybernode`

补充说明：

- 当前实现里，`ingest-jsonl / seal-ready-hours / seal-hour / run-maintenance` 启动时会清理 `staging/` 下的残留临时文件，并输出 `recovered_staging_orphans`
- 当前实现里，若发现 `live/` 下存在未被 `metadata.db` 引用的 `Parquet`，只会探测并输出 `detected_live_orphans`，不会自动删除
- 当前实现里，`ingest-jsonl` 会按 `device_id` 维护“最大已见 event_time_ms”，超出 `2分钟` 晚到窗口的整帧会被拒收并写入 `quarantine/`
- 当前实现里，若出现超过 `max_devices=4` 的新设备帧，该帧会被拒收并写入 `quarantine/`，不会中断整次导入
- 当前实现里，`run-ingest-loop` 会按文件名排序轮询输入目录，将成功文件移动到 `processed/`，解析失败文件移动到 `failed/`
- 当前实现里，`run-ingest-loop` 默认只做接收与发布；传入 `--run-maintenance` 或双水位参数后，会在每轮末尾继续推进封口/删数
- 当前实现里，`serve` 是单进程常驻入口：按目录轮询接收、周期发布、可选周期维护，并在 `SIGINT/SIGTERM` 下优雅退出
- 当前实现里，`serve` 会在停止前完成当前已开始处理的文件，并继续尝试发布已到 watermark 的窗口；不会强制刷出尚未到发布边界的 `OPEN` 窗口
- 当前实现里，`serve` 可通过 `--maintenance-interval-seconds` 控制封口/删数维护周期；启用维护后首轮即会执行一次维护检查
- 当前实现里，删数会先将目标分区推进到 `DELETING`，若 `active_queries` 中仍有冲突查询，则删除会被阻塞并回退分区状态
- 当前实现里，`delete-before / enforce-watermarks / run-maintenance` 支持查询等待参数；默认超时为 `0`，即发现冲突查询后立即阻塞返回
- 当前实现里，`exec-detail-query` 会在执行前向 `active_queries` 注册 `detail` 查询，执行结束后清理该记录
- 当前实现里，`exec-trace-query` 会在执行前向 `active_queries` 注册 `trace` 查询；在 `trace_index` 尚未实现前，先扫描命中的 `box_info` 明细文件回查
- 当前实现里，`delete-before / enforce-watermarks` 只会删除 `SEALED` 分区，不会删除尚未封口的发布批次
- 当前实现里，`seal-ready-hours / seal-hour` 会主动淘汰被替换掉的 `10分钟` 批次文件及其元数据，不保留 pre-seal 的临时发布版本
- 当前实现里，`list-partitions` 读取的是 `metadata.db` 中的运行态分区状态，不是通过扫目录现算
- 当前实现里，`run-maintenance` 的执行顺序固定为：先封口，再按水位删数
- 当前实现里，同一 `--root` 下的 CLI 命令应串行执行；若并发打开同一个 `metadata.db`，DuckDB 可能返回文件锁冲突
- 使用极小样例数据做 smoke test 时，`metadata.db` 的固定页开销可能显著大于业务数据文件，`disk-usage` 的总空间变化不适合作为压缩率判断依据
