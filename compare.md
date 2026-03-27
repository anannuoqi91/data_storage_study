# 技术路线比较结论

本文只比较当前仓库里的两条主路线：

- `traj_store`：以 `parquet_compact_zstd` 为主路径，核心形态是 `sender -> receiver -> Parquet lake`
- `traj_store_clickhose`：以 `clickhouse_compact_zstd` 为主路径，核心形态是 `sender -> receiver -> ClickHouse MergeTree`

## 结论先行

如果目标是“用尽量低的运行时复杂度，稳定把主落盘控制在 GiB/day 量级，并且在磁盘紧张时按时间窗口或版本快速删数据”，优先选 `traj_store`。

如果目标是“在持续 ingest 的同时，直接得到一个可在线查询的列式数据库，并愿意接受更高的资源占用、运维复杂度和容量口径复杂度”，可以选 `traj_store_clickhose`。

如果明确把“磁盘水位触发热删除”当成核心需求，当前阶段仍然更建议 `traj_store`。原因不是 ClickHouse 不能删，而是 `traj_store` 的删除边界更直观、空间回收预期更稳定、配套机制也更完整。

## 当前 benchmark 依据

`traj_store` README 当前引用样本：

- 样本目录：`traj_store/data/benchmarks/compact_5min_10fps_200box/`
- 5 分钟样本：`399,000` 行，`3,808,873 B`
- 单位行大小：`9.546 B/row`
- 一天外推：约 `1.650 GB/day`，约 `1.536 GiB/day`

`traj_store_clickhose` 当前 5 分钟样本：

- 样本目录：`traj_store_clickhose/data/benchmarks/ingest_20260327T014603Z/`
- 5 分钟样本：`600,000` 行，`active_bytes_on_disk_post_run = 5,053,000 B`
- 单位行大小：`8.422 B/row`
- 一天外推：约 `1.455 GB/day`，约 `1.355 GiB/day`

需要单独强调的是，`traj_store_clickhose` 的 `clickhouse_data_bytes_post_run = 60,831,181 B` 不能直接当主数据体积外推。这个口径包含整个 `clickhouse_store/` 数据目录，会混入：

- `system.*` 日志表
- metadata / access / preprocessed config 等系统目录
- merge 尚未清理掉的旧 parts

所以 ClickHouse 的主容量估算应优先看业务表 active parts，而不是整个数据目录。

## 核心比较

### 1. 主存储形态

`traj_store` 的本质是文件湖。

- 主结果是按 `date/hour` 分区的 Parquet 文件
- 数据形态简单，跨工具可读
- 更像“稳定落盘层”，不是常驻数据库服务

`traj_store_clickhose` 的本质是数据库实例。

- 主结果是 ClickHouse `MergeTree` 表
- 数据写入后立即处在可查询数据库中
- 不只是存储层，还天然带着在线查询和后台 merge 行为

### 2. 吞吐与写入稳定性

从当前样本看，`traj_store_clickhose` 在 ingest 吞吐上更强。

- 当前 5 分钟样本基本跑满 nominal `2,000 rows/s`
- `traj_store` README 当前样本没有跑满 nominal `2,000 rows/s`

这说明，如果你的首要目标是“先把持续写入稳定性打满”，ClickHouse 路线更有优势。

但这里也要保持边界清晰：

- 当前两边 README 引用的 5 分钟样本实际写入行数不同
- 因此更适合比较趋势和单位行体积，不适合把两份短时样本当成完全同口径的严格 A/B

### 3. 主数据容量

只看主数据净落盘，当前 ClickHouse 样本略优于 Parquet 样本。

- `traj_store`：`9.546 B/row`
- `traj_store_clickhose` active parts：`8.422 B/row`

也就是说，当前 ClickHouse active parts 样本比 README 里的 Parquet 样本小约 `11.8%`。

但如果看“整个运行时目录”，结论会反过来：

- Parquet 路线的目录大小基本就是主数据大小
- ClickHouse 路线的整目录大小会显著大于主数据，因为内部还带着系统表、元数据和旧 parts

所以两条路线的容量口径不能混着看：

- `traj_store` 可以直接看文件集大小
- `traj_store_clickhose` 必须区分“业务表 active parts”和“整个 ClickHouse 数据目录”

### 4. 资源占用

当前阶段，`traj_store` 更轻，`traj_store_clickhose` 更重。

`traj_store` 的优势：

- 没有独立数据库服务常驻
- receiver 直接刷 Parquet，进程模型简单
- 对单机资源更友好

`traj_store_clickhose` 的代价：

- 除了 Python receiver，还要常驻 ClickHouse server
- 当前 5 分钟样本里 ClickHouse server 峰值 RSS 约 `822 MiB`
- 后台 merge、日志表、系统元数据都会带来额外资源和磁盘开销

如果你的部署环境更接近“边缘机、单机、资源紧张”，`traj_store` 更稳。

### 5. 查询与下游能力

`traj_store` 更适合做事实主存储和离线交换层。

- Parquet 文件天然适合后续喂给 DuckDB、Spark、ClickHouse、对象存储
- 但它自己不是查询服务
- 如果要做在线分析、并发查询或二级索引，通常需要再挂一层引擎

`traj_store_clickhose` 更适合把“存储 + 查询”合在一起。

- 写入完成后即可直接 SQL 查询
- 内建 `system.parts`、`query_log`、`metric_log` 等观测能力
- 对需要快速做在线分析的场景更友好

所以如果你的目标是“写完立刻查，而且查的主体就在同一套系统里”，ClickHouse 路线更顺手。

### 6. 热删除与空间回收

如果把“磁盘空间不足时按规则删除部分数据”作为重点需求，当前更推荐 `traj_store`。

原因有三点：

- `traj_store` 当前已经采用 `version directory + active pointer` 机制，可以先切换 active 版本，再回收旧版本目录
- `traj_store` 的主数据本身就是按 `date/hour` 分区的文件集，删除边界天然清楚
- 删除文件后，空间回收预期直接，不依赖后台 merge 或 mutation 收敛

相对地，`traj_store_clickhose` 虽然也按小时分区，适合做基于时间分区的删除，但当前仓库只覆盖 ingest benchmark，还没有把完整的 `events` / `hot cache` / 保留策略都迁进来。并且在磁盘紧张场景下，ClickHouse 的空间回收会和 parts、merge、系统表目录一起形成更复杂的运行时行为。

结论不是 ClickHouse 不能删，而是：

- 如果你的删除规则主要是按时间窗口、按版本、按整批数据回收
- 并且你需要对“什么时候真的腾出空间”有更强确定性

那么 `traj_store` 更合适。

如果你的删除规则是复杂的在线局部删除，而且删完后仍然希望同一套系统继续承担读写查询，那才值得为此承担 ClickHouse 方案的复杂度。

## 两条路线的适用建议

建议优先选 `traj_store` 的情况：

- 你把主目标定义为“低复杂度主落盘”
- 你更关心可搬运、可归档、可离线重算
- 你希望磁盘水位触发删除时，行为简单、边界清楚、空间回收稳定
- 你的部署资源有限，不希望再长期维护一个数据库服务

建议考虑 `traj_store_clickhose` 的情况：

- 你已经明确需要在线查询能力
- 你希望写入后数据立即在数据库里可分析
- 你愿意接受更高的 CPU / RSS / 运维复杂度
- 你后续愿意继续补完整的保留、删除、hot cache 和运维策略

## 最终建议

站在当前仓库现状上，不是抽象讨论数据库能力，而是基于现有实现成熟度来判断：

- 把“主落盘 + 容量控制 + 热删除”作为核心目标时，优先用 `traj_store`
- 把“持续 ingest + 在线分析”作为核心目标时，再推进 `traj_store_clickhose`

换句话说，`traj_store` 更像当前阶段的稳态主路线，`traj_store_clickhose` 更像有明确上行空间、但仍需要继续补齐运行时治理能力的增强路线。
