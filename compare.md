# 技术路线比较结论

本文比较当前仓库里的三条路线：

- `traj_store`：以 `parquet_compact_zstd` 为主路径，核心形态是 `sender -> receiver -> Parquet lake`
- `traj_store_clickhose`：以 `clickhouse_compact_zstd` 为主路径，核心形态是 `sender -> receiver -> ClickHouse MergeTree`
- `traj_store_pg`：以 `pg_tdengine_compact + postgres_profile=bench_low_wal_compressed` 为新基线，核心形态是 `sender -> receiver -> TDengine facts + PostgreSQL hot state`

## 结论先行

如果目标是“用尽量低的运行时复杂度，把主落盘稳定控制在 GiB/day 量级，并且在磁盘紧张时按时间窗口或版本快速删数据”，优先选 `traj_store`。

如果目标是“在持续 ingest 的同时，直接得到一个可在线查询的列式数据库，并且希望主数据体积尽量小”，优先选 `traj_store_clickhose`。

`traj_store_pg` 的新基线比旧 PG 基线已经明显收敛，但从当前 benchmark 看，它仍不是第一优先级技术路线。它的适用前提是你明确需要“TDengine 承接全量时序事实 + PostgreSQL 承接控制面和热状态”这种双库分工；否则它会同时带来更高的容量成本和更高的运维复杂度。

如果明确把“磁盘水位触发热删除”当成核心需求，当前阶段仍然更建议 `traj_store`。三条路线里，它的删除边界最直观，空间回收预期也最稳定。

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
- whole `clickhouse_store/`：`60,831,181 B`，约 `17.519 GB/day`

`traj_store_pg` 新基线 5 分钟样本：

- 样本目录：`traj_store_pg/data/benchmarks/ingest_pg_low_wal_compact_5min/`
- 5 分钟样本：`600,000` 行，`postgres_profile = bench_low_wal_compressed`
- `tdengine_data_bytes_post_run = 34,128,431 B`，`56.881 B/row`
- `postgres_data_bytes_post_run = 31,360,399 B`，`52.267 B/row`
- `sink_bytes_post_run = 68,236,760 B`，`113.728 B/row`
- 一天外推：
  - TDengine facts：约 `9.829 GB/day`
  - PostgreSQL 热状态和批次台账目录：约 `9.032 GB/day`
  - whole sink：约 `19.652 GB/day`

需要单独强调的是：

- `traj_store_clickhose` 的主容量估算应优先看业务表 active parts，而不是整个 `clickhouse_store/`
- `traj_store_pg` 的主容量估算应优先看 `tdengine_data_bytes_post_run`；`postgres_data_bytes_post_run` 和 `sink_bytes_post_run` 更像“整条双库路线的额外代价”

## 关键数字并列

### 1. 主数据净落盘

| 路线 | README / benchmark 引用样本 | 5 分钟写入行数 | 主落盘口径 | 每行字节 | 一天外推 |
| --- | --- | ---: | ---: | ---: | ---: |
| `traj_store` `parquet_compact_zstd` | `compact_5min_10fps_200box` | `399,000` | `3,808,873 B` | `9.546 B/row` | `1.650 GB/day` |
| `traj_store_clickhose` `clickhouse_compact_zstd` | `ingest_20260327T014603Z` | `600,000` | `5,053,000 B` active parts | `8.422 B/row` | `1.455 GB/day` |
| `traj_store_pg` `pg_tdengine_compact` | `ingest_pg_low_wal_compact_5min` | `600,000` | `34,128,431 B` TDengine facts | `56.881 B/row` | `9.829 GB/day` |

直接读这个表，结论很清楚：

- 如果只比“主数据净落盘”，当前最好的是 `traj_store_clickhose`
- `traj_store` 与 ClickHouse active parts 很接近，只大约 `13.4%`
- `traj_store_pg` 新基线即使只看 TDengine facts，也约是 `traj_store` 的 `5.96x`，约是 ClickHouse active parts 的 `6.76x`

### 2. 整条路线的目录总代价

| 路线 | 整目录口径 | 每行字节 | 一天外推 | 备注 |
| --- | ---: | ---: | ---: | --- |
| `traj_store` | 近似等于主 Parquet 文件集 | `9.546 B/row` | `1.650 GB/day` | 没有独立数据库服务数据目录 |
| `traj_store_clickhose` | whole `clickhouse_store/` | `101.385 B/row` | `17.519 GB/day` | 混入 `system.*`、metadata、旧 parts |
| `traj_store_pg` | whole sink | `113.728 B/row` | `19.652 GB/day` | 同时包含 TDengine facts、PostgreSQL 目录和日志 |

这里的排序也很明确：

- `traj_store` 仍然是总目录最省的一条路线
- `traj_store_pg` 新基线虽然比旧 PG 基线明显下降，但 whole sink 仍比 ClickHouse whole store 大约 `12.2%`
- `traj_store_pg` 的 PostgreSQL 目录本身就有约 `9.032 GB/day`，这部分是另外两条主路线没有的显式额外代价

## 核心比较

### 1. 吞吐与写入稳定性

从当前样本看，`traj_store_clickhose` 和 `traj_store_pg` 新基线都已经基本跑满 nominal `2,000 rows/s`。

- `traj_store_clickhose`：`avg_rows_per_second_ingest = 1,999.60`
- `traj_store_pg` 新基线：`avg_rows_per_second_ingest = 1,999.59`

`traj_store` README 当前引用样本没有跑满 nominal `2,000 rows/s`，所以如果你的首要目标是“先把持续 ingest 打满”，当前更接近同口径满载的是 ClickHouse 和 PG 双库路线。

但这里仍要注意：

- 三份 README / benchmark 引用样本实际写入行数并不完全一致
- 更适合比较趋势、单位行体积和运行时代价，不适合把三份短时样本当成完全严格的 A/B

### 2. 容量与落盘效率

只看主数据净落盘：

- `traj_store_clickhose` 最优：`8.422 B/row`
- `traj_store` 次之：`9.546 B/row`
- `traj_store_pg` 新基线的 TDengine facts 明显更大：`56.881 B/row`

也就是说，PG 新基线虽然已经优化过 PostgreSQL 侧口径，但它并没有改变“TDengine facts 主体体积远大于 Parquet / ClickHouse active parts”这个事实。

如果把整条路线都算进去：

- `traj_store` 的文件集大小基本就是主结果
- `traj_store_clickhose` 的 whole store 约 `17.519 GB/day`
- `traj_store_pg` 的 whole sink 约 `19.652 GB/day`

所以从容量角度看：

- 主数据层：`traj_store_clickhose` 最好，`traj_store` 很接近，`traj_store_pg` 明显落后
- 整条路线：`traj_store` 最好，`traj_store_clickhose` 次之，`traj_store_pg` 仍然最重

### 3. 资源占用

`traj_store` 仍然是最轻的运行形态，因为没有常驻数据库服务。

在两条数据库路线里，当前样本呈现的是：

- `traj_store_clickhose`：ClickHouse server 峰值 RSS 约 `822 MiB`，再加上 receiver 约 `36.5 MiB`
- `traj_store_pg` 新基线：PostgreSQL + TDengine + receiver 合计峰值 RSS 约 `592 MiB`

所以从这次样本看，`traj_store_pg` 新基线的总 RSS 低于 `traj_store_clickhose`，大约是后者的 `69%`。但这里不能只看 RSS，因为它的代价是：

- 不是一套服务，而是两套数据库
- 磁盘体积仍明显高于 ClickHouse active parts 和 Parquet 主路径
- 保留、删除、备份和观测都要跨 PostgreSQL / TDengine 两边协同

### 4. 查询与下游能力

`traj_store` 更适合做事实主存储和离线交换层。

- 产物是 Parquet 文件，天然适合后续喂给 DuckDB、Spark、ClickHouse 或对象存储
- 但它自己不是在线查询服务

`traj_store_clickhose` 更适合把“存储 + 查询”合在一起。

- 写入完成后即可直接 SQL 查询
- 数据和查询都在同一套系统里
- 当前三条路线里，它最像一条完整的“可持续 ingest 的在线分析库”

`traj_store_pg` 更像职责拆分后的双库路线。

- TDengine 管全量时序事实
- PostgreSQL 管热状态和批次台账
- 这种拆分对架构边界是清楚的，但也意味着查询、保留和运维不会像单库方案那样顺手

所以如果你的目标是“写完立刻查，而且尽量留在同一套系统里查”，`traj_store_clickhose` 仍明显优于 `traj_store_pg`。

### 5. 热删除与空间回收

如果把“磁盘空间不足时按规则删除部分数据”作为重点需求，当前仍推荐 `traj_store`。

原因有三点：

- `traj_store` 的主数据本身就是按 `date/hour` 分区的文件集，删除边界天然清楚
- 当前项目已经围绕这条路线做了 `version directory + active pointer` 机制
- 删除文件后，空间回收预期直接，不依赖后台 merge、compaction 或多引擎协调

`traj_store_clickhose` 次之。

- 它按时间分区，做基于时间窗口的删除是顺手的
- 但空间回收会和 parts、merge、系统表目录一起表现，不如文件删除直观

`traj_store_pg` 在这件事上最复杂。

- 一份数据路线拆在 PostgreSQL 和 TDengine 两边
- 热状态、批次台账和全量事实需要分别考虑回收
- 当前项目只覆盖 ingest benchmark，还没有完整删除治理能力

因此，如果“热删除 + 明确的空间回收预期”是核心约束，`traj_store_pg` 不应是优先路线。

## 三条路线的适用建议

建议优先选 `traj_store` 的情况：

- 你把主目标定义为“低复杂度主落盘”
- 你更关心可搬运、可归档、可离线重算
- 你希望磁盘水位触发删除时，行为简单、边界清楚、空间回收稳定
- 你不希望长期维护数据库服务

建议优先选 `traj_store_clickhose` 的情况：

- 你已经明确需要在线查询能力
- 你希望写入后数据立即在数据库里可分析
- 你希望主数据净落盘尽量小
- 你愿意接受更高的 CPU / RSS / 运维复杂度

建议只在特定前提下考虑 `traj_store_pg` 的情况：

- 你明确想把“全量时序事实”和“热状态 / 控制面”拆到两套引擎
- 你接受 TDengine + PostgreSQL 双库协同
- 你接受它当前容量明显大于另外两条主路线
- 你看重的是架构分工，而不是单纯的容量最优或删除最简单

## 最终建议

站在当前仓库现状上，不是抽象讨论数据库能力，而是基于现有实现成熟度和 benchmark 结果来判断：

- 把“主落盘 + 容量控制 + 热删除”作为核心目标时，优先用 `traj_store`
- 把“持续 ingest + 在线分析 + 同库查询”作为核心目标时，优先推进 `traj_store_clickhose`
- `traj_store_pg` 新基线可以作为一条备选架构路线继续保留，但当前不建议把它升成第一优先级主路线

一句话概括：`traj_store` 最稳，`traj_store_clickhose` 最像在线分析主库，`traj_store_pg` 新基线虽然比旧 PG 基线收敛明显，但目前仍更像“有特定架构诉求时才值得承担的双库方案”。
