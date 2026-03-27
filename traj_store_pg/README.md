# PostgreSQL + TDengine Box Ingest Benchmark

`traj_store_pg` 延续当前仓库的 `sender -> receiver -> sink` 压测方式，但把 sink 拆成两层：

- TDengine：承接全量 `box_info` 时序事实数据
- PostgreSQL：承接批次台账和 `trace_latest_state` 热状态表

这个项目的目标不是证明某个数据库“绝对更快”，而是在与 `traj_store` / `traj_store_clickhose` 尽量一致的 workload 下，回答下面几个问题：

- PostgreSQL + TDengine 组合能否稳定承接 `10 fps * 200 box/frame` 这类持续 ingest
- 把全量时序事实放进 TDengine、把热状态放进 PostgreSQL 后，整体资源和磁盘占用大概是什么量级
- 这种“双写 route” 是否比单纯文件湖或单一列式库更适合作为后续技术路线候选

当前范围只覆盖 ingest benchmark，不包含 `events` / `hot cache` / 删除策略的完整工程化迁移。

## 当前脚本

- `scripts/06_box_sender.py`
  - 按帧持续生成并发送 `box` 批次
- `scripts/07_pg_tdengine_receiver.py`
  - 单连接、同步收包
  - 按 flush 批量写入 TDengine supertable
  - 同步写 PostgreSQL 的 `ingest_batches` 和 `trace_latest_state`
  - 记录 Python receiver、PostgreSQL、TDengine 三侧 CPU / RSS / 吞吐 / 落盘大小
- `scripts/08_run_ingest_benchmark.py`
  - 自动起停 PostgreSQL / TDengine Docker 容器
  - 跑单个布局的 sender / receiver 压测
- `scripts/09_compare_storage_layouts.py`
  - 顺序跑多种布局并输出对比结果
- `scripts/03_queries.sql`
  - PostgreSQL / TDengine 常用检查 SQL 示例
- `scripts/tdengine_http.py`
  - TDengine REST API 的最小客户端
- `scripts/ingest_common.py`
  - benchmark 公共配置、数据生成和产物目录管理

## Docker 目录

`docker/` 目录补齐了镜像获取、手工起库、初始化和停止脚本：

- `docker/00_pull_images.sh`
  - 拉取 `postgres:17` 和 `tdengine/tdengine`
- `docker/01_run_postgres.sh`
  - 启动 PostgreSQL 容器
- `docker/02_wait_postgres_ready.sh`
  - 等待 PostgreSQL ready
- `docker/03_init_postgres.sh`
  - 应用 PostgreSQL 表结构
- `docker/04_stop_postgres.sh`
  - 停止 PostgreSQL 并修正目录权限
- `docker/11_run_tdengine.sh`
  - 启动 TDengine 容器
- `docker/12_wait_tdengine_ready.sh`
  - 等待 TDengine ready
- `docker/13_init_tdengine.sh`
  - 创建 TDengine 数据库和 supertable
- `docker/14_stop_tdengine.sh`
  - 停止 TDengine 并修正目录权限
- `docker/sql/postgres/01_create_tables.sql`
  - PostgreSQL 默认基线初始化 SQL
- `docker/sql/postgres/02_create_tables_bench_low_wal.sql`
  - PostgreSQL benchmark-only 低 WAL / 压缩口径初始化 SQL
- `docker/sql/tdengine/01_create_database.sql`
  - TDengine 建库 SQL
- `docker/sql/tdengine/02_create_layout_tables.sql`
  - TDengine raw/compact supertable SQL

这些脚本主要用于手工 bring-up、独立排查和后续 smoke test；benchmark runner 仍然保留自动起停容器能力。

## Schema 目录

`schema/` 目录补齐了数据库和数据表 schema：

- `schema/postgres/0001_init/up.sql`
  - 创建 `ingest_batches` 和 `trace_latest_state`
- `schema/postgres/0001_init/down.sql`
  - 删除 PostgreSQL 基础表
- `schema/postgres/0001_init/manifest.json`
  - PostgreSQL schema 元数据
- `schema/tdengine/0001_init/up.sql`
  - 创建 TDengine database、`box_info_raw`、`box_info_compact`
- `schema/tdengine/0001_init/down.sql`
  - 删除 TDengine database
- `schema/tdengine/0001_init/manifest.json`
  - TDengine schema 元数据

## 运行前提

- 本机可用 `docker`
- Python 环境里安装 `psutil` 和 `psycopg`
- runner 默认使用官方镜像：
  - PostgreSQL：`postgres:17`
  - TDengine：`tdengine/tdengine`

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r traj_store_pg/requirements.txt
```

## 手工起库

如果你要先单独验证数据库和 schema，可以按这个顺序执行：

```bash
traj_store_pg/docker/00_pull_images.sh
traj_store_pg/docker/01_run_postgres.sh
traj_store_pg/docker/02_wait_postgres_ready.sh
traj_store_pg/docker/03_init_postgres.sh
traj_store_pg/docker/11_run_tdengine.sh
traj_store_pg/docker/12_wait_tdengine_ready.sh
traj_store_pg/docker/13_init_tdengine.sh
```

默认目录分别落在：

- PostgreSQL：`traj_store_pg/data/postgres/dev/`
- TDengine：`traj_store_pg/data/tdengine/dev/`

如果你要手工启用 benchmark-only 的 PostgreSQL 口径，可以在 PostgreSQL 两个脚本前带上同一个环境变量：

```bash
POSTGRES_PROFILE=bench_low_wal_compressed traj_store_pg/docker/01_run_postgres.sh
traj_store_pg/docker/02_wait_postgres_ready.sh
POSTGRES_PROFILE=bench_low_wal_compressed traj_store_pg/docker/03_init_postgres.sh
```

停止时分别执行：

```bash
traj_store_pg/docker/04_stop_postgres.sh
traj_store_pg/docker/14_stop_tdengine.sh
```

## 推荐单次压测

默认主路径仍然是 `pg_tdengine_compact`，但如果你现在要跑“开启 PG 压缩 / 降低 WAL 干扰”的测试口径，推荐显式带上 `--postgres-profile bench_low_wal_compressed`：

```bash
. .venv/bin/activate
python3 traj_store_pg/scripts/08_run_ingest_benchmark.py \
  --duration-seconds 30 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --storage-layout pg_tdengine_compact \
  --postgres-profile bench_low_wal_compressed \
  --insert-row-target 10000
```

这个命令会自动：

- 起一个独立 PostgreSQL Docker 容器
- 起一个独立 TDengine Docker 容器
- 为本次 run 创建独立数据目录
- 跑 sender / receiver
- 输出 sender、receiver、PostgreSQL、TDengine 资源和落盘汇总

## PostgreSQL benchmark profile

当前 PostgreSQL 侧有两种 benchmark 口径：

- `default`
  - 保持标准 `postgres:17` 默认持久化行为
  - 适合拿来观察 PostgreSQL 目录、WAL 和后台行为的自然总代价
- `bench_low_wal_compressed`
  - `ingest_batches` 和 `trace_latest_state` 改成 `UNLOGGED`
  - 通过 `POSTGRES_INITDB_WALDIR` 把 WAL 目录放到容器内 `tmpfs`，避免把 WAL 目录直接算进持久落盘口径
  - 启动时额外打开 `wal_compression=on`、`synchronous_commit=off`、`wal_level=minimal`、`autovacuum=off` 等 benchmark-only 设置
  - 如果镜像支持 `lz4` TOAST，就对 `ingest_batches.storage_layout` 启用 `lz4`；否则回退到 PostgreSQL 默认 TOAST 压缩方法

要注意两点：

- 这是一条 benchmark-only 口径，不代表可直接照搬到生产环境
- 当前 PostgreSQL 表里绝大多数字段都是定长数值列，所以所谓“PG 压缩”实际收益有限；这条口径的主要作用是降低 WAL 和后台维护对测试结果的干扰

## 两种布局含义

- `pg_tdengine_raw`
  - TDengine 保留完整 `sample_timestamp` 和浮点列
  - 用来回答“只换数据库组合，不做 schema 压缩”时的代价
- `pg_tdengine_compact`
  - TDengine 保留 `TIMESTAMP`，其他业务列改成定点整数/小整数
  - PostgreSQL 侧统一保留 canonical compact 热状态
  - 用来回答“数据库组合 + schema 压缩”后的主路径代价

## 路线设计

当前路线刻意把两类职责拆开：

- TDengine 管全量时序事实
  - 以 `trace_id` 作为 tag，按一车一子表写入 supertable
  - 目标是验证它对持续时序 ingest 的承接能力
- PostgreSQL 管事务型和热状态型数据
  - `ingest_batches` 保存每个批次的边界与统计
  - `trace_latest_state` 保存每个 `trace_id` 的最后一条状态
  - 目标是验证它是否适合作为控制面和热读索引层

这样做的原因是：

- 让 TDengine 聚焦全量时序写入和时间范围查询
- 让 PostgreSQL 聚焦主键 upsert、事务和运维元数据
- 避免把同一份全量事实在两套数据库里完整重复一遍

## 指标输出

每次压测都会在：

```text
traj_store_pg/data/benchmarks/<run-id>/
```

生成独立目录，重点看：

- `sender_summary.json`
  - sender 实际发了多少 batch / rows / bytes
- `receiver_metrics.csv`
  - receiver、PostgreSQL、TDengine 的周期采样指标
- `receiver_summary.json`
  - 接收端最终汇总
- `benchmark_summary.json`
  - sender / receiver 对账结果
- `sink/storage_layout.json`
  - 当前布局、数据库名、表名、写入策略等元数据
- `sink/postgres_data/`
  - 本次 run 的 PostgreSQL 数据目录
- `sink/tdengine_data/`
  - 本次 run 的 TDengine 数据目录

## 关键字段

- sender 口径
  - `rows_sent`
  - `avg_rows_per_second`
- receiver 口径
  - `rows_received`
  - `rows_inserted`
  - `rows_in_sink`
  - `rows_match`
- PostgreSQL 热状态
  - `postgres_trace_rows`
  - `postgres_batch_rows`
- receiver 资源
  - `receiver_peak_cpu_percent`
  - `receiver_peak_rss_bytes`
- PostgreSQL 资源
  - `postgres_peak_cpu_percent`
  - `postgres_peak_rss_bytes`
- TDengine 资源
  - `tdengine_peak_cpu_percent`
  - `tdengine_peak_rss_bytes`
- 落盘
  - `postgres_data_bytes_post_run`
  - `tdengine_data_bytes_post_run`
  - `sink_bytes_post_run`
  - `bytes_per_row_post_run`
  - `bytes_per_row_tdengine_data_post_run`
  - `bytes_per_row_postgres_data_post_run`

## 注意事项

- 这个 benchmark 测的是“双写 route”的整体代价，不只是 TDengine 单库写入代价
- `rows_in_sink` 对账口径以 TDengine 全量事实表为准；PostgreSQL 本来就只保存热状态和批次台账
- `postgres_data_bytes_post_run` 通常会显著小于 TDengine 主体落盘，因为它不保存全量 box facts
- `sink_bytes_post_run` 是整个 sink 目录口径，会混入 TDengine 日志目录，不应直接当成 TDengine 主表净体积
- 当前项目还没有补完整的保留、删除、事件构建和 hot cache 路径，所以它更像一条候选技术路线 benchmark，而不是现成生产方案

## 当前 5 分钟结果与 1 天外推

这条路线已经补了一次 5 分钟实测，样本目录是：

- `traj_store_pg/data/benchmarks/ingest_20260327T054126Z/`
- `storage_layout = pg_tdengine_compact`
- `postgres_profile = default`

关键结果如下：

- `rows_sent = 600,000`
- `rows_received = 600,000`
- `rows_in_sink = 600,000`
- `rows_match = True`
- `avg_rows_per_second_ingest = 1,999.17`
- `avg_rows_per_second_total = 1,999.08`
- `receiver_peak_rss_bytes = 59,756,544`
- `postgres_peak_rss_bytes = 108,470,272`
- `tdengine_peak_rss_bytes = 490,917,888`
- `postgres_batch_rows = 600`
- `postgres_trace_rows = 200`
- `postgres_data_bytes_post_run = 65,326,852 B`
- `tdengine_data_bytes_post_run = 34,131,021 B`
- `sink_bytes_post_run = 102,205,221 B`
- `bytes_per_row_postgres_data_post_run = 108.878 B/row`
- `bytes_per_row_tdengine_data_post_run = 56.885 B/row`
- `bytes_per_row_post_run = 170.342 B/row`

按 nominal `10 fps * 200 box/frame = 2,000 rows/s` 外推，一天约 `172,800,000` 行：

- 只看 TDengine 全量事实落盘：约 `9.830 GB/day`，约 `9.155 GiB/day`
- 只看 PostgreSQL 热状态和批次台账目录：约 `18.814 GB/day`，约 `17.522 GiB/day`
- 看整个 `sink/` 目录总代价：约 `29.435 GB/day`，约 `27.414 GiB/day`

这里要特别注意三种口径的含义：

- `tdengine_data_bytes_post_run` 更接近全量时序事实表的主落盘大小
- `postgres_data_bytes_post_run` 不代表全量 `box_info`，它包含的是热状态、批次台账以及 PostgreSQL 自身页面/WAL 等开销
- `sink_bytes_post_run` 是整条 PostgreSQL + TDengine 双写路线的总目录代价，会把 `tdengine_logs/` 也一起算进去

所以如果你要估算“全量事实层”主体体积，应优先看 `tdengine_data_bytes_post_run`；如果你要估算“整条技术路线”一天会吃掉多少磁盘，再看 `sink_bytes_post_run`。


再补一组 benchmark-only 的新口径 5 分钟实测，样本目录是：

- `traj_store_pg/data/benchmarks/ingest_pg_low_wal_compact_5min/`
- `storage_layout = pg_tdengine_compact`
- `postgres_profile = bench_low_wal_compressed`

关键结果如下：

- `rows_sent = 600,000`
- `rows_received = 600,000`
- `rows_in_sink = 600,000`
- `rows_match = True`
- `avg_rows_per_second_ingest = 1,999.59`
- `avg_rows_per_second_total = 1,999.47`
- `receiver_peak_rss_bytes = 60,067,840`
- `postgres_peak_rss_bytes = 96,612,352`
- `tdengine_peak_rss_bytes = 464,420,864`
- `postgres_batch_rows = 600`
- `postgres_trace_rows = 200`
- `postgres_data_bytes_post_run = 31,360,399 B`
- `tdengine_data_bytes_post_run = 34,128,431 B`
- `sink_bytes_post_run = 68,236,760 B`
- `bytes_per_row_postgres_data_post_run = 52.267 B/row`
- `bytes_per_row_tdengine_data_post_run = 56.881 B/row`
- `bytes_per_row_post_run = 113.728 B/row`

按 nominal `10 fps * 200 box/frame = 2,000 rows/s` 外推，一天约 `172,800,000` 行：

- 只看 TDengine 全量事实落盘：约 `9.829 GB/day`，约 `9.154 GiB/day`
- 只看 PostgreSQL 热状态和批次台账目录：约 `9.032 GB/day`，约 `8.412 GiB/day`
- 看整个 `sink/` 目录总代价：约 `19.652 GB/day`，约 `18.303 GiB/day`

和旧基线 `postgres_profile = default` 并列看，结论很清楚：

| 口径 | 旧基线 `default` | 新口径 `bench_low_wal_compressed` | 变化 |
| --- | ---: | ---: | ---: |
| PostgreSQL 5 分钟目录 | `65,326,852 B` | `31,360,399 B` | `-51.99%` |
| TDengine 5 分钟目录 | `34,131,021 B` | `34,128,431 B` | `-0.01%` |
| whole sink 5 分钟目录 | `102,205,221 B` | `68,236,760 B` | `-33.24%` |
| PostgreSQL 日外推 | `18.814 GB/day` | `9.032 GB/day` | `-51.99%` |
| TDengine 日外推 | `9.830 GB/day` | `9.829 GB/day` | `-0.01%` |
| whole sink 日外推 | `29.435 GB/day` | `19.652 GB/day` | `-33.24%` |

如果把吞吐和资源占用也一起看：

- ingest 吞吐基本不变：`1,999.17 -> 1,999.59 rows/s`
- PostgreSQL 峰值 RSS 下降约 `10.93%`
- TDengine 峰值 RSS 下降约 `5.40%`

也就是说，新口径主要压下去的是 PostgreSQL 侧的目录和 WAL 干扰，TDengine 全量事实体积几乎没有变化；整条 `traj_store_pg` 路线的总目录体积从约 `29.435 GB/day` 降到约 `19.652 GB/day`，但整体仍明显高于 `traj_store` 和 `traj_store_clickhose` 的主数据口径。

## 快速对比两种布局

```bash
. .venv/bin/activate
python3 traj_store_pg/scripts/09_compare_storage_layouts.py \
  --layouts pg_tdengine_raw pg_tdengine_compact \
  --duration-seconds 30 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --postgres-profile bench_low_wal_compressed \
  --insert-row-target 10000
```
