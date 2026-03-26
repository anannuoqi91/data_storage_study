# Box Ingest Benchmark

这个目录现在的主用途是做两件事：

- sender 持续发送 `box` 数据
- receiver 同步接收并落盘，同时记录写入阶段的 CPU / RSS / 吞吐和真实磁盘占用

重点已经从“写入 DuckDB 原生表”改成了“对比不同落盘布局的真实代价”，默认推荐主落盘使用 Parquet。

## 当前脚本

- `scripts/06_box_sender.py`
  - 按帧持续生成并发送 `box` 批次
- `scripts/07_box_receiver.py`
  - 单连接、同步收包
  - 支持 `duckdb_raw`、`duckdb_compact`、`parquet_snappy`、`parquet_zstd`、`parquet_compact_zstd`
- `scripts/08_run_ingest_benchmark.py`
  - 跑单个布局的 sender/receiver 压测
- `scripts/09_compare_storage_layouts.py`
  - 用同一份 workload 顺序跑多种布局并输出对比表
- `scripts/10_schema_ctl.py`
  - 管理 `writer.duckdb` 迁移、快照回滚、以及 `box_info/events` 的 active version 指针

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r traj_store/requirements.txt
```

## 推荐单次压测

默认直接使用 `parquet_compact_zstd`：

```bash
. .venv/bin/activate
python3 traj_store/scripts/08_run_ingest_benchmark.py \
  --duration-seconds 30 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --storage-layout parquet_compact_zstd \
  --flush-row-target 10000 \
  --row-group-size 100000
```

## 推荐布局对比

```bash
python3 traj_store/scripts/09_compare_storage_layouts.py \
  --duration-seconds 10 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --flush-row-target 5000 \
  --row-group-size 100000
```

## 各布局含义

- `duckdb_raw`
  - 基线方案，直接写 DuckDB 原生表，包含合成 `box_id`
- `duckdb_compact`
  - 仍然写 DuckDB 原生表，但把浮点量化成整数并降型
- `parquet_snappy`
  - 主落盘改成 Parquet，压缩使用 Snappy，按 `date/hour` 分区
- `parquet_zstd`
  - 主落盘改成 Parquet，压缩使用 Zstd，按 `date/hour` 分区
- `parquet_compact_zstd`
  - 主落盘改成 Parquet，压缩使用 Zstd
  - 文件内按 `date, hour, sample_offset_ms, trace_id, frame_id` 排序
  - 不写合成 `box_id`
  - `sample_timestamp` 不再整列存储，改为 `date/hour` 分区 + `sample_offset_ms`
  - `position_*`、`length/width/height`、`speed_kmh`、`spindle` 改成整数化存储

## 指标输出

每次压测都会在：

```text
traj_store/data/benchmarks/<run-id>/
```

生成独立目录，里面重点看这几个文件：

- `receiver_metrics.csv`
  - 周期采样的 CPU、RSS、吞吐、sink 大小
- `receiver_summary.json`
  - 接收端最终汇总
- `benchmark_summary.json`
  - sender / receiver 对账结果
- `sink/`
  - 实际落盘目录
  - DuckDB 布局时这里会有 `.duckdb` / `.wal`
  - Parquet 布局时这里会有 `parquet/date=.../hour=.../*.parquet`
- `sink/storage_layout.json`
  - 当前布局的元数据，例如压缩算法、排序键、compact 的 scale 与 `base_timestamp_ms`

## 关键字段

- 写入资源消耗
  - `receiver_metrics.csv` / `receiver_summary.json` 里的 `peak_cpu_percent`、`peak_rss_bytes`、`avg_rows_per_second`
- 真实落盘大小
  - `sink_bytes_post_flush`
  - `parquet_bytes_post_flush`
  - `duckdb_bytes_post_flush`
  - `wal_bytes_post_flush`
  - `bytes_per_row_post_flush`

`peak_cpu_percent` 现在已经按逻辑 CPU 数归一化到 `0-100` 口径，便于直接看单进程占用。

## 设计取向

这个基准现在默认遵循下面几条：

- 主存储优先用 Parquet，而不是全量导入 DuckDB 原生库
- Parquet 按 `date/hour` 分区
- 文件内按时间和 `trace_id` 排序，便于压缩和过滤
- 对浮点列尽量做定点整数化，减少落盘体积
- 只有做基线对比时，才保留 `duckdb_raw` 这类较重的布局

## 当前 Box Schema

当前主流程写出的 `box_info` 已经切到 `box_info.v2`，目标就是按你说的“从 schema 上先压缩”：

- 移除 `box_id`
  - 它是合成主键，分析和事件回溯基本都不会按它过滤
- 行内不再保存完整 `sample_timestamp`
  - Parquet 侧改成 `date/hour` 分区 + `sample_offset_ms`
  - 需要完整时间时再通过分区起点还原
- 浮点量统一改定点整数
  - `position_x/y/z` -> `position_x_mm/position_y_mm/position_z_mm`
  - `length/width/height` -> `*_mm`
  - `speed_kmh` -> `speed_centi_kmh`
  - `spindle` -> `spindle_centi_deg`
- 低基数字段继续降型
  - `obj_type` -> `UTINYINT`
  - `lane_id` -> `UTINYINT`

当前 `box_info` 的持久化字段如下：

```text
trace_id INTEGER
sample_offset_ms INTEGER
obj_type UTINYINT
position_x_mm INTEGER
position_y_mm INTEGER
position_z_mm INTEGER
length_mm USMALLINT
width_mm USMALLINT
height_mm USMALLINT
speed_centi_kmh USMALLINT
spindle_centi_deg USMALLINT
lane_id UTINYINT
frame_id INTEGER
PARTITION BY date, hour
```

writer 内部 staging/ingest 仍然保留 `sample_timestamp BIGINT`，只是为了简化 checkpoint、刷盘边界和事件构建；真正长期落盘的 Parquet 不再保留这列。

## 为什么 Schema 设计成这样

这版 `box_info.v2` 不是为了“字段最全”，而是明确围绕下面三件事做取舍：

- 主落盘体积要能稳定压到 GiB/day 量级，而不是随字段冗余线性膨胀
- receiver 的热路径要把 CPU 花在真实刷盘上，而不是重复写大字段和高熵字段
- 下游事件构建和范围查询仍然能按 `trace_id + 时间范围` 直接恢复轨迹

具体原因如下：

- 去掉 `box_id`
  - 这是合成主键，不是采集侧自然字段
  - 当前查询和事件构建更常按 `trace_id`、时间范围、分区裁剪来过滤
  - 多存一列不仅增加每行字节数，也会增加写入时的构造成本
- 把 `sample_timestamp` 拆成 `date/hour + sample_offset_ms`
  - `date/hour` 负责 Parquet 分区裁剪
  - `sample_offset_ms` 保留分区内的精确时间顺序
  - 这样避免每行都重复存完整绝对时间戳；需要恢复完整时间时，再用分区起点加 offset 还原
- 浮点改成定点整数
  - 位置、尺寸、速度、角度在当前场景都有明确精度上限，毫米和百分之一单位已经足够
  - 排序后的整数列比浮点列更容易压缩，也避免浮点编码噪声把 Parquet 压缩率拉低
- 低基数字段继续降型
  - `obj_type`、`lane_id`、`speed_centi_kmh`、`spindle_centi_deg` 都有明确业务上界，不需要保留成更大的通用类型
  - 更小的物理类型会同时降低 Parquet 体积和 receiver 中间缓冲占用
- staging 和长期落盘分离
  - writer 内部 staging 继续保留完整 `sample_timestamp`，是为了让 checkpoint、水位推进、flush 边界和事件构建逻辑保持简单
  - 真正长期保存的 Parquet 只保留分析和回放必需的最小字段集

这套 schema 不是纯理论取舍，已经被当前基准结果验证过一轮。以 `traj_store/data/benchmarks/compact_5min_10fps_200box/` 这次 5 分钟实测为例：

- `399,000` 行最终落成 `3,808,873 B`
- `bytes_per_row_post_flush = 9.546 B/row`
- 按 nominal `10 fps * 200 box/frame` 外推，主落盘约 `1.65 GB/day`，约 `1.54 GiB/day`

也就是说，这版 schema 的核心目标不是“极限压缩”，而是在仍然保留事件构建与时间还原能力的前提下，把主落盘控制在可接受的日增量范围内。

## 数据库版本设计

这个项目现在把“数据库版本”拆成两层管理，目标只覆盖两件事：

- 可追溯
- 可回滚

不追求通用迁移框架，也不追求自动 schema 演化。

### 1. `writer.duckdb` 用 migration + snapshot

`writer.duckdb` 是唯一需要真正做 schema migration 的数据库文件。

迁移文件放在：

```text
traj_store/schema/duckdb/<version>/
  manifest.json
  up.sql
  down.sql
```

当前基线版本是：

- `0001_init`

运行时会在 `writer.duckdb` 内维护两张元数据表：

- `traj_meta.schema_version`
  - 当前 schema 版本
- `traj_meta.schema_migration_history`
  - 迁移 / bootstrap / rollback 历史
  - 记录 `from_version`、`to_version`、`release_id`、`checksum`、`git_commit`、`snapshot_path`

每次成功迁移或回滚后，都会在：

```text
traj_store/data/backups/schema/writer_duckdb/<version>/<release-id>/
```

落一份 `writer.duckdb` 快照和 `snapshot.json`。

回滚不依赖复杂 `down.sql`，而是直接恢复目标版本快照。
`down.sql` 仅作为人工参考，不作为主回滚路径。

如果本地已经有旧版 `writer.duckdb`，但还没有版本元数据，脚本会在识别到当前表结构等价于 `0001_init` 时自动做一次 bootstrap，补齐版本记录和首个快照。

### 2. `box_info/events` 用 version directory + active pointer

Parquet lake 不做 SQL 级 migration，而是做“目录版本切换”。

目录约定如下：

```text
traj_store/data/lake/box_info/
  _current.json
  _history.jsonl
  v0001/
  v0002/

traj_store/data/lake/events/
  _current.json
  _history.jsonl
  v0001/
  v0002/
```

其中：

- `_current.json`
  - 当前激活版本指针
- `_history.jsonl`
  - 每次 activate / rollback 的切换历史
- `<version>/_manifest.json`
  - 当前版本的 schema、producer、依赖来源、git commit、更新时间

如果还没开始做目录版本化，系统也兼容历史的 root 直写模式：

- active version 会被视作 `__root__`
- 一旦开始使用 `v0001`、`v0002` 这类目录，就必须通过 `_current.json` 指定 active version，避免 `**/*.parquet` 把多版本一起扫进去

### 3. 读写规则

从这版开始，主流程脚本不再直接读：

```text
traj_store/data/lake/<dataset>/**/*.parquet
```

而是先解析 active version，再只读当前版本目录：

- `scripts/02_simulate_writer.py`
  - 写 active `box_info` 版本
  - 同步刷新该版本的 `_manifest.json`
- `scripts/04_build_events.py`
  - 只从 active `box_info` 版本读
  - 只向 active `events` 版本写
  - `events` manifest 里会记录依赖的 `box_info` 版本
- `scripts/05_build_hot_cache.py`
  - 只读取 active `box_info/events` 版本

`03_queries.sql` 里的示例 glob 也改成了占位符，使用前先通过 `schema_ctl.py status` 拿到当前 active path。

### 4. 常用操作

查看状态：

```bash
python3 traj_store/scripts/10_schema_ctl.py status
```

应用 `writer.duckdb` 迁移：

```bash
python3 traj_store/scripts/10_schema_ctl.py writer-migrate
```

回滚 `writer.duckdb` 到指定版本：

```bash
python3 traj_store/scripts/10_schema_ctl.py writer-rollback --to 0001_init
```

创建并切换 lake 版本：

```bash
python3 traj_store/scripts/10_schema_ctl.py lake-activate \
  --dataset box_info \
  --version v0002 \
  --create \
  --schema-version box_info.v2
```

把 lake 指针切回旧版本：

```bash
python3 traj_store/scripts/10_schema_ctl.py lake-rollback \
  --dataset box_info \
  --to v0001
```

### 5. 设计取舍

这套机制故意保持最小化：

- `writer.duckdb` 走 migration，是因为 checkpoint 表和内部状态必须有确定版本
- Parquet lake 走目录切换，是因为文件集最稳的回滚手段不是改 schema，而是切换 active version
- benchmark 产物继续保留现有元数据文件，不纳入统一 migration；它们是一次性结果，更适合做 provenance，不适合做回滚对象
