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
  - 文件内按 `sample_offset_ms, trace_id` 排序
  - 不写合成 `box_id`
  - `sample_timestamp` 改为 `base_timestamp_ms + sample_offset_ms`
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
