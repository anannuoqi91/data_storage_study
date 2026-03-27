# ClickHouse Box Ingest Benchmark

`traj_store_clickhose` 延续当前 `traj_store` 的主思路，保留 `sender -> receiver -> sink` 这条链路，只是把主落盘从 Parquet / DuckDB 换成了 ClickHouse。

当前项目主要回答两类问题：

- ClickHouse 在 `10 fps * 200 box/frame` 这一类轨迹写入场景下，能否稳定承接持续 ingest
- 在做了 schema 压缩和 `ZSTD` 列压缩后，ClickHouse 的资源消耗、落盘体积和长期外推量级大概是什么水平

当前范围只覆盖 ingest benchmark，不包含 `events` / `hot cache` 的完整 ClickHouse 化迁移。

## 当前脚本

- `scripts/06_box_sender.py`
  - 按帧持续生成并发送 `box` 批次
- `scripts/07_clickhouse_receiver.py`
  - 单连接、同步收包
  - 把批次转成 ClickHouse `INSERT ... FORMAT TSV`
  - 记录 Python receiver 和 ClickHouse server 两侧 CPU / RSS / 吞吐 / 落盘大小
- `scripts/08_run_ingest_benchmark.py`
  - 自动起停 Docker 里的 ClickHouse
  - 跑单个布局的 sender / receiver 压测
- `scripts/09_compare_storage_layouts.py`
  - 顺序跑多种 ClickHouse 布局并输出对比结果
- `scripts/03_queries.sql`
  - 常用检查 SQL
- `docker/01_run_clickhouse.sh`
  - 手动创建 ClickHouse Docker 容器
- `docker/02_wait_clickhouse_ready.sh`
  - 等待 HTTP 接口 ready
- `docker/03_init_database.sh`
  - 手动建库、建表
- `docker/04_stop_clickhouse.sh`
  - 停容器并回收宿主机目录权限
- `docker/05_smoke_query.sh`
  - 在建表后插入 2 行样例数据并查询验通

## 运行前提

- 本机可用 `docker`
- Python 环境里安装 `psutil`
- runner 默认使用官方镜像 `clickhouse/clickhouse-server:25.8`

## 安装

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -r traj_store_clickhose/requirements.txt
```

## 推荐单次压测

默认主路径是 `clickhouse_compact_zstd`：

```bash
. .venv/bin/activate
python3 traj_store_clickhose/scripts/08_run_ingest_benchmark.py \
  --duration-seconds 30 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --storage-layout clickhouse_compact_zstd \
  --insert-row-target 10000
```

这个命令会自动：

- 起一个独立 ClickHouse Docker 容器
- 为本次 run 创建独立数据目录
- 跑 sender / receiver
- 默认执行一次 `OPTIMIZE TABLE ... FINAL`
- 输出 sender、receiver、ClickHouse 资源和落盘汇总

## 数据库设计

当前数据库设计围绕两个目标：

- 让 ClickHouse 在 `2000 rows/s` 这类持续写入下保持稳定 ingest
- 让最终物理落盘和查询路径都尽量贴近真实分析场景，而不是只追求“能写进去”

### 1. 为什么选 `MergeTree`

当前两套表都使用 `MergeTree`：

- 这是 ClickHouse 最稳定的本地列式落盘路径
- 支持按分区落 parts，再由后台 merge
- 能直接通过 `system.parts` 看 active parts、压缩后字节数和未压缩字节数

这里不使用 `Log`、`TinyLog`、`StripeLog` 这类轻量引擎，因为它们更适合一次性导入，不适合长期作为主存储基线。

### 2. 为什么保留两套布局

当前有两张 benchmark 表：

- `box_info_raw_lz4`
  - 基线表
  - 保留完整 `sample_timestamp` 和浮点列
  - 用来回答“只换数据库，不做 schema 压缩”时 ClickHouse 的代价
- `box_info_compact_zstd`
  - 当前主路径
  - 把 `sample_timestamp` 拆成 `sample_date + sample_hour + sample_offset_ms`
  - 把位置、尺寸、速度、角度改成定点整数
  - 低基数字段保持小整数类型
  - 列级使用 `CODEC(ZSTD(...))`

这样可以把“ClickHouse 引擎本身的收益”和“schema 压缩的收益”拆开观察，而不是混成一个结论。

### 3. `compact` schema 的设计原因

`clickhouse_compact_zstd` 延续了原项目“先从 schema 上压缩”的思路：

- 不引入合成 `box_id`
  - 这类轨迹查询和事件构建更常按 `trace_id + 时间范围` 过滤
  - 额外主键只会增加写入和落盘体积
- 绝对时间拆成 `date/hour + offset`
  - 更利于排序和压缩
  - 仍然能恢复精确时间
- 浮点改定点整数
  - `position_*` 用毫米
  - `length/width/height` 用毫米
  - `speed_kmh` 用 `speed_centi_kmh`
  - `spindle` 用 `spindle_centi_deg`
- 低基数字段降型
  - `obj_type`、`lane_id` 都保持 `UInt8`
- 列级使用 `CODEC(ZSTD(...))`
  - 让高重复、低熵的列进一步压缩

这个思路和当前 `traj_store` 主路径一致，所以后续对比更有可比性。

### 4. 分区与排序键为什么这样定

`box_info_compact_zstd` 当前定义是：

- `PARTITION BY (sample_date, sample_hour)`
- `ORDER BY (sample_date, sample_hour, sample_offset_ms, trace_id, frame_id)`

主要考虑是：

- 小时级分区能自然对齐当前轨迹数据的时间过滤模式
- `sample_offset_ms` 让分区内时间顺序稳定，不需要每行重复保存完整绝对时间戳
- `trace_id`、`frame_id` 放在后面，可以兼顾同车轨迹扫描和压缩局部性

当前刻意不把分区切得更细，因为过细分区会让 active parts 数量膨胀，反而影响写入和 merge。

### 5. 写入路径为什么仍然保留 receiver

当前 receiver 仍然保留单连接、同步收包这条主思路：

- sender 按 JSON batch 发数据
- receiver 解码后，转成 ClickHouse `INSERT ... FORMAT TSV`
- 到达 `insert_row_target` 后批量写入

这里刻意没有直接把 sender 改成原生 ClickHouse 协议客户端，因为当前项目目标不是“做出 ClickHouse 最优写法”，而是先在与现有 benchmark 近似的链路约束下，观察 ClickHouse 侧的真实表现。

### 6. 为什么 benchmark 默认执行 `OPTIMIZE TABLE ... FINAL`

ClickHouse 的落盘不是“写完即最终形态”，而是：

- 先生成 active parts
- 后续再由 merge 合并和重写

所以这里默认在收包结束后执行一次 `OPTIMIZE TABLE ... FINAL`，主要是为了让 benchmark 的落盘结果更接近“压缩稳定后”的状态，避免短压测只测到一堆尚未 merge 的零碎 parts。

但要注意：

- `OPTIMIZE FINAL` 本身有额外代价
- 在大样本上它不一定适合作为每次生产写入后的动作
- benchmark 里保留它，是为了统一比较口径

## 手动 Docker 启动与建库流程

如果你要单独调试 ClickHouse 容器、建库建表过程，推荐按下面顺序执行：

```bash
./traj_store_clickhose/docker/01_run_clickhouse.sh
./traj_store_clickhose/docker/02_wait_clickhouse_ready.sh
./traj_store_clickhose/docker/03_init_database.sh
./traj_store_clickhose/docker/05_smoke_query.sh
```

如果你想验证 raw 表，也可以执行：

```bash
SMOKE_LAYOUT=raw ./traj_store_clickhose/docker/05_smoke_query.sh
```

停止并回收宿主机目录权限：

```bash
./traj_store_clickhose/docker/04_stop_clickhouse.sh
```

这五个脚本都支持通过环境变量覆盖默认值。默认值如下：

- 镜像：`clickhouse/clickhouse-server:25.8`
- 容器名：`traj-store-clickhose-dev`
- DB：`traj_store_clickhose`
- User：`bench`
- Password：`benchpass`
- HTTP 端口：`18123`
- Native 端口：`19000`
- 数据目录：`traj_store_clickhose/data/clickhouse/dev/store`
- 日志目录：`traj_store_clickhose/data/clickhouse/dev/logs`

覆盖示例：

```bash
CONTAINER_NAME=my-ch \
HTTP_PORT=28123 \
NATIVE_PORT=29000 \
DB_NAME=my_bench \
DB_USER=my_user \
DB_PASSWORD=my_pass \
./traj_store_clickhose/docker/01_run_clickhouse.sh
```

注意，benchmark runner 也使用相同的镜像、账号和建表逻辑，但它会为每次 run 创建独立容器名和独立数据目录，不复用 `traj-store-clickhose-dev` 这个手动调试容器。

## Docker 脚本与 SQL 模板说明

`docker/` 目录下当前提供五个脚本：

- `01_run_clickhouse.sh`
  - 检查容器名冲突
  - 准备挂载目录
  - 拉镜像并启动 ClickHouse
  - 通过环境变量创建 bench database / user / password
- `02_wait_clickhouse_ready.sh`
  - 轮询 HTTP 接口，直到数据库可接受认证和查询
- `03_init_database.sh`
  - 顺序执行 `docker/sql/01_create_database.sql` 和 `docker/sql/02_create_layout_tables.sql`
  - 用模板替换把 `__DB_NAME__` 渲染成当前库名
- `04_stop_clickhouse.sh`
  - 停容器，并把挂载目录权限改回当前宿主机用户
- `05_smoke_query.sh`
  - 向 benchmark 表插入 2 行样例数据
  - 按 `trace_id` 查询并验证行数与 `frame_id` 范围

对应 SQL 文件：

- `docker/sql/01_create_database.sql`
  - 建库模板
- `docker/sql/02_create_layout_tables.sql`
  - 建 `box_info_raw_lz4`
  - 建 `box_info_compact_zstd`

## 建库建表过程

当前手动建库建表流程是：

1. `01_run_clickhouse.sh`
   容器启动时，通过环境变量创建默认 DB / User / Password。
2. `02_wait_clickhouse_ready.sh`
   等 ClickHouse 的 HTTP 接口真正 ready，而不是只看容器已经 `Up`。
3. `03_init_database.sh`
   再执行显式 SQL，保证建库建表过程可追踪、可重跑、可单独调试。
4. `05_smoke_query.sh`
   对目标表插入 2 行样例数据，并按 `trace_id` 做一次查询验通。

也就是说，当前是“双保险”设计：

- 容器启动阶段先通过环境变量注入用户和数据库
- 初始化阶段再用 SQL 明确创建数据库和两套 benchmark 表

这样做的原因是：

- 官方镜像对 `default` 用户的认证约束在不同版本和配置下并不稳定
- 直接依赖 `default` 账户在某些环境下会失败
- 用显式 `bench` 用户和密码，行为更稳定，也更接近后续长期测试需求

## 注意事项

- 五个 Docker 脚本要使用同一组环境变量
  - 如果你改了 `DB_NAME`、`DB_USER`、`DB_PASSWORD`、`HTTP_PORT`，后续 `02/03/04/05` 也要带同样的值
- 不要把短压测的宿主机目录大小直接当成最终压缩结果
  - `clickhouse_store/` 里会包含元数据、system 表和目录开销
  - 小样本更应该看 `active_bytes_on_disk_post_run`
- `receiver_summary.json` 里的两类落盘指标含义不同
  - `active_bytes_on_disk_post_run` 更接近业务表 active parts 的真实压缩后大小
  - `clickhouse_data_bytes_post_run` 更接近整个容器数据目录在宿主机上的总占用
- 挂载目录需要可写
  - 当前脚本会在启动前对数据目录和日志目录执行 `chmod 777`
  - 如果容器异常退出而目录被 root 持有，优先执行 `docker/04_stop_clickhouse.sh` 回收权限
- `DB_NAME` 需要保持 ClickHouse 合法标识符
  - 当前初始化脚本和 smoke 脚本都会校验名称，只允许字母、数字和下划线
- `05_smoke_query.sh` 默认会为每次执行生成新的 `SMOKE_TRACE_ID`
  - 这样重复执行时不会和上一次的 smoke 数据混在一起
  - 如果你想复现固定样例，也可以手动传入 `SMOKE_TRACE_ID=<uint32>`
- `OPTIMIZE TABLE ... FINAL` 是 benchmark 口径动作，不是生产默认建议
- 手动调试容器和 benchmark 容器不要混用
  - 手动脚本默认使用固定 dev 容器名
  - benchmark runner 使用每次 run 独立容器，避免测试互相污染

## 指标输出

每次压测都会在：

```text
traj_store_clickhose/data/benchmarks/<run-id>/
```

生成独立目录，重点看：

- `sender_summary.json`
  - sender 实际发了多少 batch / rows / bytes
- `receiver_metrics.csv`
  - receiver 和 ClickHouse server 的周期采样指标
- `receiver_summary.json`
  - 接收端最终汇总
- `benchmark_summary.json`
  - sender / receiver 对账结果
- `sink/storage_layout.json`
  - 当前布局、排序键、codec、row target 等元数据
- `sink/clickhouse_store/`
  - 本次 run 的 ClickHouse 实际数据目录

## 关键字段

- sender 口径
  - `rows_sent`
  - `avg_rows_per_second`
- receiver 口径
  - `rows_received`
  - `rows_inserted`
  - `rows_in_sink`
  - `rows_match`
- receiver 资源
  - `receiver_avg_cpu_percent`
  - `receiver_peak_cpu_percent`
  - `receiver_avg_rss_bytes`
  - `receiver_peak_rss_bytes`
- ClickHouse 资源
  - `clickhouse_avg_cpu_percent`
  - `clickhouse_peak_cpu_percent`
  - `clickhouse_avg_rss_bytes`
  - `clickhouse_peak_rss_bytes`
- 落盘
  - `active_bytes_on_disk_post_run`
  - `active_compressed_bytes_post_run`
  - `active_uncompressed_bytes_post_run`
  - `clickhouse_data_bytes_post_run`

## 当前 5 分钟结果与 `traj_store` 对比

这里补一组当前可复现的 `clickhouse_compact_zstd` 结果，便于和 `traj_store` README 里已经记录的 `parquet_compact_zstd` 主路径做直接对照。

本次 ClickHouse 实测来自：

- `traj_store_clickhose/data/benchmarks/ingest_20260327T014603Z/`

运行参数：

```bash
. .venv/bin/activate
python3 traj_store_clickhose/scripts/08_run_ingest_benchmark.py \
  --duration-seconds 300 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --storage-layout clickhouse_compact_zstd \
  --insert-row-target 10000
```

### 1. ClickHouse 5 分钟实测

- `rows_sent = 600,000`
- `rows_received = 600,000`
- `rows_in_sink = 600,000`
- `rows_match = True`
- `avg_rows_per_second_ingest = 1,999.60`
- `avg_rows_per_second_total = 1,999.07`
- `active_bytes_on_disk_post_run = 5,053,000 B`
- `bytes_per_row_active_bytes_post_run = 8.422 B/row`
- `clickhouse_data_bytes_post_run = 60,831,181 B`
- `bytes_per_row_clickhouse_data_post_run = 101.385 B/row`
- `active_parts_post_run = 1`
- `receiver_peak_rss_bytes = 38,313,984`
- `clickhouse_peak_rss_bytes = 862,334,976`

按 nominal `10 fps * 200 box/frame = 2,000 rows/s` 外推，一天约 `172,800,000` 行：

- 只看业务表 active parts：约 `1.455 GB/day`，约 `1.355 GiB/day`
- 如果误把整个 `clickhouse_store/` 当成主存储：约 `17.519 GB/day`，约 `16.316 GiB/day`

这里要特别注意第二个数不是主表净数据。短压测下整个 `clickhouse_store/` 会混入：

- ClickHouse `system.*` 日志表
- metadata / access / preprocessed config 等目录
- merge 尚未完全清理掉的旧 parts

所以容量规划时，应优先使用 `active_bytes_on_disk_post_run`，不要直接用整个数据目录大小。

### 2. 与 `traj_store` README 当前样本对比

`traj_store` README 当前引用的是：

- `traj_store/data/benchmarks/compact_5min_10fps_200box/`

对应结果是：

- `399,000` 行最终落成 `3,808,873 B`
- `bytes_per_row_post_flush = 9.546 B/row`
- 外推主落盘约 `1.650 GB/day`，约 `1.536 GiB/day`

并列表如下：

| 路线 | README 引用样本 | 5 分钟实际写入行数 | 短时落盘口径 | 每行字节 | 一天外推 |
| --- | --- | ---: | ---: | ---: | ---: |
| `traj_store` `parquet_compact_zstd` | `compact_5min_10fps_200box` | `399,000` | `3,808,873 B` | `9.546 B/row` | `1.650 GB/day` |
| `traj_store_clickhose` `clickhouse_compact_zstd` | `ingest_20260327T014603Z` | `600,000` | `5,053,000 B` active parts | `8.422 B/row` | `1.455 GB/day` |
| `traj_store_clickhose` `clickhouse_compact_zstd` | `ingest_20260327T014603Z` | `600,000` | `60,831,181 B` whole `clickhouse_store/` | `101.385 B/row` | `17.519 GB/day` |

可以直接读出三点：

- 如果比较“主数据净落盘”，当前 ClickHouse active parts 比 `traj_store` README 里的 Parquet 样本更小，`8.422 B/row` 对 `9.546 B/row`，约少 `11.8%`
- 如果比较“短压测时整个运行时目录”，ClickHouse 会明显更大，因为它不是单纯的业务表文件，还带着服务端内部目录和日志表
- 当前 ClickHouse 这次 5 分钟 run 基本跑满了 nominal `2,000 rows/s`；而 `traj_store` README 当前样本实际写入 `399,000` 行，所以两边短时样本的 raw 行数并不是严格同口径，更适合拿 `bytes_per_row` 和日外推做对比

### 3. 两条技术路线的优缺点

`traj_store` 主路径，也就是 `parquet_compact_zstd`，优点是：

- 运行时形态简单，receiver 直接把数据落成 Parquet 文件，没有独立数据库服务
- 容量口径更直接，`sink_bytes_post_flush` 基本就是主落盘结果，不容易混入额外系统表开销
- 产物是开放文件格式，后续可以继续喂给 DuckDB、Spark、ClickHouse 或离线对象存储
- 当前项目里 `events` / `hot cache` / `migration` / `snapshot` 这一整套配套都已经围绕这条路线展开，集成成本更低

`traj_store` 主路径的缺点是：

- 当前 README 引用样本的实际吞吐没有跑满 nominal `2,000 rows/s`
- 主存储只是文件，不是常驻查询服务；要做交互式分析、并发查询或二级索引能力，通常还要再挂一层查询引擎
- flush、row group 和文件布局会直接影响写入吞吐和文件数量，需要自己管理这套文件侧策略

`traj_store_clickhose` 主路径，也就是 `clickhouse_compact_zstd`，优点是：

- 这次 5 分钟 run 已经验证，在当前 sender/receiver 约束下可以基本稳定承接 `2,000 rows/s`
- 只看业务表 active parts 时，当前样本的单位行落盘比 README 里的 Parquet 样本更小
- 写入完成后，数据已经直接落在可查询的列式数据库里，不需要额外再导入一次分析引擎
- `system.parts`、`query_log`、`metric_log` 等内建观测能力更强，便于持续看 merge、parts 和查询行为

`traj_store_clickhose` 主路径的缺点是：

- 运维复杂度更高，需要管理容器、用户、密码、数据库、表和后台 merge 行为
- 短压测时整个数据目录的体积会被 system 日志表和旧 parts 明显放大，容量估算口径更容易用错
- 资源占用更重，这次样本里 ClickHouse server 的峰值 RSS 约 `822 MiB`，再加上 Python receiver，总体常驻明显高于 `traj_store` 的单进程 Parquet 路线
- benchmark 里使用 `OPTIMIZE TABLE ... FINAL` 是为了统一口径，不等于生产默认写法，实际长期运行还要单独设计 merge 和保留策略

如果目标是“先把主落盘压到稳定、简单、可搬运的 GiB/day 量级”，`traj_store` 这条 Parquet 路线更稳妥。

如果目标是“在主落盘之外，还希望直接得到一个可持续 ingest、可在线分析的列式服务”，`traj_store_clickhose` 更有潜力，但需要接受更高的运行时和运维成本。

## 快速对比两种布局

```bash
. .venv/bin/activate
python3 traj_store_clickhose/scripts/09_compare_storage_layouts.py \
  --layouts clickhouse_raw_lz4 clickhouse_compact_zstd \
  --duration-seconds 30 \
  --fps 10 \
  --vehicles-per-frame 200 \
  --batch-frames 5 \
  --insert-row-target 10000
```
