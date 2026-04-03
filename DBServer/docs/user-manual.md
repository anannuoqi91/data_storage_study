# DBServer 初版使用文档

## 1. 文档说明

本文档基于当前仓库代码整理，目标不是描述“未来规划”，而是说明“现在这份代码已经能做什么、应该怎么用、边界在哪里”。

如果你只想快速跑通，优先看：

1. 第 4 节“部署方式”
2. 第 5 节“使用方式”
3. 第 7 节“边界与注意事项”

如果你要评估这套实现是否适合继续演进，重点看：

1. 第 2 节“当前架构”
2. 第 6 节“设计思路”
3. 第 7 节“边界与注意事项”

## 2. 项目定位与当前架构

### 2.1 项目定位

DBServer 当前是一个围绕 `box_info` 单表构建的本地离线数据服务，核心职责有四类：

- 接收 `box_info` 帧数据
- 以 `Parquet + DuckDB metadata` 的方式管理可见数据集
- 基于 release 切换对外暴露“当前可见数据”
- 提供最小可用的查询、导出、删除、封口、回滚和运维能力

它不是分布式数据库，也不是通用多表湖仓框架。当前实现更接近：

- 单机
- 单写入实例
- 本地磁盘数据面
- 受控查询入口
- 围绕 `box_info` 的专用服务

### 2.2 已实现能力

按当前代码，已经落地的能力包括：

- JSONL 文件接入
- REST API 接入
- cyber `Boxes` protobuf 接入
- `box_info` 明细 Parquet 发布
- `trace_index_hot` / `trace_index_cold` 派生索引
- `obj_type_rollup` 派生聚合
- release 管理、回滚、删数、小时封口
- 查询计划生成与本地执行
- 异步导出
- 删除/导出统一 Job API
- callback 回执
- 运行状态与 Prometheus 风格 metrics
- Docker 启动入口

### 2.3 总体架构

当前实现可以按下面这条链路理解：

```text
上游输入
  ├─ JSONL 文件
  ├─ REST /v1/ingest/box-frames
  └─ cyber Boxes protobuf
        ↓
接入适配层
  ├─ JsonlFrameSource
  ├─ API schemas -> FrameBatch
  └─ CyberBoxesFrameDecoder
        ↓
统一领域模型
  ├─ FrameBatch
  └─ BoxRecord
        ↓
校验与缓冲
  ├─ ValidatorService
  └─ IngestService
        ↓
发布链路
  ├─ PublishService -> live/box_info
  ├─ DerivedDataService -> trace_index_hot/cold, obj_type_rollup
  └─ MetadataStore + ReleaseJournal
        ↓
可见性控制
  ├─ current_release
  ├─ file_groups
  ├─ partition_states
  └─ database_releases
        ↓
查询 / 导出 / 运维
  ├─ QueryService
  ├─ ExportService
  ├─ JobService
  ├─ SealService
  ├─ RetentionService
  └─ OpsService
```

### 2.4 运行时结构

仓库里当前有两类运行方式：

- CLI 模式
  - 适合离线导入、调试、手工维护、查询和回归测试
- API 服务模式
  - 由 [server.py](/home/demo/Documents/code/data_storage_study/DBServer/src/dbserver/api/server.py) 启动
  - 由 [app.py](/home/demo/Documents/code/data_storage_study/DBServer/src/dbserver/app.py) 负责后台线程、publish loop、maintenance、job worker、callback retry 和可选 cyber ingest

API 服务模式下的后台线程模型是：

- 一个后台 publish loop
- 一个 maintenance 定时执行入口
- 一个 job worker
- 一个可选 legacy export worker
- 一个可选 cyber subscriber

说明：

- API `/v1/jobs/export` 和 `/v1/jobs/delete` 走的是统一 `JobService`
- `run-export-worker` 处理的是旧的文件型 export 队列，不是 API Job 的前置条件

## 3. 数据模型与存储布局

### 3.1 主数据集

主数据集只有一个：`box_info`

当前写入列如下：

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| `trace_id` | `INTEGER` | 轨迹 ID |
| `device_id` | `USMALLINT` | 设备 ID |
| `event_time_ms` | `BIGINT` | 事件时间，毫秒 |
| `obj_type` | `UTINYINT` | 目标类型 |
| `position_x_mm` | `INTEGER` | X，毫米 |
| `position_y_mm` | `INTEGER` | Y，毫米 |
| `position_z_mm` | `INTEGER` | Z，毫米 |
| `length_mm` | `USMALLINT` | 长，毫米 |
| `width_mm` | `USMALLINT` | 宽，毫米 |
| `height_mm` | `USMALLINT` | 高，毫米 |
| `speed_centi_kmh_100` | `USMALLINT` | 内部速度字段 |
| `spindle_centi_deg_100` | `USMALLINT` | 航向角字段 |
| `lane_id` | `UTINYINT` | 车道 ID，未知时归一为 `255` |
| `frame_id` | `INTEGER` | 帧 ID |

排序键固定为：

- `event_time_ms`
- `frame_id`
- `trace_id`

### 3.2 派生数据集

当前会和 `box_info` 一起维护两类派生数据：

- `trace_index_hot`
- `trace_index_cold`
- `obj_type_rollup`

作用分别是：

- `trace_index_hot/cold`
  - 加速按 `device_id + trace_id` 的轨迹段查询
  - 默认按 `30` 天切冷热
- `obj_type_rollup`
  - 基于 `10` 分钟窗口按 `device_id + lane_id + obj_type` 聚合

### 3.3 时间模型

当前时间模型有三个关键约束：

- 内部统一使用 `event_time_ms`
- 分区统一按 `UTC` 推导
- 可见性基于事件时间 watermark，而不是接收时间

默认参数：

- 发布窗口：`10` 分钟
- 晚到窗口：`120` 秒

这意味着：

- 长驻服务/API 不会立刻发布“刚刚发生”的实时数据
- 默认要等到数据时间落后于当前时间约 `2` 分钟后，相关窗口才会变为可发布

### 3.4 cyber `Boxes` 时间映射

当前 cyber 接入约定如下：

- 订阅消息类型是 [inno_box.proto](/home/demo/Documents/code/data_storage_study/DBServer/src/dbserver/proto/inno_box.proto) 中的 `Boxes`
- `event_time_ms` 优先取 `Boxes.lidar_timestamp`
- `lidar_timestamp` 的单位按 `ns` 处理，并转换为毫秒
- 同一个 `Boxes` 消息中的所有 box 共用同一个 `event_time_ms`
- 如果 `lidar_timestamp` 缺失，才回退到 `timestamp_ms`、`timestamp`、`timestamp_sec`

也就是说，当前实现下：

- 一个 `Boxes` 消息 = 一个 frame
- 一个 frame 内所有 box 的 `event_time_ms` 必须一致

### 3.5 release / file_group / partition_state

这是当前实现最重要的三个控制面概念：

- `release`
  - 一个“当前可见数据集”的快照版本
- `file_group`
  - 一次发布、封口、重建产生的一组物理文件
- `partition_state`
  - 某个 `UTC date + hour + device_id` 分区的状态

状态机大致如下：

- `partition_state`
  - `open`
  - `publishable`
  - `sealing`
  - `sealed`
  - `deleting`
  - `deleted`

对外查询只读 `current_release`。

这意味着：

- 文件写出来不等于立刻可见
- 当前 release 切换之后，新数据才算对外生效
- 删除、回滚、重建本质上也是在生成新的 release

### 3.6 目录布局

以 `--root ./storage_demo` 为例：

```text
storage_demo/
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
  metadata/
    metadata.db
    runtime_events.jsonl
    journal/
    checkpoints/
    jobs/
      pending/
      running/
      finished/
  quarantine/
    bad_frames/
    bad_timestamps/
  exports/
    pending/
    finished/
```

各目录职责：

- `live/`
  - 当前 release 引用的正式文件
- `staging/`
  - 发布和封口时的临时文件
- `metadata/metadata.db`
  - DuckDB 元数据控制面
- `metadata/journal`
  - release 和运维事件日志
- `quarantine/`
  - 校验失败的帧
- `exports/`
  - 旧的文件型 export 队列和导出结果

## 4. 部署方式

### 4.1 本地 Python 方式

这是最直接也最适合开发联调的方式。

建议使用仓库内虚拟环境：

```bash
python3 -m venv .venv
.venv/bin/pip install -e '.[duckdb,api,cyber]'
```

说明：

- `duckdb` 用于 metadata、Parquet 读写和查询
- `api` 用于 FastAPI / uvicorn
- `cyber` 当前只补了 protobuf 依赖，不会自动安装 Apollo Cyber runtime

常用入口：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli --help
PYTHONPATH=src .venv/bin/python -m dbserver.api.server --help
```

### 4.2 Docker 方式

仓库当前已经提供：

- [Dockerfile](/home/demo/Documents/code/data_storage_study/DBServer/Dockerfile)
- [docker-compose.yml](/home/demo/Documents/code/data_storage_study/DBServer/docker-compose.yml)
- [docker-entrypoint.sh](/home/demo/Documents/code/data_storage_study/DBServer/scripts/docker-entrypoint.sh)

容器入口默认直接启动 `dbserver-api`。

启动方式：

```bash
docker compose up --build -d
```

默认行为：

- 暴露 `8080`
- 数据根目录挂载到 `./docker_data`
- 自动以 API 服务模式启动
- 默认开启 maintenance
- 默认开启统一 job worker
- 默认关闭 cyber ingest

查看状态：

```bash
docker compose ps
docker compose logs -f dbserver
curl http://127.0.0.1:8080/healthz
```

### 4.3 Docker 环境变量

当前镜像主要通过环境变量配置：

- `DBSERVER_ROOT`
- `DBSERVER_API_HOST`
- `DBSERVER_API_PORT`
- `DBSERVER_ENABLE_MAINTENANCE`
- `DBSERVER_ENABLE_CYBER_INGEST`
- `DBSERVER_CYBER_DEVICE_IDS`
- `DBSERVER_CYBER_CHANNEL_TEMPLATE`
- `DBSERVER_CYBER_NODE_NAME`
- `DBSERVER_ENABLE_JOB_WORKER`
- `DBSERVER_ENABLE_EXPORT_WORKER`
- `DBSERVER_PUBLISH_INTERVAL_SECONDS`
- `DBSERVER_MAINTENANCE_INTERVAL_SECONDS`

### 4.4 关于 cyber 部署

这里要特别注意：

- 当前 Docker 镜像是基于 `python:3.12-slim`
- 镜像里没有内置 Apollo Cyber Python runtime
- `DBSERVER_ENABLE_CYBER_INGEST=false` 是默认值

如果你要打开 cyber ingest，需要额外保证运行环境能导入下列模块之一：

- `cyber.python.cyber_py3.cyber`
- `cyber_py3.cyber`
- `cyber`

换句话说：

- 本仓库已经具备 cyber 适配代码
- 但 cyber 运行时不是这个仓库自己打包出来的

## 5. 使用方式

### 5.1 输入格式

#### JSONL 输入

每行一帧，基本格式：

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

样例文件：

- [sample_box_frames.jsonl](/home/demo/Documents/code/data_storage_study/DBServer/examples/sample_box_frames.jsonl)

约束：

- `event_time_ms` 必须是毫秒时间戳
- 一帧至少包含一个 box
- 同一帧内所有 box 的 `device_id / frame_id / event_time_ms` 必须与帧头一致

#### REST 输入

API 使用一批 frame 的请求体：

```json
{
  "source": "rest",
  "frames": [
    {
      "device_id": 1,
      "frame_id": 1001,
      "event_time_ms": 1711922400100,
      "boxes": []
    }
  ]
}
```

#### cyber 输入

cyber 输入不是 JSON，而是 `Boxes` protobuf。

当前适配规则：

- `channel_template` 中的 `{device_id}` 或 `{num}` 映射到 `device_id`
- 一个 `Boxes` 消息解析成一个 `FrameBatch`
- `Boxes.lidar_timestamp(ns)` 转成 `event_time_ms`

### 5.2 快速开始：离线导入

初始化元数据：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  init-metadata
```

导入样例数据：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  ingest-jsonl \
  --input ./examples/sample_box_frames.jsonl
```

查看当前 release：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  current-release
```

查看状态和指标：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  status-json

PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  metrics-text
```

说明：

- `ingest-jsonl` 是离线导入友好的命令
- 它会在当前批次结束后尽量把可发布窗口全部刷出来
- 这和长驻 API 模式的实时 watermark 行为不完全一样

### 5.3 长驻目录轮询模式

如果上游会持续把 JSONL 文件投到某个目录，可以用 `serve`：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  serve \
  --input-dir ./incoming \
  --archive-dir ./incoming_done \
  --failed-dir ./incoming_failed \
  --poll-interval-seconds 2 \
  --run-maintenance \
  --status-port 18080
```

这个模式的特点：

- 持续扫描本地目录
- 处理成功的文件移到归档目录
- 处理失败的文件移到失败目录
- 可选开启 maintenance
- 可选开一个轻量状态 HTTP 端口

适用场景：

- 上游只能落本地文件
- 需要一个轻量单进程常驻服务
- 暂时不需要完整的 REST API

### 5.4 REST API 服务模式

启动服务：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.api.server \
  --root ./storage_demo \
  --host 0.0.0.0 \
  --port 8080
```

服务启动后，会自动：

- 初始化目录和 metadata
- 恢复 orphan staging 文件
- 回收 stale active queries
- 恢复运行中断的 job
- 启动后台 publish loop
- 按配置启动 maintenance / job worker / callback retry

当前 REST API 已实现的接口：

- `GET /healthz`
- `GET /status`
- `GET /v1/status`
- `GET /metrics`
- `GET /v1/metrics`
- `POST /v1/ingest/box-frames`
- `POST /v1/jobs/export`
- `POST /v1/jobs/delete`
- `GET /v1/jobs`
- `GET /v1/jobs/{job_id}`

当前没有提供 REST 查询接口。

也就是说：

- 查询仍然主要通过 CLI 执行
- 对外 HTTP 能力目前集中在 ingest、状态和异步 job

### 5.5 REST API 示例

#### 健康检查

```bash
curl http://127.0.0.1:8080/healthz
```

#### 提交一批 frame

```bash
curl -X POST http://127.0.0.1:8080/v1/ingest/box-frames \
  -H 'Content-Type: application/json' \
  -d '{
    "source": "demo",
    "frames": [
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
    ]
  }'
```

#### 提交导出 Job

```bash
curl -X POST http://127.0.0.1:8080/v1/jobs/export \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "export-001",
    "export_kind": "detail",
    "device_id": 1,
    "start_ms": 1711922400000,
    "end_ms": 1711922999999
  }'
```

#### 提交删除 Job

```bash
curl -X POST http://127.0.0.1:8080/v1/jobs/delete \
  -H 'Content-Type: application/json' \
  -d '{
    "request_id": "delete-001",
    "before_ms": 1711930000000
  }'
```

#### 查看 Job 状态

```bash
curl http://127.0.0.1:8080/v1/jobs
curl http://127.0.0.1:8080/v1/jobs/<job_id>
```

### 5.6 callback 回执

当前 Job API 支持 callback：

- `url`
- `headers`
- `secret`

行为：

- Job 完成后向回调地址发 `POST`
- 会带 `X-DBServer-Job-Id`
- 如果配置了 `secret`，会额外带 `X-DBServer-Signature`
- 签名格式是 `sha256=<hex>`
- 失败会按配置重试

这部分已经有实现，但当前仍属于轻量能力，不等同于完整的企业级任务平台。

### 5.7 cyber 模式启动

如果环境具备 cyber runtime，可以直接用 API 服务模式接 cyber：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.api.server \
  --root ./storage_demo \
  --host 0.0.0.0 \
  --port 8080 \
  --enable-cyber-ingest \
  --cyber-device-ids 1,2,3,4 \
  --cyber-channel-template 'omnisense/test/{device_id}/boxes' \
  --cyber-node-name dbserver_cyber_ingest
```

建议理解为：

- API 服务是主进程
- cyber subscriber 是这个主进程里的一个接入线程
- cyber 收到数据后仍然走和 REST 相同的 ingest/publish/release 链路

### 5.8 常用查询与维护命令

查看查询计划：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  plan-detail-query \
  --device-id 1 \
  --start-ms 1711922400000 \
  --end-ms 1711922999999
```

执行明细查询：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  exec-detail-query \
  --device-id 1 \
  --start-ms 1711922400000 \
  --end-ms 1711922999999
```

执行轨迹查询：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  exec-trace-query \
  --device-id 1 \
  --trace-id 42
```

执行 rollup 查询：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  exec-rollup-query \
  --device-id 1 \
  --start-ms 1711922400000 \
  --end-ms 1711922999999
```

封口已到期小时：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  seal-ready-hours
```

手动封口某个小时：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  seal-hour \
  --date-utc 2024-03-31 \
  --hour-utc 22 \
  --device-id 1
```

预演删除计划：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  plan-delete \
  --before-ms 1711843200000
```

执行删除：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  delete-before \
  --before-ms 1711843200000
```

重建派生数据：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  rebuild-trace-index

PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  rebuild-rollup
```

回滚到历史 release：

```bash
PYTHONPATH=src .venv/bin/python -m dbserver.cli \
  --root ./storage_demo \
  rollback-release \
  --target-release-id <release_id> \
  --retention-floor-ms 1711843200000
```

## 6. 设计思路

### 6.1 一切围绕事件时间

当前实现不是按“接收到数据的时间”分区，而是按 `event_time_ms` 分区和发布。

这样做的目的：

- 上游乱序时仍然能保持时间语义正确
- 查询、封口、删除都能围绕同一时间主轴工作
- 便于后续做稳定的 release 切换

### 6.2 release 驱动可见性

当前实现最核心的设计点是：

- 数据先写出来
- 再通过 metadata 创建 release
- 最后切换 `current_release`

这样带来的好处：

- 查询始终只看一个明确的 release
- 发布、封口、重建、回滚、删除都能复用统一切换模型
- 可以避免“查询半途中看到了混合状态文件”

### 6.3 文件不可变，状态通过切换推进

当前实现尽量把物理文件视作不可变产物：

- 新发布生成新的 file_group
- 小时封口生成新的 compacted file_group
- 删除不是修改旧 release，而是生成新 release 并移除旧 group

这是当前代码能把复杂动作保持相对清晰的关键原因。

### 6.4 数据面和控制面分离

当前仓库明确把两类数据分开：

- 数据面
  - `live/*.parquet`
- 控制面
  - `metadata.db`
  - `journal`
  - `partition_states`
  - `jobs`

好处：

- 查询和写入文件可以保持简单
- release、删除、回滚不需要依赖外部数据库
- 控制面状态足够集中，便于恢复和排障

### 6.5 staging -> live 的发布路径

当前写 Parquet 不是直接写到正式目录，而是：

1. 先写到 `staging`
2. 再 promote 到 `live`
3. 再登记 metadata 并切 release

这能降低半写文件直接暴露给查询的风险。

### 6.6 主表最小化，派生表补能力

当前设计没有把所有查询需求都塞进 `box_info`：

- 明细需求走 `box_info`
- 轨迹段定位走 `trace_index_hot/cold`
- 聚合需求走 `obj_type_rollup`

这样做的目的：

- 主表保持稳定
- 复杂查询通过派生数据补足
- 后续重建也只需要以 `box_info` 为真源

### 6.7 校验失败单独隔离

当前坏数据不会混进正式数据集，而是写进：

- `quarantine/bad_frames`
- `quarantine/bad_timestamps`

这使得系统更像“可继续运行的 pipeline”，而不是“一遇到坏数据就全局停机的导入脚本”。

### 6.8 删除优先保证安全

当前删除链路的思路是：

- 只删 sealed 数据
- 删之前先构建 delete plan
- 删除时把分区标成 `deleting`
- 可以等待活跃查询退出
- 删除完成后推进 retention floor，并生成新的 release

这让删除动作虽然仍然是本地文件删除，但不会完全失控。

## 7. 边界与注意事项

### 7.1 架构边界

当前明确边界如下：

- 单机
- 单写入实例
- 本地磁盘
- 主要围绕 `box_info` 单表
- 没有多副本、高可用、分布式一致性能力

### 7.2 数据接入边界

- JSONL、REST、cyber 都会被转换成统一 `FrameBatch`
- 当前默认最多允许 `4` 台设备同时活跃
- 超过晚到窗口的历史回灌会被拒绝
- 不做严格去重
- 业务纠错/撤回当前不支持

### 7.3 实时性边界

- 实时长驻模式下，数据是否可见取决于 watermark
- 默认晚到窗口 `120s`
- 因此新数据不是“写入即查到”

如果你要做离线回灌或开发自测，推荐：

- 用 `ingest-jsonl`
- 或显式传 `--now-ms`

### 7.4 可靠性边界

- 当前没有 WAL
- 进程异常退出时，最近一段内存缓冲可能丢失
- `memory_loss_budget_seconds=5` 只体现设计意图，当前实现本质仍是内存缓冲

### 7.5 删除边界

- 删除不可回滚
- 删除只能对 sealed 分区生效
- 如果旧文件已经被删，即使历史 release 还记得，也不能再回滚出真实数据

### 7.6 查询边界

- REST API 当前没有对外查询接口
- 查询主要通过 CLI 或 export job 完成
- 对外只保证读 `current_release`

### 7.7 水位删数边界

当前代码中的水位语义要特别注意：

- 只有当总占用 `>= low_watermark_bytes` 时，才触发自动删数
- 一旦触发，会尝试删到 `<= high_watermark_bytes`
- 因此当前实现要求 `high_watermark_bytes <= low_watermark_bytes`

也就是说，虽然名字叫 `low/high`，但实际语义更接近：

- `low_watermark_bytes`
  - 触发阈值
- `high_watermark_bytes`
  - 回落目标

使用时不要按常见“双高于低”的直觉去理解。

### 7.8 安全边界

- 当前没有鉴权
- 当前没有租户隔离
- 当前没有限流
- 当前没有审计系统
- callback 是轻量实现，不是完整消息投递系统

所以当前版本更适合：

- 内网环境
- 开发联调
- 单项目专用服务

不适合直接当作公网生产服务暴露。

### 7.9 cyber 边界

- 当前仓库有 cyber 适配代码
- 但 cyber runtime 不随仓库一起交付
- Docker 默认也不启用 cyber ingest
- 真正上线前，需要先确认目标环境能导入 Cyber Python 模块

## 8. 推荐使用姿势

如果你的目标是离线验证功能，推荐：

1. 本地 `.venv`
2. `init-metadata`
3. `ingest-jsonl`
4. `exec-*` 查询
5. 再测试 `seal`、`delete`、`rollback`

如果你的目标是单机长驻接入服务，推荐：

1. 用 API 服务模式
2. 通过 REST ingest 或 cyber ingest 接入
3. 开启 `run-maintenance`
4. 保持 `run-job-worker` 开启
5. 用 `/healthz`、`/status`、`/metrics` 做观测

如果你的目标是容器部署，推荐：

1. 直接从 `docker-compose.yml` 起步
2. 先跑通 REST ingest
3. 再决定是否真的需要 cyber runtime 注入

## 9. 结论

基于当前代码，DBServer 已经不是纯设计稿，而是一套能跑通完整闭环的初版实现：

- 可以接收数据
- 可以发布 release
- 可以查、导、删、封口、回滚
- 可以以 API 或 CLI 两种方式运行

但它当前仍然是“单机场景下的专用原型/初版工程”，不是“开箱即用的分布式生产数据库”。

如果后续继续演进，最值得优先补强的方向通常会是：

- 写入可靠性
- 部署环境一致性
- 鉴权与限流
- 统一 Job 控制面
- 完整查询 API
- cyber 运行时交付方式
