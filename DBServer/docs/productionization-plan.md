# DBServer 可部署化改造方案

## 1. 目标

本方案解决三个明确目标：

1. 支持 Docker 安装，安装后默认自动运行，默认即可对外接收数据。
2. 接收到数据后进入现有入库、发布、封口、派生数据维护链路。
3. 通过 REST API 接收外部的删除和导出指令，并在完成后主动通知对方。

## 2. 当前实现与差距

当前仓库已经具备这些基础：

- CLI 单次命令和长驻模式，入口在 `src/dbserver/cli.py`
- 本地目录轮询 ingest，长驻命令为 `serve`
- metadata、release、file_group、partition_state 模型已经可用
- status/metrics HTTP 端点已经有最小实现
- export 已经有本地异步队列
- maintenance、seal、retention、rollback 已经能跑通

当前离目标的主要差距：

- 没有 Docker 化部署产物
- 没有正式 API 服务框架，只有状态 HTTP 端点
- 没有默认对外数据接入协议
- delete/export 还不是统一 Job 系统
- 没有 callback 回执能力
- 重启恢复、幂等、鉴权、限流、任务审计都还没有

## 3. 目标架构

建议把系统拆成 5 层：

### 3.1 App 层

负责进程生命周期。

- 启动配置加载
- metadata 初始化
- orphan 恢复
- worker 启停
- API server 启停
- 单实例写锁
- 优雅停机

建议新增：

- `src/dbserver/app.py`
- `src/dbserver/bootstrap.py`

### 3.2 API 层

对外提供正式 REST API。

- ingest API
- delete/export job API
- job 查询 API
- status/metrics API

建议新增：

- `src/dbserver/api/server.py`
- `src/dbserver/api/routes_ingest.py`
- `src/dbserver/api/routes_jobs.py`
- `src/dbserver/api/routes_status.py`
- `src/dbserver/api/schemas.py`
- `src/dbserver/api/deps.py`

技术选型建议：

- `FastAPI`
- `uvicorn`
- `pydantic`

原因：

- 方便定义请求/响应模型
- OpenAPI 自动生成
- 适合后续补鉴权和回调协议

### 3.3 接入适配层

统一入口接口已经有雏形：

- [base.py](/home/demo/Documents/code/data_storage_study/DBServer/src/dbserver/adapters/base.py)

这里要补两个方向：

- `RestFrameSourceAdapter` 或直接 `IngestApiService`
- 未来的 `DdsFrameSourceAdapter`

建议新增：

- `src/dbserver/adapters/rest.py`
- `src/dbserver/services/ingest_api_service.py`

原则：

- 外部协议和内部入库链路解耦
- 任何输入源最终都转换成现有 `FrameBatch`
- REST 只是第一个 adapter，不是唯一 adapter

### 3.4 Job 层

统一管理 delete/export/rebuild/rollback 这类异步任务。

建议新增：

- `src/dbserver/services/job_service.py`
- `src/dbserver/services/job_worker.py`
- `src/dbserver/services/callback_service.py`

职责：

- 创建 job
- 抢占 job
- 执行 job
- 更新状态
- 失败重试
- callback 投递

### 3.5 核心服务层

继续复用现有服务：

- `ingest_service.py`
- `publish_service.py`
- `seal_service.py`
- `maintenance_service.py`
- `retention_service.py`
- `export_service.py`
- `ops_service.py`

但要做两类改动：

- 从 CLI 调用模式改成 API/worker 可复用模式
- 去掉“必须由命令驱动”的假设

## 4. 进程模型

建议采用单容器单进程主模型，但在进程内有多个线程或协程组件：

1. API server
2. ingest publish loop
3. maintenance loop
4. job worker loop
5. callback retry loop

建议默认启动行为：

1. 读取环境变量
2. 初始化目录和 metadata
3. 恢复 orphan、恢复 stale queries
4. 扫描 `jobs` 表，把 `running` 状态恢复成 `pending_retry` 或 `pending`
5. 启动 API server
6. 启动 worker
7. 开始提供服务

这样 Docker 安装后，容器一启动就可接请求。

## 5. REST API 设计

建议第一版对外暴露这些 API：

### 5.1 Ingest API

`POST /v1/ingest/box-frames`

用途：

- 外部系统直接提交一批 frame

请求体建议：

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

响应建议：

```json
{
  "accepted": true,
  "frame_count": 1,
  "box_count": 3,
  "received_at_ms": 1711922400999
}
```

说明：

- 默认同步接收、异步发布
- API 只负责入内存缓冲或内部队列
- 不要求请求返回时已经生成 release

### 5.2 Export Job API

`POST /v1/jobs/export`

请求体建议：

```json
{
  "request_id": "biz-export-001",
  "job_type": "export",
  "export_kind": "detail",
  "device_id": 1,
  "start_ms": 1711922400000,
  "end_ms": 1711922999999,
  "callback": {
    "url": "https://partner.example.com/callbacks/dbserver",
    "secret": "xxx"
  }
}
```

响应建议：

```json
{
  "job_id": "job_xxx",
  "status": "pending",
  "accepted_at_ms": 1711922400999
}
```

### 5.3 Delete Job API

`POST /v1/jobs/delete`

请求体建议：

```json
{
  "request_id": "biz-delete-001",
  "job_type": "delete",
  "before_ms": 1711843200000,
  "callback": {
    "url": "https://partner.example.com/callbacks/dbserver",
    "secret": "xxx"
  }
}
```

说明：

- delete 必须异步
- 内部执行仍先做 plan，再做 delete
- callback 返回最终结果

### 5.4 Job 查询 API

`GET /v1/jobs/{job_id}`

返回：

- 当前状态
- 创建时间
- 开始时间
- 结束时间
- 结果摘要
- 失败错误
- callback 状态

### 5.5 Status API

保留并升级：

- `GET /healthz`
- `GET /status`
- `GET /metrics`

新增：

- `GET /v1/releases`
- `GET /v1/partitions`

## 6. Job 模型改造

当前 export 使用文件系统队列，不够支撑 delete/export/callback 的统一控制面。

建议把 Job 主状态迁移到 metadata.db。

### 6.1 新增表

建议新增：

#### `jobs`

字段建议：

- `job_id`
- `job_type`
- `request_id`
- `status`
- `created_at_ms`
- `started_at_ms`
- `finished_at_ms`
- `priority`
- `requested_by`
- `request_payload_json`
- `result_payload_json`
- `error_code`
- `error_message`
- `retry_count`
- `next_retry_at_ms`
- `bound_release_id`

说明：

- `request_id` 用于幂等
- `bound_release_id` 用于像 export 这类需要绑定当前 release 的任务

#### `job_callbacks`

字段建议：

- `job_id`
- `callback_url`
- `callback_headers_json`
- `callback_secret`
- `callback_status`
- `callback_attempt_count`
- `last_callback_at_ms`
- `last_callback_http_status`
- `last_callback_error`

#### `job_events`

字段建议：

- `job_id`
- `seq_no`
- `event_type`
- `payload_json`
- `created_at_ms`

### 6.2 状态机

建议统一状态：

- `pending`
- `running`
- `succeeded`
- `failed`
- `callback_pending`
- `callback_running`
- `callback_succeeded`
- `callback_failed`
- `cancelled`

更简单也可以拆成两套状态：

- job 执行状态
- callback 投递状态

我更推荐后者，逻辑更清楚。

### 6.3 幂等

REST 创建 job 必须支持 `request_id` 或 `Idempotency-Key`。

规则建议：

- 同一个调用方 + 同一个 `request_id` + 同一种 job_type
- 已存在时直接返回原 job
- 不再重复创建 delete/export

## 7. 回调设计

“完成后回复对方”不要通过长连接等待，应该做 callback。

### 7.1 建议协议

任务完成后，POST 到对方指定 URL。

请求体建议：

```json
{
  "job_id": "job_xxx",
  "request_id": "biz-export-001",
  "job_type": "export",
  "status": "succeeded",
  "finished_at_ms": 1711922500000,
  "result": {
    "output_path": "/data/exports/finished/xxx.parquet",
    "row_count": 123
  },
  "error": null
}
```

### 7.2 签名

建议增加头：

- `X-DBServer-Signature`
- `X-DBServer-Timestamp`

签名算法建议：

- `HMAC-SHA256(secret, timestamp + "." + body)`

### 7.3 重试策略

建议：

- 指数退避
- 最多 10 次
- 4xx 默认不重试，5xx/网络错误重试

## 8. 数据接入链路改造

要实现“安装后就能接到数据”，需要把当前 `JsonlFrameSource` 方案升级成统一 ingest pipeline。

### 8.1 保留现有核心

这些逻辑继续复用：

- Validator
- Ingest buffer
- Publish
- Derived data build
- Seal

### 8.2 补一个 API ingest 入口

建议新增 `IngestApiService`：

- 接收一批 `FrameBatch`
- 逐帧调用校验逻辑
- 合法帧放入现有 `IngestService`
- 非法帧写 quarantine
- 决定是否在请求结束前触发一次轻量 publish

### 8.3 发布策略

有两种可选模式：

- 模式 A：每次 API ingest 后立即尝试 publish ready windows
- 模式 B：后台定时 publish

建议第一版：

- ingest 请求只负责写 buffer
- 后台每 1 秒或 2 秒尝试 publish ready windows

原因：

- API 响应时间更稳定
- 后续接 DDS/WebSocket 也能复用

## 9. 配置模型改造

当前配置只有本地路径和运行时参数，[config.py](/home/demo/Documents/code/data_storage_study/DBServer/src/dbserver/config.py)。

需要新增：

- API 监听 host/port
- 鉴权 token 或 HMAC secret
- job worker 并发度
- callback 超时
- callback 最大重试次数
- publish 循环周期
- maintenance 循环周期
- 是否启用 ingest API
- 是否启用 callback
- 数据根目录

建议：

- 保持 `AppConfig`
- 新增 `ApiSettings`
- 新增 `WorkerSettings`
- 新增 `SecuritySettings`
- 支持从环境变量读取

## 10. Docker 化改造

需要新增这些文件：

- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `scripts/docker-entrypoint.sh`

### 10.1 Dockerfile 要求

- 使用 `python:3.12-slim`
- 安装项目和 `duckdb`
- 创建非 root 用户
- 默认工作目录 `/app`
- 默认数据目录 `/data`
- 默认命令启动 API server

### 10.2 compose 要求

至少提供：

- 一个 `dbserver` 服务
- 一个宿主卷映射 `/data`
- 暴露 API 端口
- 健康检查
- 环境变量模板

### 10.3 默认启动命令

建议：

```bash
python -m dbserver.api.server --root /data
```

或者：

```bash
dbserver-api --root /data
```

## 11. 安全与鉴权

第一版最少要补：

- API Token 鉴权
- callback 签名
- 请求体大小限制
- 基础限流

建议：

- 内网部署先用静态 Bearer Token
- 外部回调用 HMAC secret

不要把 delete/export 暴露成无鉴权接口。

## 12. 可观测性

当前已有 status/metrics/runtime log，但还不够生产使用。

要补：

- API 请求日志
- job 生命周期日志
- callback 投递日志
- 请求量、错误率、耗时指标
- ingest 接收帧数、拒绝帧数指标
- callback 成功率指标
- job backlog 指标

## 13. 测试补齐

至少要新增 4 类测试：

### 13.1 API 集成测试

- ingest API 可接收合法请求
- 非法请求返回 4xx
- delete/export 可创建 job

### 13.2 Job 恢复测试

- worker 跑到一半进程退出
- 重启后 job 能恢复

### 13.3 Callback 测试

- 成功回调
- 5xx 重试
- 超时重试
- 签名正确

### 13.4 Docker smoke

- 镜像构建成功
- 容器启动自动 init
- `/healthz` 正常
- ingest API 正常

## 14. 推荐迭代顺序

建议按 5 个阶段做，不要一次把所有东西摊平。

### 阶段 A：守护进程与 Docker 化

目标：

- 有单独 API 进程入口
- Docker 可以直接启动
- 容器启动后自动初始化并保持运行

交付：

- `Dockerfile`
- `docker-compose.yml`
- `app.py`
- API server 空骨架

验收：

- `docker compose up` 后 `/healthz` 可用

### 阶段 B：REST ingest

目标：

- 安装后默认可接收外部数据

交付：

- `POST /v1/ingest/box-frames`
- API schema
- 后台 publish loop

验收：

- POST 一批 frame 后，能查到 release 和明细

### 阶段 C：统一 Job 系统

目标：

- export/delete 从 CLI 驱动改成 API + DB jobs 驱动

交付：

- `jobs` 表
- `job_worker`
- `POST /v1/jobs/export`
- `POST /v1/jobs/delete`
- `GET /v1/jobs/{job_id}`

验收：

- API 提交任务后能异步完成

### 阶段 D：callback 与安全

目标：

- 任务完成后可靠回调对方

交付：

- `job_callbacks` 表
- `callback_service`
- API Token
- callback HMAC

验收：

- export/delete 成功后对方能收到签名回调

### 阶段 E：生产补强

目标：

- 稳定运行

交付：

- 结构化日志
- 恢复机制
- Docker smoke
- API 集成测试
- 失败注入测试

## 15. 建议的新增文件清单

建议新增这些主要文件：

- `src/dbserver/app.py`
- `src/dbserver/bootstrap.py`
- `src/dbserver/api/server.py`
- `src/dbserver/api/routes_ingest.py`
- `src/dbserver/api/routes_jobs.py`
- `src/dbserver/api/routes_status.py`
- `src/dbserver/api/schemas.py`
- `src/dbserver/services/ingest_api_service.py`
- `src/dbserver/services/job_service.py`
- `src/dbserver/services/job_worker.py`
- `src/dbserver/services/callback_service.py`
- `src/dbserver/metadata/job_store.py`
- `src/dbserver/metadata/migrations.py`
- `Dockerfile`
- `.dockerignore`
- `docker-compose.yml`
- `scripts/docker-entrypoint.sh`
- `tests/test_api_ingest.py`
- `tests/test_job_worker.py`
- `tests/test_callback_service.py`
- `tests/test_docker_smoke.py`

## 16. 需要你尽快拍板的业务决策

下面这些决定会直接影响实现顺序：

1. ingest API 是同步写 buffer 还是同步写 release
2. delete/export 是否一律异步 job
3. callback 是否必须，还是允许轮询替代
4. 鉴权第一版用静态 Token 还是网关透传
5. 导出结果是否允许只返回容器内路径，还是必须支持对象存储上传

当前建议是：

- ingest API 同步写 buffer，异步 publish
- delete/export 一律异步 job
- callback 必须支持，同时保留 job 查询接口
- 第一版先用静态 Bearer Token
- 导出结果第一版先落本地文件，后续再扩对象存储

## 17. 下一轮实施建议

如果按最短闭环推进，下一轮应该直接做阶段 A + B：

1. 引入 FastAPI
2. 抽出 `app.py`
3. 新增 `POST /v1/ingest/box-frames`
4. 新增 `Dockerfile` 和 `docker-compose.yml`
5. 让容器启动后默认可运行并可接 ingest 请求

这是第一个真正“可部署”的里程碑。
