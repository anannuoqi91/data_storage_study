# 现状快照

## 项目定位
- 当前仓库是 `DBServer` 第一版原型，围绕 `box_info` 单表实现本地磁盘存储、发布、封口、删数和受控查询。
- 技术路线已经固定为 `Parquet + DuckDB + ZSTD + 单机单写实例`。

## 技术栈
- 语言：Python 3.12
- 元数据：DuckDB
- 明细存储：Parquet
- 运行入口：CLI（`dbserver.cli`）+ FastAPI（`dbserver.api.server`）

## 已有能力
- `init-metadata` 初始化元数据表和 schema 版本。
- `ingest-jsonl`、`run-ingest-loop`、`serve` 支持接收 JSONL 样例输入、校验、缓冲和 10 分钟批次发布。
- `seal-ready-hours`、`seal-hour` 支持小时封口。
- `plan-delete`、`delete-before`、`enforce-watermarks` 支持 retention 删除与水位清理。
- `plan-detail-query`、`exec-detail-query`、`plan-trace-query`、`exec-trace-query` 支持明细查询。
- `status-json`、`metrics-text` 提供一次性状态和指标输出；`serve --status-port` 在允许绑定端口的环境下提供 `/healthz`、`/status`、`/metrics`。
- `submit-detail-export`、`submit-trace-export`、`submit-rollup-export`、`run-export-queue`、`list-exports` 提供异步导出队列，输出 `Parquet` 到 `exports/finished/`。
- 元数据里已有 `schema_versions`、`database_releases`、`file_groups`、`partition_states`、`active_queries`、`current_release`。
- `release journal`、checkpoint、运行时日志已落地。
- `tests/test_cli_smoke.py` 提供端到端 smoke，覆盖 ingest、query、maintenance、trace cooling 和 observability 基础行为。

## 本轮补齐后的能力
- 发布时自动生成 `trace_index` 和 `obj_type rollup` 派生文件。
- `run-maintenance`、`serve --run-maintenance`、`run-ingest-loop --run-maintenance` 会自动将超过热窗口的 `trace_index_hot` 批量转为 `trace_index_cold`。
- 新增 `list-releases`、`rollback-release`、`plan/exec-trace-lookup-query`、`plan/exec-rollup-query`、`rebuild-trace-index`、`rebuild-rollup`。
- 新增状态/指标接口：单次命令 `status-json`、`metrics-text`，以及 `serve` 内置的 HTTP observability 端点。
- 新增异步导出任务队列，导出任务在创建时固定 `release_id`，串行导出到 `exports/finished/`，格式为 `Parquet`。
- 修正 `seal-ready-hours` 的触发时机，使其符合 `hour_end + late_window`。
- `seal-ready-hours`、`seal-hour` 现在会同步把命中小时分区内的 `trace_index_hot/cold` 和 `obj_type rollup` 重写为最终小时级 sealed 文件。
- 删除计划会一并覆盖派生数据文件组，避免 `trace_index/rollup` 成为保留期之外的孤儿数据。
- 新增 `dbserver.api.server` FastAPI 入口，提供 `/healthz`、`/status`、`/metrics`、`POST /v1/ingest/box-frames`，并内置后台 publish loop。
- 新增 REST Job 控制面：`POST /v1/jobs/export`、`POST /v1/jobs/delete`、`GET /v1/jobs`、`GET /v1/jobs/{job_id}`，后台 job worker 会自动处理 export/delete 任务。
- 新增任务 callback 回执机制，支持自定义 header、共享 secret 的 `HMAC-SHA256` 签名，以及失败重试。
- 新增 cyber DDS 接入适配骨架：支持创建 cyber node、按 `omnisense/test/{device_id}/boxes` 订阅 `Boxes` protobuf，并映射到统一 `FrameBatch`。
- `dbserver.api.server` 已支持通过启动参数或环境变量打开 cyber ingest，接收后的数据直接进入现有 validator / ingest / publish 链路。
- 新增 Docker 化部署骨架：`Dockerfile`、`docker-compose.yml`、`scripts/docker-entrypoint.sh`。
- `tests/test_api_server.py` 现已覆盖 REST ingest、export job、delete job 和 callback 闭环。
- `tests/test_cyber_adapter.py` 覆盖 protobuf 映射和 fake cyber runtime 下的订阅回调行为。

## 现存缺口
- 自动化测试已经有 smoke 基线，但还没有更细粒度失败注入和回归集。
- Job 控制面当前仍是单机本地文件队列，没有迁到 `metadata.db`。
- 还没有 API 鉴权、幂等 token 独立存储、限流和多实例抢占控制。
- 当前开发环境没有真实 cyber runtime，尚未做“真实 DDS 总线 + 真 reader”集成验证。
