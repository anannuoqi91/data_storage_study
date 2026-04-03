项目画像
  DBServer 不是通用数据库，而是一个围绕 box_info 单表的单机受控数据服务：接收 JSONL、REST、cyber Boxes 数据，写入 Parquet，用 DuckDB 维护 metadata，通过 release 切换控制“当前可见数据”，
  并提供查询、导出、删数、封口、回滚和运维能力。docs/user-manual.md:19 README.md:6 .sdd/snapshot.md:3

  它的最终形态已经比较清晰：常驻运行时以 FastAPI 服务为主，内部带 publish loop、maintenance、job worker、callback retry 和可选 cyber subscriber；CLI 保留做离线导入、查询和运维；Docker/
  compose 已经是默认部署骨架。docs/user-manual.md:99 docs/user-manual.md:528 src/dbserver/api/server.py:89 src/dbserver/app.py:484 Dockerfile:1 docker-compose.yml:1

  但它当前仍是“单机场景下的专用原型/初版工程”，不是分布式生产数据库。主要缺口是：没有 WAL，没有鉴权/限流，Job 仍是本地文件队列，没有 REST 查询接口，真实 cyber runtime 还没做实总线验证。
  docs/user-manual.md:913 docs/user-manual.md:925 docs/user-manual.md:948 .sdd/snapshot.md:43 src/dbserver/services/job_service.py:183

  实施前需要掌握的知识

  - 数据语义：box_info 字段、event_time_ms、UTC 分区、10 分钟发布窗口、2 分钟晚到、小时封口、删除不可回滚、release/file_group/partition_state 模型。docs/user-manual.md:122 docs/user-
    manual.md:202
  - 数据工程：Parquet 布局、DuckDB metadata、staging -> live -> release 发布路径、trace_index_hot/cold 和 obj_type_rollup 的派生与重建。docs/user-manual.md:153 docs/user-manual.md:837
  - 后端服务：Python 3.12、FastAPI/Uvicorn、Pydantic schema、后台线程循环、异步 job、callback HMAC、结构化日志、Prometheus 风格指标。
  - 接入集成：JSONL 回放、REST ingest、protobuf/cyber 消息映射，以及 cyber runtime 不随仓库交付这一现实约束。docs/user-manual.md:365
  - 运维与风险：单机单写限制、无 WAL 的丢数窗口、磁盘水位删数策略、回滚边界、无鉴权时的暴露风险。docs/user-manual.md:882

  建议的工作规划

  1. 先拍板边界：是否必须支持 cyber、是否需要 REST 查询 API、实时性 SLA、可接受的数据丢失窗口、鉴权方式、导出结果是本地文件还是对象存储。
  2. 做基线验证：本地跑通 CLI、API、Docker 三条路径，确认 ingest、publish、query、export、delete、seal、rollback 全链路。
  3. 落接入链路：定义上游帧协议和字段映射，补齐坏数据隔离、晚到数据、幂等与回放策略。
  4. 固化部署方案：容器镜像、挂载目录、环境变量、健康检查、日志采集、监控告警、磁盘容量阈值。
  5. 补生产化短板：鉴权、限流、Job 持久化到 metadata.db、失败注入测试、真实 cyber runtime 集成、恢复演练。
  6. 建运维手册：封口、删除、回滚、重建、告警处理、容量治理、版本升级和数据恢复流程。
  7. 最后做试运行：接真实或准真实数据，做性能压测、稳定性观察和灰度验收，再决定是否进入更正式的生产环境。