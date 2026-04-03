# 页面：运维控制台

## 基本信息
- 所属客户端：运维控制端
- 对应故事：US002、US003、US005、US006

## 页面元素
1. 当前 release 卡片
2. 分区状态表
3. 手动 seal 操作区
4. delete / enforce-watermarks 操作区
5. rollback 操作区
6. rebuild trace_index / rollup 操作区
7. 活跃查询列表

## API 调用
- `GET /status/current-release`
- `GET /status/partitions`
- `POST /ops/seal-ready-hours`
- `POST /ops/delete-before`
- `POST /ops/rollback`
- `POST /ops/rebuild-trace-index`
- `POST /ops/rebuild-rollup`

## 跳转关系
- 控制台首页即总入口
- 高风险操作前必须二次确认
