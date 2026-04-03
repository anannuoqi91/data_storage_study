# 页面：轨迹与聚合查询入口

## 基本信息
- 所属客户端：受控查询端
- 对应故事：US004、US005

## 页面元素
1. 轨迹查询表单：`device_id`、`trace_id`、可选时间范围
2. 聚合查询表单：`device_id`、时间范围、可选 `lane_id` / `obj_type`
3. 轨迹段结果列表
4. 聚合桶结果表格

## API 调用
- `GET /query/trace-lookup`
- `GET /query/rollup`

## 跳转关系
- 查询首页内切换 detail / trace / rollup 三个标签
