# 实施指令

## 你需要做的
1. 发布 `box_info` 文件并创建 release
2. 更新 `partition_states`
3. 封口时只处理满足 watermark 的小时分区
4. 删除被 seal 替换的旧物理文件

## 禁止事项
- 不要提前 seal 当前小时
- 不要在未切换 release 前删除旧文件
