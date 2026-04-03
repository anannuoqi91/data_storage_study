# 流程

1. 从 rollup 文件中读取聚合桶
2. 构建 delete plan
3. 等待冲突查询退出
4. 切换 release 并删除文件
