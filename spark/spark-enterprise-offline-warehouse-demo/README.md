# Spark Enterprise Offline Warehouse Demo

企业级风格的 Spark 离线数仓 Demo。

当前最小闭环：

- MySQL 业务表准备模拟数据
- Spark JDBC 读取 MySQL 写入 ODS
- Spark 从 ODS 加工 DWD / DWS / ADS
- 本地先跑通，再上传 `bigdata` 做 Spark on YARN 联调

详细规划见 [PROJECT_PLAN.md](./PROJECT_PLAN.md)。
