# Spark 离线数仓 Demo 全流程记录

## 1. 本次目标

完成一个企业级风格的 Spark 离线数仓 Demo，并按以下顺序完成验证：

1. 远程环境确认
2. 官方 Spark on YARN Demo 验证
3. 本地代码开发与本地全链路测试
4. 打包上传到远程服务器
5. 在远程服务器逐个任务 `spark-submit`
6. 校验 ODS / DWD / DWS / ADS 全链路结果

## 2. 远程环境确认

目标服务器：

- `ssh bigdata`

远程上下文文件：

- `/opt/module/codex_context_prompt.txt`

确认到的关键环境：

- `Spark`：`/opt/module/spark`
- `Hadoop`：`/opt/module/hadoop-3.1.3`
- `JDK`：`/opt/module/jdk1.8.0_202`
- `mysql` 客户端可用
- `HDFS` 可用
- `YARN` 可用

关键检查结果：

- `NameNode`、`DataNode`、`ResourceManager`、`NodeManager` 正常运行
- `HDFS` 根目录可访问
- `MySQL 8.0.46` 可登录
- 本机可直连远程 `3306`

## 3. Spark on YARN 官方 Demo 验证

验证命令使用 Spark 自带示例：

- `org.apache.spark.examples.SparkPi`

验证过程中的关键发现：

- 第一次提交卡在 `ACCEPTED`
- 根因不是 Spark 故障，而是 YARN `default` 队列的 `AM resource limit` 太小，同时当时有其他应用占用了 AM 资源
- 资源释放后，`SparkPi` 成功运行

最终验证结果：

- 应用：`application_1778724721626_0007`
- 最终状态：`SUCCEEDED`
- 日志结果：`Pi is roughly 3.1395351395351394`

结论：

- 当前 `bigdata` 环境上的 `Spark on YARN` 可以跑通

## 4. 本地开发过程

项目目录：

- `spark/spark-enterprise-offline-warehouse-demo`

实现内容：

- `pom.xml`
- 配置文件 `application.properties`
- MySQL 初始化脚本 `scripts/mysql_setup.sql`
- 四个分层任务：
  - `MysqlToOdsJob`
  - `OdsToDwdJob`
  - `DwdToDwsJob`
  - `DwsToAdsJob`
- 本地校验任务：
  - `LocalVerificationRunner`

设计原则：

- 打成一个 fat jar
- 每层一个独立 main 类
- 运行时按任务分开提交，便于后续接入调度工具

## 5. 本地测试过程

### 5.1 本地测试方式

由于本机没有系统级 `spark-submit`，本地验证采用：

- `Java 8 + java -cp + SparkSession(local[*])`

### 5.2 本地遇到的问题

问题 1：

- `provided` 依赖导致 `mvn exec:java` 直接运行时找不到 Spark 类

处理：

- 改用 `dependency:build-classpath + java -cp`

问题 2：

- `DWD` 按分区路径读取时，`dt` 列不会自动带回
- `DwdToDwsJob` 中按 `dt` 聚合时报 `UNRESOLVED_COLUMN`

处理：

- 在 `DwdToDwsJob` 和 `DwsToAdsJob` 中，读取分区路径后用 `withColumn("dt", lit(bizDate))` 补回业务日期

问题 3：

- 本机默认 `Java 21`
- Spark 3.4.1 在该启动方式下出现 `DirectByteBuffer` 相关兼容问题

处理：

- 本地构建、运行、校验全部强制使用 `Java 8`

### 5.3 本地测试结果

本地 `.local-warehouse` 已完整产出：

- `ODS`
- `DWD`
- `DWS`
- `ADS`

本地校验关键结果：

- `ads_trade_overview_sample = [4,4,906.00,707.00,226.50]`

含义：

- 下单数：`4`
- 下单人数：`4`
- 下单金额：`906.00`
- 支付金额：`707.00`
- 平均客单价：`226.50`

## 6. 打包与上传策略

用户明确要求：

- 先完整上传
- 上传完成后再统一校验 jar 是否正确

因此实际执行策略为：

1. 本地 Maven 打包 fat jar
2. 上传 jar 和脚本到远程目录
3. 上传结束后统一校验
4. 校验通过后再执行任务

远程部署目录最终采用：

- `/opt/module/spark/usrlib/spark-enterprise-offline-warehouse-demo`

上传方式：

- 由于 `scp` 在当前环境下不稳定，最终采用 `tar | ssh tar` 方式传输

## 7. 上传后远程校验

校验内容：

- 文件存在
- 文件大小正常
- 本地 / 远端 `md5` 一致
- `jar tf` 能正常列出 jar 内容

关键校验结果：

- 本地 jar md5：`e9685db9bc0b82ab7acca36967b40287`
- 远端 jar md5：`e9685db9bc0b82ab7acca36967b40287`

说明：

- 上传后的 jar 与本地构建产物完全一致

## 8. 远程逐任务提交流程

远程先执行：

- MySQL 初始化脚本
- HDFS 目标目录清理与重建

目标 HDFS 路径：

- `hdfs://bigdata:9000/warehouse/spark_offline_demo`

### 8.1 ODS 任务

主类：

- `com.study.offline.job.MysqlToOdsJob`

应用：

- `application_1778724721626_0008`

状态：

- `SUCCEEDED`

### 8.2 DWD 任务

主类：

- `com.study.offline.job.OdsToDwdJob`

应用：

- `application_1778724721626_0009`

状态：

- `SUCCEEDED`

### 8.3 DWS 任务

主类：

- `com.study.offline.job.DwdToDwsJob`

应用：

- `application_1778724721626_0010`

状态：

- `SUCCEEDED`

### 8.4 ADS 任务

主类：

- `com.study.offline.job.DwsToAdsJob`

应用：

- `application_1778724721626_0011`

状态：

- `SUCCEEDED`

## 9. 远程产出验证

### 9.1 HDFS 分层目录

已验证以下分层目录存在：

- `/warehouse/spark_offline_demo/ods`
- `/warehouse/spark_offline_demo/dwd`
- `/warehouse/spark_offline_demo/dws`
- `/warehouse/spark_offline_demo/ads`

各层核心表都存在 `dt=2026-05-14` 分区文件。

### 9.2 远程校验程序结果

在 `bigdata` 上使用 `LocalVerificationRunner` 直接读取 HDFS 做了最终校验。

关键结果：

- `ods/ods_user_info = 4`
- `ads/ads_user_growth_analysis = 2`
- `ads_trade_overview_sample = [4,4,906.00,707.00,226.50]`

从日志与产物可确认：

- ODS 成功落地
- DWD 成功落地
- DWS 成功落地
- ADS 成功落地

## 10. 最终结论

本次 Spark 离线数仓 Demo 已完成以下闭环：

1. 环境确认完成
2. Spark on YARN 官方 Demo 验证通过
3. 本地开发完成
4. 本地全链路测试通过
5. jar 打包上传完成
6. 上传后 jar 校验通过
7. 远程四个任务分别 `spark-submit` 成功
8. ODS / DWD / DWS / ADS 结果均已验证

## 11. 当前推荐运行方式

当前最推荐的生产风格使用方式是：

- 一个 fat jar
- 多个独立 main 类
- 调度工具分别调度：
  - `MysqlToOdsJob`
  - `OdsToDwdJob`
  - `DwdToDwsJob`
  - `DwsToAdsJob`

这样更适合：

- 失败重跑
- 分层解耦
- 调度编排
- 资源隔离

## 12. 后续可继续增强

后续如果继续演进，可以补充：

- README 运行手册
- 建表设计文档
- 指标口径文档
- DolphinScheduler / Airflow 调度示例
- 增量抽取策略
- 分区覆盖与幂等控制
- Hive Metastore / 外表接入
