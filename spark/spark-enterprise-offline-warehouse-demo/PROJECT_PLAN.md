# Spark 企业级离线数仓 Demo 计划说明

## 1. 项目目标

基于远程服务器 `bigdata` 已有环境，构建一个贴近企业级实践的 Spark 离线数仓 Demo。

本项目的目标不是只做几个样例 SQL，而是尽量模拟企业里常见的离线数仓链路：

- 业务数据先落在 `MySQL`
- 使用 `Spark` 直接从 `MySQL` 抽取数据进入 `ODS`
- 继续通过 `Spark` 完成 `DWD`、`DWS`、`ADS` 分层加工
- 分层结果优先落到 `HDFS`
- 通过统一脚本、统一目录、统一命名规范，体现可维护、可扩展、可调度的工程化思路

本阶段先输出项目计划文档，作为后续建表、开发、联调和验收的依据。

## 2. 当前环境与默认约束

根据远程服务器 `bigdata` 的实际情况，当前可默认使用以下环境：

- 连接方式：`ssh bigdata`
- 主目录：`/opt/module`
- 已确认可用组件：
  - `Spark`：`/opt/module/spark`
  - `Hadoop`：`/opt/module/hadoop-3.1.3`
  - `mysql` 客户端：`/usr/bin/mysql`
- 协作约定：
  - 优先复用现有组件和目录
  - 先检查现状，再修改或新增
  - 非必要不做高风险操作，例如删除历史数据、覆盖关键配置、大规模重启服务

因此，本项目默认按“**本地开发，远程 bigdata 联调运行**”的方式推进。

## 3. 项目定位

这是一个“企业级风格的最小离线数仓闭环”项目，重点体现以下能力：

- 业务建模能力
- 数仓分层设计能力
- Spark 工程化开发能力
- 批处理链路拆分能力
- 可复跑、可回灌、可扩展的任务设计能力

默认不追求一次性做成特别重的生产系统，而是先做出一个结构合理、链路完整、便于扩展的样板工程。

## 4. 业务场景设计

默认采用“电商交易离线数仓”场景。

原因：

- 业务模型通用，容易理解
- 同时具备订单、支付、商品、用户、店铺等典型主题
- 既能体现事实表，也能体现维度表
- 容易设计 ODS、DWD、DWS、ADS 全链路

### 4.1 MySQL 源表规划

建议先在 MySQL 中构建以下业务表：

- `user_info`
  - 用户基础信息
- `product_info`
  - 商品基础信息
- `shop_info`
  - 店铺基础信息
- `order_info`
  - 订单主表
- `order_item`
  - 订单明细表
- `payment_info`
  - 支付记录表

如有需要，后续可以补充：

- `coupon_info`
- `product_category`
- `refund_info`

### 4.2 业务问题导向

最终希望通过数仓回答这些业务问题：

- 每天的下单金额、支付金额、支付转化率是多少
- 各店铺的销量、销售额、客单价如何
- 各商品的销量排行、销售额排行如何
- 新老用户贡献占比如何
- 哪些区域、店铺、品类表现更好

## 5. 数仓分层设计

本项目采用标准的离线数仓四层模型：

- `ODS`
- `DWD`
- `DWS`
- `ADS`

### 5.1 ODS 层

定位：

- 承接源系统原始数据
- 尽量少做业务加工
- 主要解决“可追溯、可复算、可审计”

计划表：

- `ods_user_info`
- `ods_product_info`
- `ods_shop_info`
- `ods_order_info`
- `ods_order_item`
- `ods_payment_info`

设计原则：

- 字段尽量与源表保持一致
- 增加抽取日期分区字段，如 `dt`
- 保留必要的技术字段，如抽取时间、来源标识

### 5.2 DWD 层

定位：

- 对 ODS 明细进行清洗、规范化、业务口径统一
- 形成可直接复用的业务明细事实表和退化维度字段

计划表：

- `dwd_order_detail`
  - 订单明细宽表，关联订单、商品、用户、店铺信息
- `dwd_payment_detail`
  - 支付明细表

建议动作：

- 统一状态字段
- 清洗空值、脏数据、重复数据
- 统一时间字段格式
- 计算明细层金额字段，例如原价金额、优惠金额、实付金额

### 5.3 DWS 层

定位：

- 面向主题汇总
- 为报表与指标分析提供可复用中间层

计划表：

- `dws_trade_day_summary`
  - 每日交易汇总
- `dws_shop_day_summary`
  - 每日店铺交易汇总
- `dws_product_day_summary`
  - 每日商品交易汇总
- `dws_user_day_summary`
  - 每日用户交易汇总

典型指标：

- 下单人数
- 支付人数
- 下单笔数
- 支付笔数
- 下单金额
- 支付金额
- 客单价

### 5.4 ADS 层

定位：

- 面向最终应用和报表
- 指标更贴近业务分析口径

计划表：

- `ads_trade_overview`
  - 交易总览看板
- `ads_top10_products`
  - 商品销量 TopN
- `ads_top10_shops`
  - 店铺销售额 TopN
- `ads_user_growth_analysis`
  - 新老用户贡献分析

## 6. 技术方案规划

### 6.1 计算引擎

- `Spark SQL` 作为主实现方式
- 必要时使用少量 `Spark Core / DataFrame API`

这样做的原因：

- 离线数仓以 SQL 建模最直观
- 更贴近企业中“数仓 SQL + 少量工程代码”的组织方式
- 便于后续接入调度脚本和分任务运行

### 6.2 存储方案

当前阶段默认采用：

- `MySQL`：源数据
- `HDFS`：ODS / DWD / DWS / ADS 结果存储

文件格式建议：

- 优先使用 `Parquet`

原因：

- 列式存储，适合分析型查询
- 与 Spark 兼容性好
- 比 CSV / JSON 更接近生产场景

### 6.3 分区策略

默认采用按天分区：

- `dt=yyyy-MM-dd`

后续根据表的特点可扩展：

- 事实表按 `dt`
- 汇总表按 `dt`
- 若某些 ADS 表为快照表，可按统计日期分区

### 6.4 运行方式

建议将任务拆分为多个 Spark Job，而不是把所有 SQL 写在一个 main 方法里。

默认拆分如下：

1. `MysqlToOdsJob`
2. `OdsToDwdJob`
3. `DwdToDwsJob`
4. `DwsToAdsJob`

这样做的好处：

- 失败隔离更清晰
- 更符合生产环境任务拆分思路
- 方便后续接入调度系统

## 7. 工程结构规划

建议项目目录如下：

```text
spark/spark-enterprise-offline-warehouse-demo
├── PROJECT_PLAN.md
├── README.md
├── pom.xml
├── docs
│   ├── architecture.md
│   └── table-design.md
├── scripts
│   ├── mysql_setup.sql
│   ├── run_all.sh
│   ├── run_ods.sh
│   ├── run_dwd.sh
│   ├── run_dws.sh
│   └── run_ads.sh
├── src/main/java/com/study/offline/common
│   ├── AppConfig.java
│   ├── SparkSessionFactory.java
│   └── JdbcOptions.java
├── src/main/java/com/study/offline/job
│   ├── MysqlToOdsJob.java
│   ├── OdsToDwdJob.java
│   ├── DwdToDwsJob.java
│   └── DwsToAdsJob.java
└── src/main/resources
    └── application.properties
```

工程风格要求：

- 配置外置化
- 作业职责单一
- SQL 与 Job 逻辑尽量分层
- 目录命名与表命名统一

## 8. 数据流转计划

### 8.1 第一步：MySQL 准备模拟业务数据

内容包括：

- 创建 demo 数据库
- 创建业务表
- 初始化基础数据和交易数据

目标：

- 让后续 Spark 任务有明确、稳定的输入

### 8.2 第二步：Spark 读取 MySQL 生成 ODS

实现思路：

- 通过 JDBC 分别读取 MySQL 业务表
- 原样写入 HDFS 对应的 ODS 路径

示例路径：

- `/warehouse/ods/ods_user_info/dt=2026-05-14`
- `/warehouse/ods/ods_order_info/dt=2026-05-14`

### 8.3 第三步：ODS 加工到 DWD

实现重点：

- 订单主表与订单明细表关联
- 订单与商品、店铺、用户关联
- 形成统一口径的订单明细宽表

核心产物：

- `dwd_order_detail`

### 8.4 第四步：DWD 汇总到 DWS

实现重点：

- 按天聚合
- 按商品、店铺、用户等主题汇总
- 形成可复用公共指标层

### 8.5 第五步：DWS 生成 ADS

实现重点：

- 生成最终看板数据
- 生成 TopN 排行
- 生成业务分析表

## 9. 企业级实践模拟点

为了让这个 Demo 更像真实项目，计划在实现时体现以下实践：

- 分层明确，不跨层复用脏口径
- 统一命名规范
- 所有任务支持传入业务日期 `dt`
- 保留可复跑能力
- 脚本化运行，而不是手工拼命令
- 配置与代码分离
- 核心 SQL 与建表脚本可沉淀为文档

如果时间允许，还可以补充：

- 分区覆盖写入与幂等控制
- 任务日志规范
- Spark 参数模板
- 调优建议和运行资源说明

## 10. 开发实施步骤

建议按以下顺序推进：

1. 盘点远程 `bigdata` 上 MySQL、HDFS、Spark 实际可用性
2. 确定业务模型与源表结构
3. 编写 `mysql_setup.sql`
4. 初始化 Spark Maven 工程
5. 实现 `MysqlToOdsJob`
6. 实现 `OdsToDwdJob`
7. 实现 `DwdToDwsJob`
8. 实现 `DwsToAdsJob`
9. 编写统一运行脚本
10. 上传远程服务器联调验证
11. 输出架构文档、表结构文档和使用说明

## 11. 验收标准

本项目完成时，至少满足以下标准：

1. MySQL 中存在完整的模拟业务数据
2. Spark 可以从 MySQL 成功抽取到 ODS
3. ODS、DWD、DWS、ADS 都有实际落地结果
4. 关键指标能够通过 Spark SQL 查询验证
5. 项目具备清晰的目录结构、运行脚本和说明文档
6. 能在 `bigdata` 环境完成端到端运行

## 12. 本阶段输出与下一步

本阶段输出：

- 项目计划文档 `PROJECT_PLAN.md`

下一步建议直接开始以下工作：

1. 远程确认 MySQL 连接信息、数据库权限和 HDFS 路径
2. 创建 Spark 项目骨架
3. 设计 MySQL 源表和初始化脚本
4. 优先打通 `MySQL -> ODS` 最小链路

如果你认可这份计划，下一步我就直接开始：

- 先检查远程 `bigdata` 上的 MySQL / HDFS 细节
- 然后把这个 Spark 离线数仓项目骨架一并搭出来
