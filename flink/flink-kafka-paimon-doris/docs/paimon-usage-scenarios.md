# Paimon 使用场景全景

Apache Paimon 是一套流式数据湖存储，核心能力是把“实时写入”和“批量查询”统一在同一个表上。本文覆盖 Paimon 在当前技术栈中的主要使用场景，并结合本项目的 Kafka → Paimon → Doris 链路给出推荐选型。

> 每个场景都对应 Paimon 的一种或多种表类型/配置组合，建议按需取用，而不是把所有开关都打开。

---

## 1. 实时数据入湖（Streaming Lakehouse）

**一句话**：Kafka 数据实时落入 Paimon，形成"可查询的实时数仓一层"。

### 典型链路

```
Kafka → Flink SQL → Paimon → Trino/Spark/Doris 查询
```

### 特点

- Paimon 承担 ODS/DWD 层沉淀，替代 Hive 小时级分区
- 分钟级可见性，写入后立即可查
- 支持主键去重、Append-Only 两种模式

### 适用表类型

| 场景 | 表类型 | merge-engine |
|------|--------|-------------|
| Kafka 日志、埋点、事件 | Append-Only | 不设主键 |
| Kafka 业务数据、需要去重 | 主键表 | `deduplicate` |

### 本项目中

这是本项目的基线场景——DataProducer 写入 Kafka，Flink SQL 消费并写入 Paimon ODS 层。

---

## 2. MySQL CDC 数据入湖（Change Data Capture）

**一句话**：把 MySQL 的变更日志通过 Flink CDC 实时同步到 Paimon，实现"实时 + 历史"统一查询。

### 典型链路

```
MySQL Binlog → Flink CDC → Paimon
```

### 特点

- Paimon 主键表天然对齐 MySQL 主键
- 自动处理 INSERT / UPDATE / DELETE
- 保留全量 + 增量 changelog
- 替代 Canal → Kafka → Hive 的多跳链路

### 关键配置

```sql
CREATE TABLE mysql_orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2),
    status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'deduplicate',
    'changelog-producer' = 'input'
);
```

- `deduplicate`：同主键保留最新一条
- `changelog-producer = input`：保留完整变更流，供下游流式消费

---

## 3. Partial-Update：宽表拼接

**一句话**：多个上游各自更新宽表中的一部分列，Paimon 按主键合并成完整结果。

### 场景举例

- 用户画像宽表（基础信息流 + 行为特征流）
- 商品画像宽表（商品信息 + 库存 + 价格）
- 订单宽表（订单基础信息 + 支付信息 + 物流信息）

### 详见

本项目已有一篇详细文档：

→ [Paimon Partial-Update 场景文档：用户画像宽表](./partial-update-user-profile.md)

---

## 4. Lookup Join：维度表关联

**一句话**：把 Paimon 当成 Flink 流计算中的"实时维度表"来做 Lookup Join，替代 HBase/Redis 外部 KV。

### 典型场景

```
事实流（Kafka）──→ Flink ──→ 宽表结果
                         │
                    Lookup Join
                         │
               Paimon 维度表（实时更新）
```

### 为什么不用 HBase/Redis

- Paimon 维度表天然与数仓统一存储，不引入额外系统
- 维度变更历史可追溯（Time Travel）
- 同一张表既能做 Lookup Join，也能做批查询

### 关键配置

```sql
-- 维度表建在 Paimon 中
CREATE TABLE dim_user (
    user_id BIGINT,
    name STRING,
    level STRING,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH ('merge-engine' = 'deduplicate');

-- Flink SQL 中做 Lookup Join
SELECT
    o.order_id,
    o.amount,
    d.name,
    d.level
FROM kafka_orders o
LEFT JOIN dim_user /*+ OPTIONS('lookup'='true') */ FOR SYSTEM_TIME AS OF o.proctime AS d
    ON o.user_id = d.user_id;
```

---

## 5. 流批一体查询

**一句话**：同一张 Paimon 表，下游既可以做流式增量消费，也可以做批式全量查询。

### 流式读取

```sql
-- Flink 中持续消费增量
SELECT * FROM orders_paimon /*+ OPTIONS('scan.mode'='latest') */;
```

### 批量读取

```sql
-- Spark / Trino 中查询快照
SELECT * FROM orders_paimon /* paimon */
WHERE dt >= '2026-05-01';
```

### Changelog Producer 选型

| producer | 用途 | 代价 |
|----------|------|------|
| `none` | 仅批查询 | 无额外开销 |
| `input` | 输出输入变更 | 增量可见，不合并 |
| `lookup` | 输出合并后的完整行 | 写放大较高 |
| `full-compaction` | 输出合并后的完整行 | 等待 Compaction |

---

## 6. 数据归档与审计日志（Append-Only）

**一句话**：不可变数据直接用 Append-Only 表存储，查询时用 Time Travel 回溯历史。

### 场景

- 操作审计日志
- 交易流水
- IoT 传感器时序数据

### 建表

```sql
CREATE TABLE audit_log (
    event_time TIMESTAMP(3),
    user_id BIGINT,
    action STRING,
    detail STRING
) WITH (
    'bucket' = '8',
    'file.format' = 'parquet'
);
-- 无主键 = Append-Only
```

### Time Travel 查询

```sql
-- 查 3 小时前的快照
SELECT * FROM audit_log /* paimon */
WHERE event_time > TIMESTAMP '2026-05-13 10:00:00';

-- Spark 中按快照回溯
SELECT * FROM audit_log
VERSION AS OF 1234567890123;
```

---

## 7. 多引擎互通

**一句话**：Paimon 表可被 Flink / Spark / Trino / Hive 等多引擎直接读写。

### 支持矩阵

| 引擎 | 写入 | 读取（批） | 读取（流） |
|------|------|----------|----------|
| Flink | ✅ | ✅ | ✅ |
| Spark 3 | ✅ | ✅ | ❌ |
| Trino | ❌ | ✅ | ❌ |
| Hive | ❌ | ✅ | ❌ |
| StarRocks/Doris | ❌ | ✅（外部表） | ❌ |

### 本项目中的应用

Flink 写入 Paimon → Doris 通过 Multi-Catalog 外部表读取做 OLAP 查询。

---

## 8. 数据版本管理（Tag / Branch）

**一句话**：用 Paimon 的 Tag 和 Branch 功能管理数据快照版本，适合 ML 特征回溯、报表重跑。

### 场景

- ML 训练：用 Tag 锁定某天凌晨的特征快照
- 报表重跑：基于某个历史 Tag 重新计算
- 数据发布：从开发 Branch 合并到生产 Branch

### 操作示例

```sql
-- Flink SQL 创建 Tag
CALL sys.create_tag('default', 'orders_paimon', 'report_20260513');

-- Spark 读取指定 Tag
-- 通过 catalog 读取 tag 快照
```

---

## 9. 场景速查表

| 场景 | 表类型 | merge-engine | changelog-producer | 本项目中是否涉及 |
|------|--------|-------------|-------------------|:---:|
| 实时数据入湖 | Append-Only / 主键表 | `deduplicate` | `input` | ✅ 核心链路 |
| CDC 同步 | 主键表 | `deduplicate` | `input` | 可选替换 |
| Partial-Update 宽表 | 主键表 | `partial-update` | `full-compaction` | ✅ 已有文档 |
| Lookup Join 维度表 | 主键表 | `deduplicate` | `none` | 待评估 |
| 流批一体查询 | 主键表 | - | `input`/`lookup` | ✅ 均为读写 |
| 审计/日志/时序 | Append-Only | - | - | 可选 |
| 多引擎互通 | 任意 | - | - | ✅ Doris 外部表 |
| 版本/Tag/Branch | 任意 | - | - | 可选 |

---

## 10. 推荐建表决策流程

```
是否需要主键去重？
 ├── 否 → Append-Only 表
 └── 是 → 主键表
          ├── 上游只有一路 → merge-engine = deduplicate
          └── 上游多路、各自更新不同列 → merge-engine = partial-update
                   ├── Chronological → 无需 sequence-group
                   └── 乱序/多流 → 添加 sequence-group
```

---

## 11. 参考资料

- Apache Paimon 0.8 官方文档：<https://paimon.apache.org/docs/0.8/>
- Merge Engine：<https://paimon.apache.org/docs/0.8/primary-key-table/merge-engine/>
- CDC Ingestion：<https://paimon.apache.org/docs/0.8/cdc-ingestion/overview/>
- Flink Lookup Join：<https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/dev/table/sql/queries/joins/#lookup-join>
