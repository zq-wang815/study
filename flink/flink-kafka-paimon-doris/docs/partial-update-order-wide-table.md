# Paimon Partial-Update 简明示例：订单宽表

这篇文档再用一个更短的例子说明 `partial-update` 适合什么场景。

一句话理解：

不是先在 Flink 里把多条流 Join 成完整订单，再写入结果；而是让每条流只写自己负责的字段，最后由 Paimon 按主键合并。

## 1. 场景

我们要构建一张订单宽表，主键是 `order_id`。

上游有两条流：

- 订单创建流：`order_id`、`user_id`、`create_time`、`amount`
- 订单支付流：`order_id`、`pay_time`、`pay_status`

目标宽表：

```sql
CREATE TABLE dwd_order_wide (
    order_id BIGINT,
    user_id BIGINT,
    create_time TIMESTAMP(3),
    amount DECIMAL(10, 2),
    pay_time TIMESTAMP(3),
    pay_status STRING,
    PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'partial-update'
);
```

## 2. 传统做法

传统方式一般是在 Flink 里做双流 Join：

- 一边消费订单创建流
- 一边消费订单支付流
- 按 `order_id` 维护两侧状态
- 关联后输出完整订单

问题是：

- Flink 要长期维护 Join 状态
- 数据量一大，Checkpoint 和恢复都会变重
- 作业更容易因为状态膨胀变得不稳定

## 3. Partial-Update 做法

思路很简单：

- 创建流只写创建相关字段
- 支付流只写支付相关字段
- 两条流都写到同一张 Paimon 表

### 3.1 订单创建流写法

```sql
INSERT INTO paimon_catalog.default.dwd_order_wide
SELECT
    order_id,
    user_id,
    create_time,
    amount,
    CAST(NULL AS TIMESTAMP(3)) AS pay_time,
    CAST(NULL AS STRING) AS pay_status
FROM kafka_catalog.default.order_created;
```

### 3.2 订单支付流写法

```sql
INSERT INTO paimon_catalog.default.dwd_order_wide
SELECT
    order_id,
    CAST(NULL AS BIGINT) AS user_id,
    CAST(NULL AS TIMESTAMP(3)) AS create_time,
    CAST(NULL AS DECIMAL(10, 2)) AS amount,
    pay_time,
    pay_status
FROM kafka_catalog.default.order_paid;
```

## 4. 合并效果

假设先后写入两条记录：

```text
记录 1：
{order_id=1001, user_id=88, create_time='2026-05-13 10:00:00', amount=99.00, pay_time=NULL, pay_status=NULL}

记录 2：
{order_id=1001, user_id=NULL, create_time=NULL, amount=NULL, pay_time='2026-05-13 10:05:00', pay_status='PAID'}
```

查询宽表时，Paimon 会按主键合并为：

```text
{order_id=1001, user_id=88, create_time='2026-05-13 10:00:00', amount=99.00, pay_time='2026-05-13 10:05:00', pay_status='PAID'}
```

## 5. 关键点

### 5.1 为什么要写 `NULL`

因为在 `partial-update` 里：

- 非空字段会更新
- `NULL` 字段不会覆盖旧值

所以不负责的列必须写 `NULL`，不要写默认值，否则会把已有数据冲掉。

### 5.2 为什么它更轻

因为 Flink 不再负责保存两边历史并实时 Join，只需要：

- 读 Kafka
- 做简单字段映射
- 写入 Paimon

状态压力会小很多。

## 6. 适合什么场景

这个模式特别适合：

- 订单宽表
- 用户画像宽表
- 商品画像宽表
- 多数据源按主键补充不同字段的场景

如果你的核心痛点是 Flink Join 状态太大，那 `partial-update` 往往是一个很实用的替代方案。
