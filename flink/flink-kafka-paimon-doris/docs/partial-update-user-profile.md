# Paimon Partial-Update 场景文档：用户画像宽表

本文用一个“用户画像实时宽表”的例子，对比传统双流 Join 和 Paimon `partial-update` 两种实现方式，帮助理解为什么很多原本压在 Flink 状态上的关联逻辑，可以转移到 Paimon 存储层完成。

一句话概括：

把原先在计算层做的“昂贵 Join”，变成在存储层做的“按主键增量合并”。

## 1. 场景设定

我们有两张实时流表，需要按 `user_id` 合并成一张用户画像宽表：

- 流表 A：用户基础信息，字段为 `user_id`、`name`、`age`
- 流表 B：用户行为特征，字段为 `user_id`、`last_login`、`points`

目标宽表如下：

| user_id | name | age | last_login | points |
|---|---|---:|---|---:|
| 1001 | 张三 | 25 | 2026-05-13 10:20:00 | 1200 |

这类场景的特点是：

- 主键明确，通常是 `user_id`
- 不同数据源只更新宽表中的一部分列
- 宽表会被频繁更新，但并不希望在 Flink 中维护巨大的 Join 状态

## 2. 传统方案：双流 Join，状态压力在 Flink

传统做法一般是让一个 Flink 作业同时消费两条 Kafka 流，在作业内完成双流 Join。

### 2.1 执行过程

1. Flink 同时消费表 A 和表 B。
2. 状态后端分别保存两侧历史数据，通常落在 RocksDB。
3. 任意一侧来了新事件，都去另一侧状态里查找匹配主键。
4. 拼出完整宽表后输出。

### 2.2 问题在哪里

这套方式的核心代价，不在 SQL 本身，而在“为了做 Join，不得不长期维护大状态”：

- 每条数据都伴随着状态读写
- 状态规模可能快速增长到 GB 甚至 TB 级
- Checkpoint 变慢，容易超时
- 故障恢复时间长
- CPU、内存、磁盘 IO 压力都比较大

在一些极端场景里，团队会把部分状态外置到 HBase、Redis 等 KV 系统中，来减轻 Flink 状态后端压力。但这也意味着：

- 架构更复杂
- 数据副本更多
- 运维成本更高
- 一致性链路更长

## 3. 新方案：Partial-Update，把合并交给 Paimon

Paimon 的 `partial-update` merge engine 很适合这类“同主键、多来源、部分列更新”的宽表场景。

它的核心思想不是先在 Flink 里把一整行 Join 完再写入，而是：

- 每个上游只负责自己那几列
- 直接把局部更新写到同一张 Paimon 主键表
- Paimon 按主键把多次更新合并成最终宽表结果

### 3.1 先定义目标宽表

```sql
CREATE TABLE user_profile_paimon (
    user_id BIGINT,
    name STRING,
    age INT,
    last_login TIMESTAMP(3),
    points BIGINT,
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'partial-update'
);
```

### 3.2 再让不同来源分别写入同一张表

- 任务 1 只写 `name`、`age`
- 任务 2 只写 `last_login`、`points`

写入时，不负责更新的列必须显式写成 `NULL`，这样 Paimon 才不会覆盖已有值。

例如：

```text
记录 1：{user_id=1, name='张三', age=25, last_login=NULL, points=NULL}
记录 2：{user_id=1, name=NULL, age=NULL, last_login='2026-05-13 10:00:00', points=100}
```

最终查询结果会按主键合并为：

```text
{user_id=1, name='张三', age=25, last_login='2026-05-13 10:00:00', points=100}
```

## 4. 这和“双流 Join”到底有什么本质区别

最关键的变化是职责转移：

| 维度 | 传统双流 Join | Paimon Partial-Update |
|---|---|---|
| 关联发生在哪里 | Flink 计算层 | Paimon 存储层 |
| Flink 是否维护大状态 | 是 | 基本不需要 |
| 处理方式 | 先关联，再输出整行 | 先写局部列，再按主键合并 |
| 系统复杂度 | 高 | 低 |
| 适配场景 | 低延迟、数据量可控 | 大数据量、部分列频繁更新 |

更直观一点说：

- 双流 Join 的思路是“Flink 先把完整结果算出来”
- `partial-update` 的思路是“Flink 先分别把碎片写进去，Paimon 再把碎片拼起来”

## 5. 为什么这种方式更轻

### 5.1 Flink 状态几乎清零

因为不需要实时维护两条流的历史 Join 状态，Flink 作业通常只做：

- 读取 Kafka
- 字段裁剪 / 格式转换
- 直接写入 Paimon

这会显著降低：

- Checkpoint 压力
- 作业恢复时间
- 资源占用

### 5.2 架构更简单

如果宽表就是最终沉淀层，那么数据可以统一落在 Paimon 中，不必额外引入 HBase、Redis 一类外部 KV 系统来“替 Flink 扛状态”。

### 5.3 更适合“列级更新”

用户画像、商品画像、订单宽表、设备宽表，很多时候并不是整行一起更新，而是不同来源在不同时间补齐不同字段。`partial-update` 对这种模式非常自然。

## 6. 推荐实现方式

### 6.1 方案一：两个独立 Flink 任务

这是更推荐的方案，因为隔离性更好：

- 基础信息流任务故障，不影响行为流任务
- 两边可以独立扩缩容
- 各自的消费位点、重试策略、发布节奏都更清晰

#### 任务 1：写入基础信息

```sql
INSERT INTO paimon_catalog.default.user_profile_paimon
SELECT
    user_id,
    name,
    age,
    CAST(NULL AS TIMESTAMP(3)) AS last_login,
    CAST(NULL AS BIGINT) AS points
FROM kafka_catalog.default.user_base_info;
```

#### 任务 2：写入行为特征

```sql
INSERT INTO paimon_catalog.default.user_profile_paimon
SELECT
    user_id,
    CAST(NULL AS STRING) AS name,
    CAST(NULL AS INT) AS age,
    last_login,
    points
FROM kafka_catalog.default.user_behavior;
```

### 6.2 方案二：同一个任务里做分流后多 Sink

如果场景比较简单，也可以一个任务里读取统一流后拆成两路，再分别写入同一张 Paimon 表。优点是部署少，缺点是隔离性不如独立任务。

## 7. Java 代码示例

下面给出两个独立 Flink 任务的示例。为了突出 `partial-update` 的核心写法，示例省略了完整的反序列化实现和异常处理。

### 7.1 写入基础信息流

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WriteBaseInfoToPaimon {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 官方文档建议关闭 sink upsert materialize，避免主键表出现异常行为
        tableEnv.getConfig().getConfiguration()
            .setString("table.exec.sink.upsert-materialize", "NONE");

        tableEnv.executeSql(
            "CREATE CATALOG paimon_catalog WITH (" +
            "  'type' = 'paimon'," +
            "  'warehouse' = 'hdfs://namenode:8020/paimon/warehouse'" +
            ")"
        );
        tableEnv.useCatalog("paimon_catalog");

        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS user_profile_paimon (" +
            "  user_id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  last_login TIMESTAMP(3)," +
            "  points BIGINT," +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH (" +
            "  'merge-engine' = 'partial-update'" +
            ")"
        );

        KafkaSource<Row> source = KafkaSource.<Row>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("user_base_info")
            .setGroupId("paimon-base-info-writer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleRowDeserializer())
            .build();

        DataStream<Row> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "base-info-source"
        );

        tableEnv.createTemporaryView("base_info_stream", stream);

        tableEnv.executeSql(
            "INSERT INTO user_profile_paimon " +
            "SELECT " +
            "  user_id, " +
            "  name, " +
            "  age, " +
            "  CAST(NULL AS TIMESTAMP(3)) AS last_login, " +
            "  CAST(NULL AS BIGINT) AS points " +
            "FROM base_info_stream"
        ).await();
    }
}
```

### 7.2 写入行为特征流

```java
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WriteBehaviorToPaimon {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.getConfig().getConfiguration()
            .setString("table.exec.sink.upsert-materialize", "NONE");

        tableEnv.executeSql(
            "CREATE CATALOG paimon_catalog WITH (" +
            "  'type' = 'paimon'," +
            "  'warehouse' = 'hdfs://namenode:8020/paimon/warehouse'" +
            ")"
        );
        tableEnv.useCatalog("paimon_catalog");

        tableEnv.executeSql(
            "CREATE TABLE IF NOT EXISTS user_profile_paimon (" +
            "  user_id BIGINT," +
            "  name STRING," +
            "  age INT," +
            "  last_login TIMESTAMP(3)," +
            "  points BIGINT," +
            "  PRIMARY KEY (user_id) NOT ENFORCED" +
            ") WITH (" +
            "  'merge-engine' = 'partial-update'" +
            ")"
        );

        KafkaSource<Row> source = KafkaSource.<Row>builder()
            .setBootstrapServers("kafka:9092")
            .setTopics("user_behavior")
            .setGroupId("paimon-behavior-writer")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleRowDeserializer())
            .build();

        DataStream<Row> stream = env.fromSource(
            source,
            WatermarkStrategy.noWatermarks(),
            "behavior-source"
        );

        tableEnv.createTemporaryView("behavior_stream", stream);

        tableEnv.executeSql(
            "INSERT INTO user_profile_paimon " +
            "SELECT " +
            "  user_id, " +
            "  CAST(NULL AS STRING) AS name, " +
            "  CAST(NULL AS INT) AS age, " +
            "  last_login, " +
            "  points " +
            "FROM behavior_stream"
        ).await();
    }
}
```

## 8. 几个关键语义，一定要写清楚

### 8.1 `NULL` 的含义是不更新，不是把值改成空

`partial-update` 的核心语义是“非空字段参与更新，空字段不覆盖旧值”。

所以：

- `CAST(NULL AS STRING)` 表示“不更新这一列”
- `''`、`0`、`1970-01-01` 这类值都不是“跳过更新”，而是“真的要把旧值覆盖掉”

### 8.2 合并语义由 merge engine 保证，不要简单理解成“只有 Compaction 后才可见”

为了便于理解，很多介绍会说“Paimon 在后台 Compaction 时把多条记录合并成一条”。这个说法方向没错，但工程上要更严谨一些：

- `partial-update` 的合并规则由 `merge-engine` 定义
- Paimon 底层使用 LSM Tree 组织主键表
- 查询同一主键时，会按照 merge engine 语义合并多份更新
- Compaction 的作用是整理 sorted runs、降低查询放大，并为某些 changelog 生产方式提供支持

所以更准确的理解是：

Paimon 把“同主键多次更新如何合并”这件事内建到了存储引擎里，而不是要求 Flink 先把整行 Join 出来。

### 8.3 `changelog-producer = 'input'` 只会返回输入变更

如果下游还要持续消费这张 `partial-update` 表的流式变更，需要特别注意：

- `input` 只会把输入记录原样作为 changelog 输出
- 它不会自动帮你生成“合并后的完整宽表变更”

按照 Paimon 官方文档，`partial-update` 做流式查询时，更适合搭配：

- `lookup`
- `full-compaction`

如果只是写入宽表并供下游批查或点查，`input` 是否开启取决于你的消费方式；不要把它理解成“开启后下游就能直接拿到完整宽表更新”。

### 8.4 多流乱序时，建议考虑 `sequence-group`

如果两个来源都会频繁更新，而且存在乱序、重放、补数等情况，单纯依赖“非空覆盖”可能不够。

Paimon 官方文档专门给 `partial-update` 提供了 `sequence-group`，用于：

- 为不同列组定义各自的顺序字段
- 解决多流更新下的乱序问题
- 实现更严格的“按列组更新”

例如可以把基础信息和行为信息拆成两个序列组：

```sql
CREATE TABLE user_profile_paimon (
    user_id BIGINT,
    name STRING,
    age INT,
    base_ts TIMESTAMP(3),
    last_login TIMESTAMP(3),
    points BIGINT,
    behavior_ts TIMESTAMP(3),
    PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
    'merge-engine' = 'partial-update',
    'fields.base_ts.sequence-group' = 'name,age',
    'fields.behavior_ts.sequence-group' = 'last_login,points'
);
```

这样两类字段各自按自己的时间线更新，能减少跨流覆盖带来的歧义。

## 9. 什么时候适合用 Partial-Update

更适合：

- 用户画像、商品画像、设备画像这类宽表
- 多个来源按同一主键补充不同字段
- 允许分钟级可见性
- 希望减少 Flink 状态和运维负担

不一定适合：

- 必须严格追求毫秒级结果且强依赖流上 Join 输出
- 需要复杂的非主键关联条件
- 下游强依赖完整、逐条、实时的宽表 changelog，而你又没有设计好对应的 changelog producer

## 10. 推荐写法小结

在生产里，可以把这类方案总结成四条规则：

1. 宽表必须建成 Paimon 主键表，并开启 `merge-engine = partial-update`
2. 每个上游只写自己负责的列，其他列一律显式写 `NULL`
3. Flink SQL 中建议关闭 `table.exec.sink.upsert-materialize`
4. 多流乱序明显时，优先设计 `sequence-group`

## 11. 一段可直接放到分享里的总结

传统双流 Join 的问题，不是“Join 语法慢”，而是“为了 Join，不得不在 Flink 中维护大状态”。

`partial-update` 的价值，在于把“宽表拼接”这件事从计算层前移到了存储引擎内部：Flink 不再负责保存两边历史并实时撮合整行，而是只负责把自己那部分列写进去；Paimon 再基于主键和 merge engine 把多次更新合并成最终结果。

这本质上是用更便宜、更稳定的存储侧合并能力，换掉高成本的计算侧大状态。

## 12. 参考资料

- Apache Paimon 0.8.2 Merge Engine: <https://paimon.apache.org/docs/0.8/primary-key-table/merge-engine/>
- Apache Paimon Partial Update: <https://paimon.apache.org/docs/1.2/primary-key-table/merge-engine/partial-update/>
