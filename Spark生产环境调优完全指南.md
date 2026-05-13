# Spark 生产环境调优完全指南

> 本文档覆盖 Spark 生产环境中常见的调优场景，每个场景包含：场景描述、排查思路、解决办法。涵盖内存调优、Shuffle 调优、数据倾斜、Join 优化、SQL 优化、资源分配、GC 调优、流处理、IO 优化、序列化、动态资源分配、广播优化、分区管理、小文件治理、推测执行等多个维度。

---

## 目录

1. [场景1：Executor OOM — 堆内存溢出](#场景1executor-oom--堆内存溢出)
2. [场景2：GC Overhead Limit Exceeded — 频繁GC导致任务失败](#场景2gc-overhead-limit-exceeded--频繁gc导致任务失败)
3. [场景3：Shuffle Fetch 失败 — FetchFailedException](#场景3shuffle-fetch-失败--fetchfailedexception)
4. [场景4：数据倾斜 — 部分 Task 运行时间远超其他](#场景4数据倾斜--部分-task-运行时间远超其他)
5. [场景5：Shuffle Write/Read 数据量过大导致磁盘溢出](#场景5shuffle-writeread-数据量过大导致磁盘溢出)
6. [场景6：Broadcast Join OOM — 广播表过大导致 Driver/Executor 内存溢出](#场景6broadcast-join-oom--广播表过大导致-driverexecutor-内存溢出)
7. [场景7：小文件问题 — 读取性能下降、产生场景与根因分析](#场景7小文件问题--读取性能下降产生场景与根因分析)
8. [场景8：读取数据源时分区数不合理导致并行度不足或过多](#场景8读取数据源时分区数不合理导致并行度不足或过多)
9. [场景9：Hive SQL on Spark — 执行计划不合理](#场景9hive-sql-on-spark--执行计划不合理)
10. [场景10：流处理（Structured Streaming）背压与延迟](#场景10流处理structured-streaming背压与延迟)
11. [场景11：动态资源分配导致的性能抖动](#场景11动态资源分配导致的性能抖动)
12. [场景12：大量 UDF 调用导致性能瓶颈](#场景12大量-udf-调用导致性能瓶颈)
13. [场景13：Kryo 序列化缓冲区不足](#场景13kryo-序列化缓冲区不足)
14. [场景14：Join 策略选择不当 — SortMergeJoin 性能差](#场景14join-策略选择不当--sortmergejoin-性能差)
15. [场景15：磁盘 IO 成为瓶颈 — Shuffle Spill 频繁](#场景15磁盘-io-成为瓶颈--shuffle-spill-频繁)
16. [场景16：数据本地性差导致网络传输开销大](#场景16数据本地性差导致网络传输开销大)
17. [场景17：推测执行导致资源浪费或任务重复](#场景17推测执行导致资源浪费或任务重复)
18. [场景18：YARN 资源不足 — 任务长时间排队](#场景18yarn-资源不足--任务长时间排队)
19. [场景19：Spark SQL 谓词下推失效](#场景19spark-sql-谓词下推失效)
20. [场景20：Cartesian Product / Cross Join 导致数据膨胀](#场景20cartesian-product--cross-join-导致数据膨胀)
21. [场景21：窗口函数在大数据集上性能差](#场景21窗口函数在大数据集上性能差)
22. [场景22：Delta Lake / Iceberg / Hudi 写入性能问题](#场景22delta-lake--iceberg--hudi-写入性能问题)
23. [场景23：Driver 端 OOM — 结果集过大](#场景23driver-端-oom--结果集过大)
24. [场景24：Kubernetes 上 Spark 的 Pod 频繁驱逐](#场景24kubernetes-上-spark-的-pod-频繁驱逐)
25. [场景25：AQE（Adaptive Query Execution）相关调优](#场景25aqeadaptive-query-execution相关调优)
26. [场景26：多表 Join 顺序导致的性能问题](#场景26多表-join-顺序导致的性能问题)
27. [场景27：RDD/DataFrame 缓存策略不当](#场景27rdddataframe-缓存策略不当)
28. [场景28：Task 调度延迟 — 大量 Task 等待执行](#场景28task-调度延迟--大量-task-等待执行)

---

## 场景1：Executor OOM — 堆内存溢出

### 场景描述

某电商平台的每日报表任务，需要从 Hive 中读取过去 30 天的交易明细（约 5 亿条记录），经过多表 Join 和聚合后写入到 MySQL 报表库。任务运行约 30 分钟后，部分 Executor 报出 `OutOfMemoryError: Java heap space`，导致 Stage 重试多次后任务失败。核心日志如下：

```
org.apache.spark.memory.SparkOutOfMemoryError: Unable to acquire 256 bytes of memory, got 0
java.lang.OutOfMemoryError: Java heap space
```

### 排查思路

1. **确认 OOM 发生的 Task 类型**：通过 Spark UI 定位失败的 Stage，查看是 Map 端还是 Reduce 端 OOM。
2. **查看 Executor 日志**：确认 OOM 时 Task 正在执行什么操作（Shuffle Read、聚合、Join 等）。
3. **分析数据分布**：检查是否存在数据倾斜导致单个 Partition 数据过大。
4. **检查 Executor 内存配置**：`spark.executor.memory`、`spark.memory.fraction`、`spark.sql.shuffle.partitions` 等参数。
5. **检查 Shuffle 数据量**：在 Spark UI 的 Stage 详情中查看 Shuffle Read Size 是否过大。
6. **分析 GC 日志**：如果开启了 GC 日志，检查是 Young GC 过于频繁还是 Old Gen 已满。

### 解决办法

**参数调整：**

```properties
# 增加 Executor 堆内存
spark.executor.memory=16g

# 调整堆内内存比例：Storage 和 Execution 各占 50%（Storage 占 unified 内存的 50%）
spark.memory.fraction=0.6
spark.memory.storageFraction=0.5

# 增加 Shuffle 分区数，减少单个 Task 处理的数据量
spark.sql.shuffle.partitions=800

# 启用堆外内存（用于 Shuffle 和缓存）
spark.memory.offHeap.enabled=true
spark.memory.offHeap.size=4g
```

**代码优化：**

- 如果 OOM 发生在聚合操作，改用两阶段聚合（加盐去盐）缓解数据倾斜。
- 避免在 `map` 或 `flatMap` 中创建大量临时对象，尽量复用对象。
- 将大表 Join 改写为：小表 Broadcast Join，大表使用分桶或排序优化。

**服务器配置：**

- 如果单节点物理内存有限，增加 Executor 数量而非单个 Executor 内存。
- 考虑将数据存储在支持列式裁剪和谓词下推的格式中（Parquet、ORC），减少读取时的内存开销。

---

## 场景2：GC Overhead Limit Exceeded — 频繁GC导致任务失败

### 场景描述

某广告平台的实时特征计算任务，使用 Spark 从 Kafka 消费数据后进行多种 UDF 计算，生成用户画像特征。任务运行过程中，每隔一段时间就会出现 Executor 被 YARN 杀死的情况，错误信息为：

```
java.lang.OutOfMemoryError: GC overhead limit exceeded
ExecutorLostFailure (executor X exited caused by one of the running tasks)
  Reason: Container killed by YARN for exceeding memory limits. XX GB of XX GB physical memory used.
```

### 排查思路

1. **检查 GC 日志**：通过 `-XX:+PrintGCDetails` 参数分析 GC 频率和停顿时间。
2. **Spark UI 分析**：查看 Storage 页签，确认是否有大量数据被缓存到内存。
3. **分析内存使用模式**：确认是 UDF 内部创建了过多临时对象，还是缓存的数据太多。
4. **检查 `spark.memory.fraction`**：如果该值过大，留给 JVM 自身的堆外空间就不足，会导致频繁 GC。
5. **检查 Python UDF**：Python UDF 会在单独的 Python 进程中执行，数据序列化/反序列化开销大，也会导致内存问题。

### 解决办法

**GC 参数调整：**

```properties
# 使用 G1GC 替代默认的 Parallel GC
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:ConcGCThreads=4 -XX:MaxGCPauseMillis=200

# 对于堆内存较大的 Executor（>16GB），考虑使用 G1GC
# 如果堆内存较小（<4GB），使用 CMS 可能更合适
spark.executor.extraJavaOptions=-XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=70 -XX:+UseCMSInitiatingOccupancyOnly

# 调整 G1GC 的 Region 大小（如果堆 > 32GB）
-XX:G1HeapRegionSize=32m

# 限制 GC 停顿时间目标
-XX:MaxGCPauseMillis=500
```

**内存参数调整：**

```properties
# 降低 unified 内存占比，给 JVM 元空间和堆外留更多空间
spark.memory.fraction=0.6

# 减少分配给 Storage 的内存比例，Execution 内存优先
spark.memory.storageFraction=0.3
```

**代码优化：**

- 避免在 UDF 中创建大量短生命周期对象（如在循环内使用 `new` 创建对象）。
- 对于 Java/Scala UDF，优先使用基本类型数组而非对象列表。
- 将 Python UDF 替换为 Scala/Java UDF，或使用 Pandas UDF 减少序列化开销。
- 减少 `cache()` 的使用量，只缓存真正需要多次使用的中间结果，使用 `MEMORY_AND_DISK_SER` 策略。

---

## 场景3：Shuffle Fetch 失败 — FetchFailedException

### 场景描述

某金融风控系统的每日离线计算任务，需要在 1000+ Executor 上运行，每天处理约 20TB 数据。任务经常在 Shuffle Read 阶段失败，某个 Stage 运行几小时后出现：

```
org.apache.spark.shuffle.FetchFailedException: Failed to connect to /192.168.1.100:45889
  at org.apache.spark.storage.ShuffleBlockFetcherIterator.throwFetchFailedException
Caused by: java.io.IOException: Connection reset by peer
```

或者是：

```
org.apache.spark.shuffle.MetadataFetchFailedException: Missing an output location for shuffle X
```

### 排查思路

1. **Spark UI 定位问题 Stage**：查看失败的 Stage 的 Shuffle Read 量和 Task 分布。
2. **检查 Executor 日志**：查看提供 Shuffle 数据的 Executor 是否已经挂掉（GC 超时、OOM、或被 YARN 杀掉）。
3. **网络排查**：在对应节点上 `ping` 或 `telnet` 检查网络连通性。
4. **Executor 心跳**：看 Executor 是否因为 GC 过长导致心跳超时，被 Driver 认为丢失。
5. **检查 Shuffle 服务**：确认 External Shuffle Service 是否在运行且健康。
6. **磁盘空间**：检查 Executor 节点磁盘是否已满导致 Shuffle 文件无法写入。

### 解决办法

**Shuffle 服务配置：**

```properties
# 启用 External Shuffle Service（YARN 上强烈推荐）
spark.shuffle.service.enabled=true
spark.dynamicAllocation.enabled=true

# 增加 Shuffle 服务的端口数和重试次数
spark.shuffle.io.maxRetries=10
spark.shuffle.io.retryWait=60s

# 减少单次 fetch 的块大小，避免连接超时
spark.reducer.maxSizeInFlight=48m
spark.reducer.maxReqsInFlight=32

# 增加 Shuffle 超时时间（网络不稳定时）
spark.network.timeout=600s
spark.shuffle.io.connectionTimeout=120s

# 使用基于排序的 Shuffle（默认），减少文件数量
spark.shuffle.sort.bypassMergeThreshold=200
```

**服务器配置：**

- 确保 External Shuffle Service 在所有 NodeManager 节点正确运行。
- 增加 NodeManager 的本地磁盘空间，Shuffle 数据需要足够的临时空间。
- 如果使用 SSD，将 `spark.local.dir` 指向 SSD 路径以加速 Shuffle 读写。

```properties
spark.local.dir=/ssd1/spark,/ssd2/spark
```

**代码优化：**

- 减少 Shuffle 数据量：提前过滤不需要的字段和数据行。
- 在 Join 前先进行聚合，减少参与 Shuffle 的数据量。
- 使用 `reduceByKey` 替代 `groupByKey`，在 Map 端进行预聚合。

---

## 场景4：数据倾斜 — 部分 Task 运行时间远超其他

### 场景描述

某社交平台的 DAU 计算任务，需要对用户行为日志按 `user_id` 进行 `groupBy` 聚合。在 Spark UI 上观察到：某个 Stage 中有 799 个 Task 在 5 分钟内完成，但有几个 Task 运行了 2 小时以上。这些长尾 Task 处理的数据量是其他 Task 的 100 倍以上。日志中出现：

```
Executor task launch worker for task X.X in stage X.X (TID X)
...
[Stage X:==============>                               (799 + 1) / 800]
```

最后一个 Task 迟迟不完成。

### 排查思路

1. **Spark UI Stage 页签**：查看每个 Task 的 Shuffle Read Size 和 Duration，识别倾斜的 Partition。
2. **SQL 执行计划**：通过 `explain()` 或 Spark UI 的 SQL 页签查看执行计划，确认 Join 或聚合的 Key。
3. **数据分析**：对导致倾斜的 Key 进行采样分析，确认是否存在热点 Key（如 null 值、默认值、或大 V 用户）。
4. **查看 Shuffle Write 数据分布**：在 Map 端检查输出数据在各 Key 上的分布。

### 解决办法

**方案A：两阶段聚合（加盐去盐）— 适用于 groupBy 聚合倾斜**

```sql
-- 第一步：给 Key 加随机盐（0~99），创建带盐的临时视图
CREATE OR REPLACE TEMP VIEW salted_data AS
SELECT
  user_id,
  action_count,
  CONCAT(user_id, '_', CAST(FLOOR(RAND() * 100) AS INT)) AS salted_key
FROM user_actions;

-- 第二步：带盐聚合（每个盐值内部聚合，分散热点 Key）
CREATE OR REPLACE TEMP VIEW partial_agg AS
SELECT
  salted_key,
  SUM(action_count) AS partial_sum
FROM salted_data
GROUP BY salted_key;

-- 第三步：去盐，提取原始 user_id 做最终聚合
SELECT
  SPLIT(salted_key, '_')[0] AS user_id,
  SUM(partial_sum) AS total_sum
FROM partial_agg
GROUP BY SPLIT(salted_key, '_')[0];
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
import org.apache.spark.sql.functions._
import spark.implicits._

val saltNum = 100 // 盐值数量
val df = spark.read.parquet("/data/user_actions")

// 第一步：给 Key 加随机盐
val saltedDF = df
  .withColumn("salt", (rand() * saltNum).cast("int"))
  .withColumn("salted_key", concat(col("user_id"), lit("_"), col("salt")))

// 第二步：带盐聚合
val agg1 = saltedDF
  .groupBy("salted_key")
  .agg(sum("action_count").as("partial_sum"))

// 第三步：去盐，最终聚合
val result = agg1
  .withColumn("user_id", split(col("salted_key"), "_")(0))
  .groupBy("user_id")
  .agg(sum("partial_sum").as("total_sum"))
```

</details>

**方案B：倾斜 Key 单独处理 + 非倾斜 Key 正常处理 — 适用于 Join 倾斜**

```sql
-- 第一步：识别倾斜 Key（先采样确认热点 Key）
SELECT key, COUNT(*) AS cnt
FROM big_table
GROUP BY key
ORDER BY cnt DESC
LIMIT 20;

-- 假设确认热点 key = 'hot_value_1', 'hot_value_2'

-- 第二步：倾斜数据单独 Join（使用 Broadcast Hint 处理热点 Key）
CREATE OR REPLACE TEMP VIEW skewed_result AS
SELECT /*+ BROADCAST(s) */ b.*, s.*
FROM big_table b
JOIN small_table s ON b.key = s.key
WHERE b.key IN ('hot_value_1', 'hot_value_2');

-- 第三步：非倾斜数据正常 Join
CREATE OR REPLACE TEMP VIEW normal_result AS
SELECT b.*, s.*
FROM big_table b
JOIN small_table s ON b.key = s.key
WHERE b.key NOT IN ('hot_value_1', 'hot_value_2');

-- 第四步：合并结果
SELECT * FROM skewed_result
UNION ALL
SELECT * FROM normal_result;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
import org.apache.spark.sql.functions._

// 识别倾斜 Key（假设阈值是某个数量以上）
val skewedKeys = df.groupBy("key").count()
  .filter(col("count") > 10000000)
  .select("key")
  .collect()
  .map(_.getString(0))
  .toSet

// 倾斜数据单独 Join（使用 Broadcast）
val skewedDF = df.filter(col("key").isin(skewedKeys.toSeq: _*))
val normalDF = df.filter(!col("key").isin(skewedKeys.toSeq: _*))

// 小表广播
val skewedResult = skewedDF.join(broadcast(smallTable), "key")
val normalResult = normalDF.join(smallTable, "key")

val finalResult = skewedResult.union(normalResult)
```

</details>

**方案C：使用 AQE 自动处理倾斜 Join**

```properties
# Spark 3.0+ 开启 AQE
spark.sql.adaptive.enabled=true
# 开启倾斜 Join 自动优化
spark.sql.adaptive.skewJoin.enabled=true
# 设置倾斜因子阈值
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
# 设置倾斜 Partition 最小数据量
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB
```

**方案D：Broadcast Join 替代 Shuffle Join**

如果小表数据量在 Broadcast 阈值内（默认 10MB），使用 Broadcast Hint：

```sql
SELECT /*+ BROADCAST(small_table) */ *
FROM big_table JOIN small_table ON big_table.key = small_table.key
```

或增大 Broadcast 阈值：

```properties
spark.sql.autoBroadcastJoinThreshold=104857600  # 100MB
```

**方案E：过滤 null 值或无效值**

```sql
-- 如果 null 值不需要参与计算，提前过滤掉
SELECT key, SUM(value) AS total
FROM source_table
WHERE key IS NOT NULL
GROUP BY key;

-- 如果 null 值需要单独统计（NULL 聚合 + 非 NULL 聚合）
-- 非 NULL 的部分
SELECT key, SUM(value) AS total
FROM source_table
WHERE key IS NOT NULL
GROUP BY key
UNION ALL
-- NULL 的部分，单独统计
SELECT 'NULL' AS key, SUM(value) AS total
FROM source_table
WHERE key IS NULL;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 如果 null 值不需要参与计算
val result = df.filter(col("key").isNotNull).groupBy("key").agg(...)

// 如果 null 值需要单独统计
val nullResult = df.filter(col("key").isNull).agg(...)
val nonNullResult = df.filter(col("key").isNotNull).groupBy("key").agg(...)
```

</details>

---

## 场景5：Shuffle Write/Read 数据量过大导致磁盘溢出

### 场景描述

某物流公司的路径规划任务需要对 GPS 轨迹数据进行大规模 Join 操作。任务运行时，Executor 所在节点磁盘 IO 飙升至 100%，`iotop` 显示大量磁盘写入操作。Spark UI 显示 Shuffle Write 数据量超过 5TB，部分 Stage 的 Spill（溢写）量很大，Task 运行缓慢。监控系统发出磁盘使用率告警。

### 排查思路

1. **Spark UI 查看 Shuffle Write Size**：在 Stage 详情中查看 Shuffle Write 总量。
2. **检查 Spill 情况**：在 Task Metrics 中查看 Memory Spilled 和 Disk Spilled。
3. **SQL 执行计划分析**：确认哪些操作触发了 Shuffle（Join、GroupBy、Window、Sort）。
4. **检查数据压缩**：确认 Shuffle 数据是否开启了压缩。
5. **磁盘 IO 监控**：使用 `iostat` 查看磁盘读写速率和 IO 等待时间。

### 解决办法

**参数调整：**

```properties
# 开启 Shuffle 压缩（默认已开启，但需确认）
spark.shuffle.compress=true
spark.shuffle.spill.compress=true

# 使用更高压缩比的压缩算法（以 CPU 换 IO）
spark.io.compression.codec=zstd
# 或使用 lz4（速度快，压缩比适中）
spark.io.compression.codec=lz4

# 使用 ZSTD 时可调整压缩级别
spark.io.compression.zstd.level=3
```

**减少 Shuffle 数据量：**

```sql
-- 在 Shuffle 前裁剪列和提前过滤，减少参与 Shuffle 的数据量
SELECT key, SUM(value) AS total
FROM (
  SELECT key, value
  FROM source_table
  WHERE value > 0    -- 提前过滤无效数据
) t
GROUP BY key;
```

```scala
// 在 Shuffle 前裁剪列
val optimized = df
  .select("key", "value")  // 只保留需要的列
  .filter(col("value") > 0) // 提前过滤
  .groupBy("key")
  .agg(...)

// 使用 reduceByKey 进行 Map 端预聚合
rdd.reduceByKey(_ + _) // 而非 groupByKey 然后 mapValues
```

**服务器配置：**

- 为每个 Executor 挂载多块 SSD 磁盘用于 Shuffle 临时数据。
- 配置 `spark.local.dir` 使用逗号分隔的多路径。

```properties
spark.local.dir=/data1/spark,/data2/spark,/data3/spark,/data4/spark
```

**代码优化：**

- 对超大表采用分桶（Bucket）存储，在 Join 时可以避免 Shuffle。
- 将多次使用的中间结果物化到 HDFS，避免重复计算。
- 对于增量更新场景，只处理增量数据而非全量数据。

---

## 场景6：Broadcast Join OOM — 广播表过大导致 Driver/Executor 内存溢出

### 场景描述

某 BI 系统的报表查询，有一个维度表（约 500MB）需要与事实表进行 Join。开发者在代码中使用了 `broadcast()` Hint 强制广播。任务提交后，Driver 端报 OOM 错误：

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.execution.SparkPlan$$anon$1.executeBroadcast
```

或者在 Executor 端报：

```
org.apache.spark.SparkException: Cannot broadcast the table that is larger than 8GB: 8.2 GB
```

### 排查思路

1. **检查广播表实际大小**：查看 Spark UI 中 SQL 页签的 BroadcastExchange 节点的 `dataSize`。
2. **检查 Driver 内存**：`spark.driver.memory` 是否足够存放广播数据。
3. **检查 `autoBroadcastJoinThreshold` 配置**：自动广播的阈值是否设置过大。
4. **分析表数据**：确认被广播的表是否经过了过滤或裁剪可以减少体积。
5. **检查 Broadcast 超时**：`spark.sql.broadcastTimeout` 是否过短，或网络传输瓶颈导致广播慢。

### 解决办法

**参数调整：**

```properties
# 合理设置自动广播阈值（不建议超过 100MB 自动广播）
spark.sql.autoBroadcastJoinThreshold=104857600  # 100MB

# 增加 Broadcast 超时时间（网络慢或数据量大时）
spark.sql.broadcastTimeout=1200

# 增加 Driver 内存以容纳广播数据
spark.driver.memory=8g
spark.driver.maxResultSize=4g

# 压缩广播数据
spark.broadcast.compress=true
```

**代码优化：**

```sql
-- 对维度表进行裁剪和过滤，减小体积后再广播
CREATE OR REPLACE TEMP VIEW filtered_dim AS
SELECT id, name, category       -- 只取需要的列
FROM dim_table
WHERE status = 'active';        -- 过滤不需要的行

-- 使用 BROADCAST hint 将精简后的维度表广播
SELECT /*+ BROADCAST(d) */ f.*, d.name, d.category
FROM fact_table f
JOIN filtered_dim d ON f.dim_id = d.id;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 对维度表进行裁剪和过滤后再广播（减少广播数据量）
val filteredDim = dimTable
  .select("id", "name", "category") // 只取需要的列
  .filter(col("status") === "active") // 过滤不需要的行
  .cache() // 先缓存减小后的表

val result = factTable.join(broadcast(filteredDim), "dim_id")
```

</details>

**替代方案：**

- 如果维度表确实很大（>1GB），使用 **SortMergeJoin** 替代 Broadcast Join。
- 使用 **分桶（Bucketing）** 在写入时就做好数据组织，Join 时避免 Shuffle。

```sql
-- 创建分桶表，按 Join Key 分 256 个桶
CREATE TABLE dim_table_bucketed (id BIGINT, name STRING)
USING parquet
CLUSTERED BY (id) INTO 256 BUCKETS;

CREATE TABLE fact_table_bucketed (dim_id BIGINT, value DOUBLE)
USING parquet
CLUSTERED BY (dim_id) INTO 256 BUCKETS;
```

---

## 场景7：小文件问题 — 读取性能下降、产生场景与根因分析

### 场景描述

某日志分析平台每小时产生大量小日志文件（平均每个 5-10MB），存储在 HDFS 的某个分区目录下。Spark 任务读取这些文件时，生成了 50000+ 个 Task（推测一个 Task 读取一个文件）。由于 Task 调度开销极大，任务启动就需要 20+ 分钟，实际计算时间反而很短。大量的 Task 还导致 Driver 端的内存压力增大（需要跟踪每个 Task 的状态）。

### 排查思路

1. **Spark UI 查看 Task 数量**：在 Tasks 区域查看单个 Stage 的 Task 总数。
2. **Spark UI 查看 Task 耗时分布**：看是否大量 Task 耗时极短（毫秒级），说明每个文件太小。
3. **检查输入路径**：通过 `hdfs dfs -count` 查看目录下的文件数量和平均大小。
4. **查看 HDFS 文件统计**：`hdfs fsck /path -files -blocks` 查看文件大小分布。
5. **SQL 执行计划**：检查 `FileScan` 节点的 `number of files read` 指标。

### 解决办法

**方案A：使用分区合并参数（读取时合并小文件）**

```properties
# 使用 coalesce 减少分区（不经过 Shuffle）
# 前提：数据不需要打散分布

# 开启文件分区合并（减少读取时的 Task 数）
spark.sql.files.maxPartitionBytes=268435456  # 256MB，增大每个分区的数据量
spark.sql.files.openCostInBytes=16777216    # 16MB，小于此大小的文件将被合并读取

# 对于 Parquet：合并小文件一起读取
spark.sql.files.maxPartitionBytes=536870912  # 512MB
spark.files.openCostInBytes=33554432        # 32MB
```

**方案B：定期合并小文件（治本）**

```sql
-- 使用 Spark SQL 的 REPARTITION Hint 控制输出文件数
INSERT OVERWRITE TABLE target_table
SELECT /*+ REPARTITION(100) */ *
FROM source_table
WHERE dt = '2024-01-01';

-- 或使用 COALESCE Hint（减少输出文件数，不触发 Shuffle）
INSERT OVERWRITE TABLE target_table
SELECT /*+ COALESCE(50) */ *
FROM source_table
WHERE dt = '2024-01-01';
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 读取后写入时控制并行度，减少输出文件数
df.repartition(100).write.mode("overwrite").parquet("/output/path")

// 或者使用 coalesce（不加随机 seed 以保持有序合并）
df.coalesce(50).write.mode("overwrite").parquet("/output/path")
```

</details>

**方案C：使用 Delta Lake / Iceberg 的 Compaction 功能**

```scala
// Delta Lake 合并小文件
import io.delta.tables.DeltaTable

DeltaTable.forPath(spark, "/delta/table/path")
  .optimize()
  .executeCompaction()

// Iceberg 重写小文件
spark.sql("CALL catalog.system.rewrite_data_files('database.table_name')")
```

**方案D：调整 AQE 分区合并参数**

```properties
# 利用 AQE 在 Shuffle 后自动合并小分区，减少输出文件数
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.coalescePartitions.minPartitionSize=64MB
spark.sql.adaptive.coalescePartitions.initialPartitionNum=200
spark.sql.adaptive.advisoryPartitionSizeInBytes=256MB
```

---

### 小文件产生的典型场景与根因分析

小文件问题不仅影响读取性能，更是一个贯穿数据全生命周期的系统性问题。以下详细分析小文件的产生场景和根因。

#### 场景A：动态分区写入 — 高基数分区列导致文件数爆炸

**产生过程：**

某电商系统每小时将订单数据写入按 `dt`（日期）和 `seller_id`（商家ID）分区的 Hive 表。某天促销活动期间有 50000 个活跃商家。Spark 任务配置了 200 个 Shuffle 分区。

写入时，每个 Task 在遍历数据时，对于遇到的每一个 `(dt, seller_id)` 组合，都会在对应分区目录下创建一个新文件。因为 `seller_id` 有 50000 个不同值，即使每个商家只有几条记录，也会产生大量接近空的小文件。

最终结果：200 个 Task × 50000 个商家 = 理论上最多 10,000,000 个文件（实际受数据分布影响，通常是几万到几十万个）。

**核心机制：**

```
HDFS 写入路径 = /warehouse/table/dt=2024-01-01/seller_id=12345/part-00000.parquet
```

每个 Task 对每个动态分区值至少产生一个文件。如果分区列基数高，文件数 = Task数 × 分区列基数（最坏情况）。

**根因：**
- 动态分区列基数过高（如 merchant_id、user_id、session_id 等）。
- 数据在分区列上分布极不均匀（大量分区的数据量很小）。
- Spark 写入时每个 Task 独立写文件，不做跨 Task 合并。

#### 场景B：过度并行写入 — Shuffle 分区数远大于数据量所需

**产生过程：**

某运营报表任务每天处理 5GB 数据，但 `spark.sql.shuffle.partitions` 被全局设置为 2000（从大数据任务复制过来的配置）。写入时产生 2000 个文件，每个文件仅 2.5MB，远小于 HDFS 推荐块大小（128MB/256MB）。

**根因：**

写入文件数 ≈ 最后一个 Stage 的 Task 数（通常是 Shuffle 分区数）。Shuffle 分区数设置过大直接导致文件数过多。

| 数据量 | Shuffle 分区数 | 平均文件大小 | 是否合理 |
|--------|---------------|-------------|---------|
| 5GB    | 2000          | 2.5MB       | 文件太小 |
| 5GB    | 40            | 128MB       | 合理 |
| 100GB  | 2000          | 50MB        | 偏小 |
| 100GB  | 800           | 128MB       | 合理 |
| 1TB    | 2000          | 512MB       | 可接受 |

#### 场景C：流式/微批量写入 — 每个 Batch 产生一批新文件

**产生过程：**

某实时数仓使用 Spark Structured Streaming 每 1 分钟一个 Micro-batch 将 Kafka 数据写入 HDFS。数据流速约 1000 条/秒，每个 batch 处理 60000 条记录。每个 batch 写入产生 10 个 Parquet 文件（根据分区数），每个文件约 200KB。

运行一天后：24小时 × 60分钟 × 10文件 = 14400 个小文件。

运行一个月后：超过 40 万个文件在该分区目录下。

**根因：**
- 流式写入的 Micro-batch 间隔短，单 batch 数据量小。
- 每个 batch 独立产生一组文件，没有跨 batch 的合并机制。
- 流式任务长期运行，文件数持续累积。

#### 场景D：频繁的增量更新（MERGE / UPSERT）— Delta/Hudi/Iceberg 的写放大

**产生过程：**

某风控系统使用 Delta Lake，每 10 分钟运行一次 MERGE 操作更新订单状态（例如标记"已审核"），每次 MERGE 涉及整个表 1% 的数据（约 100 万条）。Delta Lake 的 Copy-on-Write 机制会：
1. 读取包含待更新记录的原始 Parquet 文件
2. 将更新后的数据写入新的 Parquet 文件
3. 标记旧文件为待删除（tombstone）

每次 MERGE 产生一批新文件，每天 144 次 MERGE，即使每条记录只被更新一次，也会产生大量需要 Vacuum 的过期文件。

**根因：**
- MERGE 操作的 Copy-on-Write 机制每次重写整个文件。
- 高频 UPDATE/DELETE 导致文件碎片化。
- Compaction/Vacuum 不及时导致过期文件堆积。

#### 场景E：分区粒度过细 — 时间粒度从"天"降到"小时"

**产生过程：**

某日志平台原先按 `dt=yyyy-MM-dd` 天分区，每天 500GB 数据，1000 个文件。后来为了支持更快的增量查询，改为按 `dt=yyyy-MM-dd/HH` 小时分区。

假设数据均匀分布：每个小时分区只有约 20GB 数据，但 Shuffle 分区数仍然是 200。每小时分区产生 200 个文件，每个文件仅 100MB。

一天下来：24小时 × 200文件 = 4800 个文件（远多于原来按天分区的 1000 个文件）。

**根因：**
- 分区粒度变细后，单个分区的数据量减少，但 Task 数不变。
- 分区列基数增加 N 倍，文件数也随之增加约 N 倍。

#### 场景F：Spark SQL INSERT OVERWRITE 未控制输出并行度

**产生过程：**

```sql
-- 危险：使用默认 Shuffle 分区数（200），可能产生 200 个文件
INSERT OVERWRITE TABLE target_partitioned
SELECT * FROM source_table WHERE dt = '2024-01-01';

-- 如果 source_table 有 10000 个小文件，可能导致更多零碎文件
```

**根因：**
- 未使用 `REPARTITION` 或 `COALESCE` Hint 控制输出文件数。
- 源表本身存在小文件问题，通过 SELECT 传递到目标表。
- 未开启 AQE 的自动分区合并功能。

#### 场景G：多级分区 + JOIN 导致写入放大

**产生过程：**

```sql
-- 三个分区列：dt, category, region
INSERT INTO TABLE sales_report PARTITION(dt, category, region)
SELECT
  /* 数据来自多个表的 JOIN */
  a.transaction_date AS dt,
  b.product_category AS category,
  c.store_region AS region,
  a.amount
FROM transactions a
JOIN products b ON a.product_id = b.id
JOIN stores c ON a.store_id = c.id
WHERE a.transaction_date = '2024-01-01';
```

假设：
- `category` 有 300 个值
- `region` 有 50 个值
- Shuffle 分区数 = 200

最坏文件数 = 200(Task) × 组合的分区数。JOIN 后的 Shuffle 使得数据按(category, region)重新分布，每个 Task 可能触达多个分区组合，文件数可能达到数千至数万个。

**根因：**
- JOIN 操作改变了数据分布，Shuffle 后数据被重新分区。
- 多级分区列的组合基数很高时，Task 遍历的(分区列组合)数量极大。
- 每个 Task 内按分区列拆分写文件，存在 1:N 的放大效应。

---

### 小文件的系统性危害

| 层面 | 危害 | 说明 |
|------|------|------|
| **HDFS NameNode** | 内存压力 | 每个文件/目录/block 在 NameNode 内存中占用约 150 bytes 元数据，1 亿个小文件占用约 15GB NameNode 内存 |
| **读取性能** | Task 数量爆炸 | 如场景7所述，每个文件至少产生 1 个 Task |
| **读取性能** | IO 效率低 | 大量随机读取代价远高于顺序读取大文件，磁盘寻道时间增加 |
| **查询性能** | 元数据扫描慢 | 分区目录下列出文件（listStatus）操作随文件数线性增长 |
| **写入性能** | Commit 慢 | Hive/Spark 写入后需要 rename 和 commit 文件，文件数多时 commit 阶段耗时可能超过计算阶段 |
| **压缩效率** | 压缩比降低 | 小文件内的数据量少，列式存储的字典编码和压缩算法效果差 |
| **数据跳跃** | 谓词下推失效 | Parquet 文件内没有足够的 Row Group 统计信息用于谓词下推 |

---

### 小文件治理策略总结

| 策略 | 适用场景 | 实施方式 |
|------|---------|---------|
| **写入时合并** | 所有 Spark 写入 | 使用 `REPARTITION`/`COALESCE` Hint 控制输出文件数 |
| **AQE 自动合并** | Spark 3.0+ | `spark.sql.adaptive.coalescePartitions.enabled=true` |
| **定时 Compaction** | Delta/Iceberg/Hudi 表 | 定期运行 `OPTIMIZE`/`rewrite_data_files` 合并小文件 |
| **调整分区粒度** | 设计阶段 | 选择合适的分区列，避免基数过高的列作为分区列 |
| **流式写入合并** | Structured Streaming | 使用 `foreachBatch` + `REPARTITION`，或定期 Compaction |
| **文件大小目标** | 所有写入 | 每个文件目标 128MB-512MB（HDFS block 大小的 1-4 倍） |
| **HDFS 联邦** | 超大规模集群 | 多个 NameNode 分担元数据压力 |

---

## 场景8：读取数据源时分区数不合理导致并行度不足或过多

### 场景描述

一个数据迁移任务，从 HDFS 读取约 2TB 的 Parquet 数据，经过简单的 ETL 转换后写入另一个集群。任务配置了 50 个 Executor，每个 4 核。但由于默认的分区计算逻辑，只产生了 20 个分区（Task），导致：
- 只有 20 个 Core 在工作，剩余 180 个 Core 闲置
- 每个 Task 处理约 100GB 数据，内存不够导致大量 Spill
- 任务运行 8 小时才完成

反过来，另一个场景：从大量小文件中读取数据，产生 50000 个 Task，每个 Task 只处理几 MB 数据，Task 调度开销远大于计算开销。

### 排查思路

1. **Spark UI 查看并行度**：在 Stage 详情中看 Task 数量和 Executor Core 比例。
2. **计算理想分区数**：2-3 倍于总 Core 数（`分区数 ≈ 总Core × (2~3)`）。
3. **检查分区大小**：每个分区的数据量建议在 128MB-256MB 之间。
4. **分析文件数**：确认输入文件数与分区数的对应关系。

### 解决办法

**读取时控制分区数：**

```sql
-- 方式1：创建临时视图后用 REPARTITION Hint 控制分区
CREATE OR REPLACE TEMP VIEW repartitioned_data AS
SELECT /*+ REPARTITION(400) */ *
FROM parquet.`/data/path`;

-- 方式2：使用 COALESCE Hint 减少输出分区（避免 Shuffle）
SELECT /*+ COALESCE(50) */ *
FROM parquet.`/data/path`;
```

<details>
<summary>JDBC 读取时显式设置分区（DataFrame API）</summary>

```scala
// JDBC 读取时显式设置分区
val jdbcDF = spark.read.format("jdbc")
  .option("url", jdbcUrl)
  .option("dbtable", tableName)
  .option("user", user)
  .option("password", password)
  .option("numPartitions", 100)
  .option("partitionColumn", "id")
  .option("lowerBound", 1)
  .option("upperBound", 10000000)
  .load()
```

</details>

**参数调整：**

```properties
# 控制每个分区最大数据量（增大 = 减少分区数）
spark.sql.files.maxPartitionBytes=268435456    # 256MB

# Split 的打开开销（小于此值的文件合并）
spark.sql.files.openCostInBytes=16777216       # 16MB

# 默认 Shuffle 分区数
spark.sql.shuffle.partitions=400

# 并行文件读取的最大分区数
spark.sql.files.maxPartitionNum=2000
```

**经验公式：**

```
推荐 Shuffle 分区数 = 总 Executor Core 数 × 2 到 3 倍
读取文件分区数 = 总数据量 / 期望的分区大小（128MB~256MB）
```

---

## 场景9：Hive SQL on Spark — 执行计划不合理

### 场景描述

某数据仓库团队将 Hive on MR 迁移到 Hive on Spark（或直接使用 Spark SQL）。一条很简单的 SQL（`SELECT a, count(*) FROM table WHERE dt='2024-01-01' GROUP BY a`）在 Hive on MR 上 10 分钟完成，在 Spark 上却跑了 1 小时。检查发现 Spark 生成了一个效率很低的执行计划：先做全表 Sort 再做聚合，而不是直接在 Map 端聚合（因为 Hive 表的统计信息缺失或不准确）。

### 排查思路

1. **对比执行计划**：使用 `EXPLAIN EXTENDED` 查看执行计划的详细成本信息。
2. **检查表统计信息**：通过 `DESCRIBE EXTENDED table` 或 `ANALYZE TABLE` 检查统计信息是否准确。
3. **Spark UI SQL 页签**：查看 SQL 的 DAG 图，分析每个 Stage 的数据量是否合理。
4. **检查 CBO（Cost-Based Optimizer）配置**：是否开启了基于成本的优化。
5. **对比原始 Hive 和 Spark SQL 的结果**：确认语义等价，排除语法兼容性问题。

### 解决办法

**更新统计信息：**

```sql
-- 更新表级统计信息
ANALYZE TABLE my_table COMPUTE STATISTICS;

-- 更新分区统计信息
ANALYZE TABLE my_table PARTITION(dt='2024-01-01') COMPUTE STATISTICS;

-- 更新列级统计信息
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS col1, col2;
```

**开启 CBO 优化：**

```properties
# 开启 CBO（Spark 2.2+）
spark.sql.cbo.enabled=true
spark.sql.cbo.joinReorder.enabled=true
spark.sql.cbo.starSchemaDetection=true

# 开启统计信息收集
spark.sql.statistics.histogram.enabled=true
spark.sql.statistics.size.autoUpdate.enabled=true
```

**SQL 改写优化：**

```sql
-- 不好的写法：子查询中包含不必要的列
SELECT a, count(*) FROM (
  SELECT * FROM table WHERE dt = '2024-01-01'
) t GROUP BY a;

-- 好的写法：尽早裁剪列和过滤
SELECT a, count(*) FROM table
WHERE dt = '2024-01-01'
GROUP BY a;

-- 不好的写法：先 Distinct 再 Count
SELECT count(DISTINCT a) FROM table;

-- 好的写法（如果允许近似值）
SELECT approx_count_distinct(a) FROM table;
```

**Hive on Spark 特殊参数：**

```properties
# Hive on Spark 模式
set hive.execution.engine=spark;

# 调整 Spark 的并行度
set spark.executor.cores=4;
set spark.executor.memory=8g;
set spark.sql.shuffle.partitions=200;

# 启用向量化查询
set spark.sql.hive.convertMetastoreParquet=true;
set spark.sql.parquet.enableVectorizedReader=true;
```

---

## 场景10：流处理（Structured Streaming）背压与延迟

### 场景描述

某实时监控系统使用 Spark Structured Streaming 从 Kafka 消费传感器数据（每秒约 10 万条消息），进行窗口聚合后写入 InfluxDB。在生产运行中，发现：
- 消费延迟（Consumer Lag）持续增长，从最初的几秒增长到 30 分钟以上
- 监控告警显示处理延迟（Processing Delay）持续增大
- 部分 Micro-batch 处理时间超过 Batch Interval
- Kafka 日志积压严重

### 排查思路

1. **Streaming Query 指标监控**：通过 `StreamingQueryListener` 或 Spark UI 的 Streaming 页签获取 `inputRowsPerSecond`、`processedRowsPerSecond`、`numInputRows` 等指标。
2. **对比输入速率和处理速率**：如果 `inputRowsPerSecond` > `processedRowsPerSecond`，说明存在背压。
3. **分析瓶颈 Stage**：在 Spark UI 中查看是哪个 Stage 处理时间最长。
4. **检查 State 大小**：如果使用了有状态操作（如 `flatMapGroupsWithState`、`mapGroupsWithState`），检查 State Store 的大小和读写延迟。
5. **检查 Sink 写入性能**：确认下游存储（如 MySQL、HBase、InfluxDB）是否成为瓶颈。

### 解决办法

**背压控制参数：**

```properties
# 限制每个 Trigger 的最大 Offset 数量（Kafka）
spark.sql.streaming.kafka.maxOffsetsPerTrigger=100000

# 控制每批最大处理数据条数（文件源）
spark.sql.streaming.fileSource.maxFilesPerTrigger=100

# 开启背压机制（Spark 2.3+，根据处理速率动态调整消费速率）
spark.streaming.backpressure.enabled=true
spark.streaming.backpressure.initialRate=1000  # 初始每秒处理条数
spark.streaming.kafka.maxRatePerPartition=5000  # 每个 Kafka 分区最大处理速率
```

**State Store 优化：**

```properties
# 使用 RocksDB 作为 State Store（替代默认的 HDFSBackedStateStore）
spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider

# 配置 RocksDB 缓存
spark.sql.streaming.stateStore.rocksdb.blockCacheSizeMB=512
spark.sql.streaming.stateStore.rocksdb.writeBufferSizeMB=64

# 设置 State 的保留时间（清理过期状态）
spark.sql.streaming.stateStore.minDeltasForSnapshot=10

# 开启状态压缩
spark.sql.streaming.stateStore.compression.enabled=true
spark.sql.streaming.stateStore.compression.codec=zstd
```

**触发间隔调整：**

```scala
val query = df.writeStream
  .trigger(Trigger.ProcessingTime("30 seconds"))  // 不要设置过短的触发间隔
  .option("checkpointLocation", "/checkpoint/path")
  .outputMode("append")
  .foreachBatch { (batchDF, batchId) =>
    // 在 foreachBatch 中对每个 Micro-batch 做批量写入优化
    batchDF
      .repartition(10)  // 控制写入并行度
      .write
      .mode("append")
      .jdbc(url, table, props)
  }
  .start()
```

**Sink 写入优化：**

- 外部存储使用批量写入 API，减少连接数。
- 使用连接池，避免频繁建立/销毁连接。
- 如果写 MySQL，调整 `rewriteBatchedStatements=true`。

---

## 场景11：动态资源分配导致的性能抖动

### 场景描述

某推荐系统的离线训练数据准备任务，开启了 YARN 上的动态资源分配（Dynamic Allocation）。任务启动后分配到 20 个 Executor，运行 30 分钟后弹性扩展到 200 个 Executor。但在扩展过程中：
- Executor 频繁申请和释放，导致任务运行时间波动大
- Shuffle 数据在 Executor 释放后需要重新计算（因为 Executor 携带了部分 Shuffle 数据）
- 有时释放的 Executor 上正在进行 Shuffle Read，导致 Fetch 失败和 Task 重试
- 总体任务时间从 2 小时延长至 3.5 小时

### 排查思路

1. **Spark UI Executors 页签**：观察 Executor 数量的变化趋势，是否频繁震荡。
2. **检查 Executor 空闲时间配置**：`spark.dynamicAllocation.executorIdleTimeout` 是否设置过短。
3. **分析 Shuffle 依赖**：开启 External Shuffle Service 可避免 Executor 被移除时丢失 Shuffle 数据。
4. **任务负载分析**：任务的不同阶段资源需求不同，初期可能只需要少量 Executor（过滤阶段），Shuffle 阶段需要更多 Executor。

### 解决办法

**优化动态分配参数：**

```properties
# 启用动态分配
spark.dynamicAllocation.enabled=true

# 必须开启 External Shuffle Service（防止 Executor 被移除时丢失 Shuffle 数据）
spark.shuffle.service.enabled=true

# 延长空闲超时，避免频繁释放
spark.dynamicAllocation.executorIdleTimeout=120s

# 缓存数据超时（Executor 上有缓存数据时不释放）
spark.dynamicAllocation.cachedExecutorIdleTimeout=600s

# 设置最小和最大 Executor 数，避免完全从零开始
spark.dynamicAllocation.minExecutors=10
spark.dynamicAllocation.maxExecutors=100

# 初始 Executor 数量
spark.dynamicAllocation.initialExecutors=20

# 每次弹性调整的步长（避免一次增删太多）
spark.dynamicAllocation.schedulerBacklogTimeout=5s
spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=15s
```

**如果动态分配引起 Shuffle 重算问题过于严重：**

```properties
# 关闭动态分配，使用固定资源
spark.dynamicAllocation.enabled=false
spark.executor.instances=80
```

**权衡建议：**

- 对于 **ETL 类** 任务（长时间运行，Shuffle 数据量大）：建议关闭动态分配。
- 对于 **Ad-hoc 查询** 或交互式分析：建议开启动态分配。
- 对于 **多租户集群**：开启动态分配以提高整体资源利用率。

---

## 场景12：大量 UDF 调用导致性能瓶颈

### 场景描述

某 NLP 处理管道使用 Spark 对文本进行分词、实体识别和情感分析。代码中定义了多个 Python UDF：

```python
from pyspark.sql.functions import udf

@udf("string")
def complex_tokenize(text):
    # 加载大型词典，做复杂分词
    import jieba
    return " ".join(jieba.cut(text))

# 对 1 亿行文本应用此 UDF
result = df.withColumn("tokens", complex_tokenize(col("text")))
```

任务运行极慢，Spark UI 显示 90% 的时间消耗在 UDF 执行上，而且 CPU 利用率很低（因为 Python UDF 需要序列化数据到 Python 进程）。

排查发现每个 Row 都需要序列化到 Python、反序列化、执行 UDF、再序列化回 JVM。

### 排查思路

1. **Spark UI 分析 CPU 利用率**：如果 UDF 多且 CPU 低，说明大量时间花在序列化上。
2. **对比 Scala/Java UDF 和 Python UDF 的性能**：Python UDF 通常比 Scala UDF 慢 10-100 倍。
3. **分析 UDF 内部逻辑**：是否有不必要的初始化（如每次调用加载模型/词典）。
4. **SQL 执行计划**：检查 UDF 是否阻碍了优化（如谓词下推、列裁剪）。

### 解决办法

**方案A：将 Python UDF 改为 Pandas UDF（提升 100 倍）**

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf("string")
def tokenize_udf(texts: pd.Series) -> pd.Series:
    import jieba
    return texts.apply(lambda t: " ".join(jieba.cut(t)) if t else "")
```

**方案B：用 Spark SQL 内置函数替代 UDF**

尽可能使用 Spark SQL 内置函数，避免 UDF。例如：

```sql
-- 不好的写法：自定义 UDF
SELECT my_upper(name) AS upper_name FROM table;

-- 好的写法：使用内置函数
SELECT UPPER(name) AS upper_name FROM table;

-- 更多内置函数替代 UDF 的示例
-- 字符串处理：UPPER、LOWER、TRIM、CONCAT、SUBSTRING、REGEXP_REPLACE
-- 日期处理：DATE_FORMAT、DATE_ADD、DATEDIFF、UNIX_TIMESTAMP、TO_DATE
-- 数学运算：ROUND、ABS、CEIL、FLOOR、POWER、SQRT
-- 条件判断：CASE WHEN、IF、COALESCE、NULLIF
-- JSON 处理：GET_JSON_OBJECT、FROM_JSON、TO_JSON
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 不好的写法
val upperDF = df.withColumn("upper_name", udf((s: String) => s.toUpperCase).apply(col("name")))

// 好的写法
val upperDF = df.withColumn("upper_name", upper(col("name")))
```

</details>

**方案C：将 UDF 中重复的初始化逻辑移到 mapPartitions**

```scala
// 每个 Partition 初始化一次模型，而非每条记录初始化一次
df.mapPartitions { partition =>
  // 分区级别初始化（只执行一次）
  val tokenizer = new Tokenizer() // 加载模型/词典

  partition.map { row =>
    val tokens = tokenizer.tokenize(row.getString(0))
    (row.getString(0), tokens)
  }
}
```

**方案D：对于 Java/Scala UDF，使用 Codegen 友好的写法**

```scala
// 避免在 UDF 中使用不可变对象的频繁创建
// 使用 while 循环替代 foreach
// 尽量减少 if/else 分支（不利于 JIT 编译）
```

---

## 场景13：Kryo 序列化缓冲区不足

### 场景描述

某图计算应用使用 Spark GraphX，数据量大且对象关系复杂。运行时报出：

```
java.lang.IllegalArgumentException: Should use a buffer size > 0
  at com.esotericsoftware.kryo.io.Output.<init>
```

或：

```
com.esotericsoftware.kryo.KryoException: Buffer overflow. Available: 0, required: X
  Serialization trace:
  ...
```

Task 失败并且重试多次。

### 排查思路

1. **确认序列化配置**：检查是否启用了 Kryo 序列化以及缓冲区大小。
2. **分析序列化对象大小**：通过 Spark UI 查看 Task 的 Shuffle Write 数据量和 Shuffle Spill 数据量。
3. **检查是否注册了自定义类**：未注册的类 Kryo 会用全类名序列化，体积大且慢。
4. **对比 Java 序列化**：临时切回 Java 序列化看是否正常，确认问题在 Kryo。

### 解决办法

**参数调整：**

```properties
# 使用 Kryo 序列化
spark.serializer=org.apache.spark.serializer.KryoSerializer

# 增大 Kryo 缓冲区（默认 64k，可能不够）
spark.kryoserializer.buffer=1024k

# 增大 Kryo 最大缓冲区（缓冲区不够时会自动扩容到此值）
spark.kryoserializer.buffer.max=512m

# 注册自定义类（极大影响序列化性能）
spark.kryo.classesToRegister=com.example.MyClass,com.example.MyOtherClass

# 要求所有类都注册（更严格的序列化，避免未注册类用全名序列化）
spark.kryo.registrationRequired=false
```

**代码优化：**

```scala
// 注册自定义类
val conf = new SparkConf()
  .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  .registerKryoClasses(Array(
    classOf[MyClass],
    classOf[MyEdgeClass],
    classOf[Array[MyClass]]
  ))
```

**使用更高效的序列化方式：**

对于图计算或机器学习场景，如果对象太大，考虑：
- 使用 Protobuf/FlatBuffers 等更紧凑的序列化格式自定义序列化逻辑。
- 将大对象拆分为多个小对象，分批传输。

---

## 场景14：Join 策略选择不当 — SortMergeJoin 性能差

### 场景描述

某风控系统需要将交易表（fact_transaction, 100GB, 按`txn_id`分区）与商户信息表（dim_merchant, 200MB）进行 Join 以补充商户信息。由于没有显式指定 Broadcast Hint，且 Spark 统计信息不准确，Spark 选择了 SortMergeJoin。结果：
- 100GB 的交易表需要全量 Shuffle
- Shuffle Write 数据量达到 100GB，严重影响集群 IO
- Join 耗时超过 40 分钟

同样的逻辑，如果使用 Broadcast Join，将 200MB 的维度表广播到每个 Executor，只需 3 分钟。

### 排查思路

1. **Spark SQL 执行计划**：`df.explain("extended")` 查看 Join 策略。
2. **检查统计信息**：`DESCRIBE EXTENDED` 确认表的统计信息是否准确。
3. **比较 Join 侧的数据大小**：确认哪个表更适合广播。
4. **检查 Broadcast Join 阈值**：是否因阈值太小导致自动广播失败。

### 解决办法

**参数调整：**

```properties
# 增大自动广播阈值（200MB 的表应该能被自动广播）
spark.sql.autoBroadcastJoinThreshold=524288000  # 500MB

# 如果数据量超出阈值但仍想广播，使用 Broadcast Hint
```

**使用 Broadcast Hint：**

```scala
import org.apache.spark.sql.functions.broadcast

val result = factTable.join(broadcast(dimTable), "merchant_id")
```

```sql
SELECT /*+ BROADCAST(d) */ f.*, d.merchant_name
FROM fact_transaction f
JOIN dim_merchant d ON f.merchant_id = d.id
```

**如果两个表都很大，选择小表广播（扩大广播阈值）：**

```scala
// 对其中一个表进行过滤裁剪，减小体积后再广播
val smallSide = largeTableA
  .select("key", "needed_col") // 只取 Join Key 和需要的列
  .filter(col("date") === "2024-01-01") // 只取当天的数据
  .cache()

val bigSide = largeTableB
val result = bigSide.join(broadcast(smallSide), "key")
```

**其他 Join 策略优化：**

```scala
// 如果两个表都很大且已经分桶，使用 Bucket Join 避免 Shuffle
// 前提：两张表按相同的 Key 分相同数量的桶
spark.sql("SET spark.sql.sources.bucketing.enabled=true")

// Shuffle Hash Join 在一侧数据量适中时比 SortMergeJoin 快
// Spark 3.0+ 中 AQE 会自动转换
spark.sql.adaptive.enabled=true
spark.sql.adaptive.localShuffleReader.enabled=true
```

---

## 场景15：磁盘 IO 成为瓶颈 — Shuffle Spill 频繁

### 场景描述

某画像平台计算用户标签，需要对海量行为数据按用户维度进行复杂的多 Key 聚合。运行时发现：
- 通过 Ganglia / Prometheus 监控发现节点磁盘 IO 利用率持续 100%
- `iostat -x 1` 显示 `await`（等待时间）高达 200ms 以上
- Spark UI 显示 Shuffle Spill (Memory) 和 Shuffle Spill (Disk) 数值很大
- Task 进度缓慢，GC 时间也偏长

### 排查思路

1. **操作系统级监控**：`iostat`、`iotop`、`dstat` 查看磁盘 IO 利用率。
2. **Spark UI 任务指标**：查看 Shuffle Spill(Memory) 和 Spill(Disk) 量。
3. **Executor 日志**：看是否有 "Shuffle data too large to fit in memory" 相关日志。
4. **确认存储介质**：`df -h` 和 `lsblk` 确认磁盘类型（HDD vs SSD）。
5. **内存使用率**：查看 `spark.memory.fraction` 和 `spark.memory.storageFraction` 是否合理。

### 解决办法

**参数调整：**

```properties
# 增大 Execution 内存占比，减少 Spill
spark.memory.fraction=0.8
spark.memory.storageFraction=0.3  # Storage 只占 30%，Execution 有更多空间

# 减少单次 Shuffle Fetch 的数据量
spark.reducer.maxSizeInFlight=24m  # 减小，降低网络+磁盘压力

# 增加到多块磁盘
spark.local.dir=/ssd1/spark,/ssd2/spark,/ssd3/spark,/ssd4/spark

# 使用压缩减少磁盘写入
spark.shuffle.compress=true
spark.io.compression.codec=zstd
```

**代码优化：**

```sql
-- Map 端预聚合：使用 GROUP BY 聚合，Spark 会自动在 Map 端做预聚合（Partial Aggregation）
-- 避免先展开再聚合的中间结果膨胀
SELECT key, SUM(value) AS total
FROM source_table
GROUP BY key;

-- 多个聚合函数一起计算（一次 Shuffle 完成多个聚合）
SELECT
  key,
  SUM(value)   AS total,
  COUNT(value) AS cnt,
  AVG(value)   AS avg_val
FROM source_table
GROUP BY key;
```

<details>
<summary>等价的 RDD API 写法（点击展开）</summary>

```scala
// Map 端预聚合，减少 Shuffle 数据量
// 不好的写法
rdd.groupByKey().mapValues(_.sum)

// 好的写法
rdd.reduceByKey(_ + _) // Map 端预聚合
rdd.aggregateByKey(zeroValue)(seqOp, combOp) // 带初始值的预聚合

// 使用 DataFrame API 而非 RDD
// DataFrame 自动做 Map 端预聚合
df.groupBy("key").agg(sum("value"))
```

</details>

**硬件升级：**

- 将 `spark.local.dir` 指向 **SSD 或 NVMe 磁盘**。
- 确保每个 Executor 分配 2-4 块独立磁盘，并行读写。
- 如果使用云环境，选择 IOPS 优化的实例类型。

---

## 场景16：数据本地性差导致网络传输开销大

### 场景描述

某广告平台的数据清洗任务，需要从 HDFS 读取广告曝光日志后处理。任务分配了 200 个 Executor，但 Spark UI 的 Stage 详情中显示：
- `PROCESS_LOCAL` 比例很低（< 10%）
- `RACK_LOCAL` 或 `ANY` 比例很高（> 60%）
- 网络流量很大（Ganglia 监控显示节点间网络带宽打满）
- Task 执行时间波动大

检查发现，由于 Executor 被调度到了没有 HDFS 数据副本的节点上，大量数据需要跨网络读取。

### 排查思路

1. **Spark UI Stage 详情**：查看 Task 级别的 Locality Level 分布。
2. **检查 `spark.locality.wait`**：是否给数据本地调度留了足够时间。
3. **HDFS 副本分布**：`hdfs fsck /path -files -blocks -locations` 确认数据分布。
4. **集群拓扑**：检查 Executor 节点和 DataNode 节点的对应关系。
5. **动态资源分配影响**：动态分配可能导致 Executor 在数据不在的节点上启动。

### 解决办法

**参数调整：**

```properties
# 增加本地性等待时间（给调度器更多时间找到有数据的节点）
spark.locality.wait=5s          # 默认 3s
spark.locality.wait.process=5s  # PROCESS_LOCAL
spark.locality.wait.node=8s     # NODE_LOCAL
spark.locality.wait.rack=10s    # RACK_LOCAL

# 数据本地性等待超时后降级的延迟
spark.locality.wait.stage=30s
```

**集群调度策略：**

- 使用 `spark.yarn.executor.nodeLabelExpression` 将 Executor 调度到有数据的节点。
- 如果 HDFS 数据和计算节点分离，考虑开启数据本地性延迟等待。

**Spark on YARN 节点标签：**

```properties
# 将 Spark Executor 调度到特定的 YARN Node Label
spark.yarn.executor.nodeLabelExpression=spark-executor

# 或者在提交命令中指定
--conf spark.yarn.executor.nodeLabelExpression=spark-executor
```

**数据布局优化：**

```bash
# 增加 HDFS 副本数，增大数据本地性概率
hdfs dfs -setrep -w 3 /data/path
```

---

## 场景17：推测执行导致资源浪费或任务重复

### 场景描述

某用户画像系统的工作流，包含 50+ 个 Spark 任务。开启了推测执行后：
- 发现某些 Task 因为 GC 暂停导致进度报告延迟，Spark 错误地启动了推测副本来执行相同的 Task
- 推测副本和原 Task 都运行完毕，造成 2 倍的计算资源浪费
- 某些非幂等的写入操作因为两个副本同时写导致数据错误

另一方面，对于某些确实运行缓慢的 Task（数据倾斜导致），推测执行虽然会启动副本，但倾斜 Key 的数据在两个 Executor 上都一样慢，没有起到加速效果。

### 排查思路

1. **Spark UI 查看推测副本**：在 Task 列表中，推测执行的 Task 会有标记。
2. **检查发生推测执行的原因**：是 GC 慢还是数据倾斜。
3. **评估资源影响**：对比开启/关闭推测执行的总体任务时间。
4. **确认写入操作幂等性**：如果输出操作不幂等，关闭推测执行。

### 解决办法

**参数调整：**

```properties
# 关闭推测执行（默认 false，但建议显式设置）
spark.speculation=false

# 如果一定要开启，调整更保守的参数
spark.speculation=true
spark.speculation.interval=5000ms         # 检查频率，不要太频繁
spark.speculation.multiplier=3            # 进度倍数，大于此倍数才推测
spark.speculation.quantile=0.9            # 需要推测执行的百分比阈值

# 排除某些对幂等性敏感的 Stage
# 可以在代码级别对某些 RDD 操作禁用推测执行
```

**代码中的处理：**

```scala
// 对于非幂等的输出操作，显式禁用推测执行
df.rdd
  .mapPartitions { partition =>
    // 非幂等操作，如调用外部 API
    partition.foreach { row =>
      sendToExternalAPI(row)
    }
    Iterator.empty
  }
  // 设置允许推测执行 = false（需要在 SparkContext 级别配置）
```

**最佳实践：**

- 对于 **ETL 管道**（Shuffle 数据量大、输出写入 HDFS 等幂等操作）：谨慎开启。
- 对于 **机器学习训练**（迭代计算，单 Task 慢是常见现象）：关闭推测执行。
- 优先通过解决数据倾斜、GC 问题来减少慢 Task，而不是依赖推测执行。

---

## 场景18：YARN 资源不足 — 任务长时间排队

### 场景描述

某大数据平台的 YARN 集群有 500 个节点，每个节点 64GB 内存、16 核。一个 Spark 应用申请 200 个 Executor，每个 8GB 内存、4 核。任务提交后长时间处于 ACCEPTED 状态：

```
2024-01-01 10:00:00 INFO  Client: Application report for application_xxx (state: ACCEPTED)
...
2024-01-01 11:30:00 INFO  Client: Application report for application_xxx (state: ACCEPTED)
```

查看 YARN RM UI 发现：
- 集群总内存使用率 90%+
- 大量 Spark 任务在排队
- 部分 Executor 请求的资源（如 2 核 16GB）超过了单个 NodeManager 的可用资源，导致永远无法分配

### 排查思路

1. **YARN RM UI / YARN CLI**：`yarn application -list` 查看所有应用状态和资源使用。
2. **ResourceManager 日志**：检查资源分配日志。
3. **检查单个 Executor 资源配置**：确认是否超过单个 NM 的最大分配限制。
4. **分析队列容量**：`yarn queue -status <queue_name>` 查看队列资源限制。
5. **检查资源碎片**：是否有许多小资源请求导致资源碎片化。

### 解决办法

**YARN 层面的参数调整：**

```properties
# YARN yarn-site.xml
# 增大 NodeManager 分配给 Container 的内存
yarn.nodemanager.resource.memory-mb=57344  # 56GB (64GB 物理内存)

# 调整最小分配单元（避免碎片）
yarn.scheduler.minimum-allocation-mb=2048  # 2GB
yarn.scheduler.maximum-allocation-mb=32768 # 32GB

# 调整队列容量
yarn.scheduler.capacity.root.default.capacity=80
yarn.scheduler.capacity.root.default.maximum-capacity=90
```

**Spark 资源申请优化：**

```properties
# 每个 Executor 的内存不要设置得太极端
# 太大：容易超过单机资源，排队等待时间长
# 太小：Task 并行度不够，Executor 数量太多增加调度压力
spark.executor.memory=8g    # 建议 4g-16g 之间
spark.executor.cores=4      # 建议 3-5 核（HDFS 吞吐量限制）

# 使用动态资源分配，按需获取
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=5
spark.dynamicAllocation.maxExecutors=200

# 初始申请适量的 Executor
spark.dynamicAllocation.initialExecutors=20
```

**提交策略优化：**

```bash
# 提交时指定优先级（优先级高的先分配）
spark-submit \
  --master yarn \
  --queue root.high_priority \
  --conf spark.yarn.priority=1 \
  ...

# 或者使用 Fair Scheduler 配置资源抢占
```

**任务拆分：**

- 将超大任务拆分为多个小任务，分阶段提交。
- 使用 Oozie / Airflow 编排，前一个任务完成后释放资源再提交下一个。

---

## 场景19：Spark SQL 谓词下推失效

### 场景描述

某数据仓库查询场景，从 Parquet 格式的订单表（按 `dt` 分区，总计 500 个分区、10TB 数据）中查询某一天的数据：

```sql
-- 期望只扫描 dt='2024-01-01' 分区，实际却扫描了全表
SELECT order_id, user_id, amount
FROM orders
WHERE dt = '2024-01-01' AND amount > 100;
```

发现任务扫描了大量不必要的分区（Scan 了全部 500 个分区），耗时极长。

排查发现：
- 分区列 `dt` 不是 Parquet 文件的 Partition 列（只是普通列），真正的分区列是 `order_date`。
- 或者因为 UDF / 复杂表达式导致谓词无法下推。
- 或者使用了不支持谓词下推的数据源格式。

### 排查思路

1. **查看执行计划**：`df.explain("extended")` 查看 `PushedFilters` 部分是否只读或 `*`。
2. **检查分区信息**：`DESCRIBE EXTENDED table` 或 Spark catalog 查看分区定义。
3. **分析 Filter 表达式**：确认过滤条件是否使用了 Spark 优化器可以下推的表达式。
4. **确认文件格式**：Parquet、ORC 支持谓词下推；JSON、CSV、Avro 支持有限。

### 解决办法

**确保分区裁剪生效：**

```sql
-- 正确的分区过滤：使用真正的分区列过滤
SELECT order_id, user_id, amount
FROM orders
WHERE order_date = '2024-01-01' AND amount > 100;

-- 直接读取特定分区目录
-- spark.read.option("basePath", "/data/orders").parquet("/data/orders/order_date=2024-01-01")
```

```properties
# 分区相关参数
spark.sql.sources.partitionOverwriteMode=dynamic
spark.sql.hive.caseSensitiveInferenceMode=NEVER_INFER
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 正确的分区过滤写法（使用 DataFrameReader 的分区过滤）
val orders = spark.read
  .parquet("/data/orders")
  .filter(col("order_date") === "2024-01-01") // 使用真正的分区列

// 更好的写法：在读取时就指定分区
val orders = spark.read
  .option("basePath", "/data/orders")
  .parquet("/data/orders/order_date=2024-01-01")
```

</details>

**确保列式存储的谓词下推（Parquet/ORC）：**

```scala
// Parquet 谓词下推需要以下配置
spark.sql.parquet.filterPushdown=true                  // 默认 true
spark.sql.parquet.mergeSchema=false                    // 避免模式合并
spark.sql.hive.convertMetastoreParquet=true
spark.sql.hive.convertMetastoreParquet.mergeSchema=false

// ORC 谓词下推
spark.sql.orc.filterPushdown=true
```

**避免阻断谓词下推的写法：**

```sql
-- 不好的写法：UDF 阻断下推，无法在文件级别过滤
SELECT * FROM orders
WHERE my_udf(amount) > 100 AND dt = '2024-01-01';

-- 好的写法：先用基础表达式下推，再应用 UDF
SELECT * FROM (
  SELECT * FROM orders
  WHERE dt = '2024-01-01' AND amount IS NOT NULL  -- 先下推
) t
WHERE my_udf(amount) > 100;

-- 不好的写法：模糊匹配可能阻断下推
SELECT * FROM orders WHERE name LIKE '%keyword%';

-- 好的写法：如果只是前缀匹配，可以使用
SELECT * FROM orders WHERE name LIKE 'keyword_%';
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 不好的写法：UDF 阻断下推
df.filter(udf_condition(col("amount")) > 100)

// 好的写法：先下推基本的过滤条件
df.filter(col("dt") === "2024-01-01" && col("amount") > 100)

// 不好的写法：模糊匹配可能阻断下推
df.filter(col("name").contains("keyword"))

// 好的写法：如果只是前缀匹配，使用 startWith
df.filter(col("name").startsWith("keyword_"))
```

</details>

**数据格式选择：**

```properties
# 优选支持高效谓词下推的格式
# Parquet: 列式存储 + 谓词下推 + 字典编码
# ORC: 列式存储 + 谓词下推 + 轻量级索引（Bloom Filter）
# 避免：JSON、CSV（无法高效下推）、Avro（行式存储）
```

---

## 场景20：Cartesian Product / Cross Join 导致数据膨胀

### 场景描述

某推荐系统需要计算用户与商品的相似度矩阵，使用了 Cross Join（笛卡尔积）。100 万用户 × 10 万商品 = 1000 亿条记录。任务：
- 运行了几个小时没有产出
- Shuffle 和 Spill 数据量极大（几十 TB）
- 部分 Executor OOM
- Spark UI 中某个 Stage 的 Shuffle Read 量远超预期

警告：如果在 Spark 代码中看到以下模式，几乎一定有 Cross Join：

```sql
-- 笛卡尔积：没有 ON 条件或 CROSS JOIN
SELECT * FROM table_a CROSS JOIN table_b;

-- 隐式笛卡尔积（多表查询忘记加关联条件）
SELECT * FROM table_a a, table_b b;  -- 危险！
```

### 排查思路

1. **Spark UI SQL 页签**：检查执行计划中是否有 `CartesianProduct` 节点。
2. **检查 Join 条件**：确认 `join(df2, "key")` 或 `join(df2, col("a") === col("b"))` 是否正确。
3. **估算数据膨胀量**：`df1.count() * df2.count()` 估算结果集大小。
4. **检查 SQL 中的 Join 条件**：`ON` 子句是否遗漏了关联条件。

### 解决办法

**代码层面避免 Cross Join：**

```sql
-- 确认 Join 条件正确且非空
SELECT *
FROM table_a a
INNER JOIN table_b b ON a.user_id = b.user_id;

-- 如果确实需要 Cross Join，先大幅过滤缩小数据规模
SELECT *
FROM (
  SELECT * FROM table_a WHERE active = true    -- 先过滤
) a
CROSS JOIN (
  SELECT * FROM table_b WHERE in_stock = true  -- 先过滤
) b;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 确认 Join 条件正确且非空
val result = df1.join(df2, df1("user_id") === df2("user_id"), "inner")

// 如果确实需要 Cross Join，添加过滤条件减小规模
val filtered1 = df1.filter(col("active") === true)
val filtered2 = df2.filter(col("in_stock") === true)
val result = filtered1.crossJoin(filtered2)
```

</details>

**使用 Broadcast Nested Loop Join 替代 Cartesian Product：**

```sql
-- 如果一侧数据很小，使用 BROADCAST hint
SELECT /*+ BROADCAST(b) */ *
FROM large_table a
CROSS JOIN small_table b;
```

**参数限制 Cross Join：**

```properties
# 禁止 Cross Join（Spark 2.3+，建议设为 true 防止意外）
spark.sql.crossJoin.enabled=false
```

**替代算法：**

```scala
// 对于用户-商品相似度计算，使用 MinHash / LSH 等近似算法
// 而不是精确计算所有用户×所有商品

// 使用基于聚类的预筛选
// 先按用户兴趣分类，再在每个类内计算相似度
// 数据量：1000 万 × 1000 = 10 亿（而非 1000 亿）
```

---

## 场景21：窗口函数在大数据集上性能差

### 场景描述

某金融风控应用需要对每个账户的交易记录按时间排序后计算滚动 30 天的统计指标：

```sql
SELECT
  account_id,
  txn_date,
  amount,
  SUM(amount) OVER (
    PARTITION BY account_id
    ORDER BY txn_date
    RANGE BETWEEN INTERVAL 30 DAYS PRECEDING AND CURRENT ROW
  ) AS rolling_30d_sum
FROM transactions
```

数据量约 10 亿条交易记录，500 万账户，每个账户的交易数分布不均匀（从 1 条到 10 万条）。任务运行缓慢：
- Sort 和 Window 算子占据了 80% 以上的时间
- 大量 Shuffle（需要按 account_id 重分区）
- 单个 Partition 数据量大时出现 Spill

### 排查思路

1. **查看执行计划**：`EXPLAIN` 查看 `Window` 算子和 `Sort` 算子的数据量。
2. **分析数据分布**：检查是否存在极端账户（交易数特别多的大账户）。
3. **检查 Shuffle 分区数**：Window 函数通常需要按 `PARTITION BY` 列进行 Shuffle。
4. **监控内存使用**：Window 函数需要在内存中维护窗口内的数据。

### 解决办法

**参数调整：**

```properties
# 增加 Shuffle 分区数（如果数据倾斜不严重）
spark.sql.shuffle.partitions=800

# 启用 AQE 自动调整分区
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
```

**SQL 优化：**

```sql
-- 如果只需要当前行和前 30 天的数据，用 ROWS 替代 RANGE
-- ROWS 基于行号，比 RANGE（基于值）快很多
SELECT
  account_id,
  txn_date,
  amount,
  SUM(amount) OVER (
    PARTITION BY account_id
    ORDER BY txn_date
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_sum
FROM transactions
```

**代码优化（加盐打散倾斜 Key）：**

```sql
-- 第一步：识别大账户（交易记录超过阈值的 account_id）
SELECT account_id, COUNT(*) AS txn_count
FROM transactions
GROUP BY account_id
HAVING COUNT(*) > 10000;

-- 第二步：对大账户数据加盐（将倾斜 Key 拆分成多个 Salt Key）
CREATE OR REPLACE TEMP VIEW salted_transactions AS
SELECT
  account_id,
  txn_date,
  amount,
  CONCAT(account_id, '_', CAST(FLOOR(RAND() * 10) AS INT)) AS salted_account
FROM transactions
WHERE account_id IN (/* 热点 account_id 列表 */);

-- 第三步：先在 Salt Key 层面做窗口计算
SELECT
  salted_account,
  txn_date,
  amount,
  SUM(amount) OVER (
    PARTITION BY salted_account
    ORDER BY txn_date
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_sum_partial
FROM salted_transactions;

-- 第四步：去掉 Salt 后缀，做最终聚合
SELECT
  SPLIT(salted_account, '_')[0] AS account_id,
  txn_date,
  SUM(rolling_30d_sum_partial) OVER (
    PARTITION BY SPLIT(salted_account, '_')[0]
    ORDER BY txn_date
    ROWS BETWEEN 30 PRECEDING AND CURRENT ROW
  ) AS rolling_30d_sum
FROM salted_transactions;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 如果有数据倾斜，对大账户单独处理
import org.apache.spark.sql.functions._

// 识别大账户（交易记录超过某个阈值）
val accountSizes = transactions
  .groupBy("account_id").count()
  .cache()

val largeAccounts = accountSizes
  .filter(col("count") > 10000)
  .select("account_id").as[Long].collect().toSet

// 大账户数据：加随机盐分片后计算
val largeData = transactions
  .filter(col("account_id").isin(largeAccounts.toSeq: _*))
val normalData = transactions
  .filter(!col("account_id").isin(largeAccounts.toSeq: _*))

// 正常计算（数据均匀）
val normalResult = normalData.selectExpr("*",
  "SUM(amount) OVER (PARTITION BY account_id ORDER BY txn_date " +
  "ROWS BETWEEN 30 PRECEDING AND CURRENT ROW) AS rolling_30d_sum"
)

// 大账户数据加盐计算
val saltedData = largeData
  .withColumn("salt", (rand() * 10).cast("int"))
  .withColumn("salted_account", concat(col("account_id"), lit("_"), col("salt")))
  // ... 分片处理
```

</details>

**使用近似算法：**

```scala
// 如果不需要 100% 精确，使用近似窗口函数或降低精度
// 使用桶采样替代全量计算
```

---

## 场景22：Delta Lake / Iceberg / Hudi 写入性能问题

### 场景描述

某数据湖项目使用 Delta Lake 存储订单数据，每日需要写入约 2 亿条记录。任务运行中遇到：
- 写入速度极慢，TPS（每秒事务数）只有几百条
- 产生了大量小文件（文件数 5000+，每个几十 MB）
- CHECKPOINT 和日志清理操作耗时很长
- 并发读取和写入时出现冲突

### 排查思路

1. **查看文件列表**：`hdfs dfs -ls -R /delta/table | wc -l` 查看文件数量和大小分布。
2. **检查 Delta 日志**：`_delta_log/` 目录下的事务日志是否过大。
3. **写入并行度分析**：Spark UI 查看写入 Stage 的 Task 数和写入数据量。
4. **Compaction 检查**：是否有定期的 Compaction 作业。
5. **并发分析**：确认是否有多个作业同时写同一个表。

### 解决办法

**写入参数优化：**

```properties
# Delta Lake 写入优化
spark.databricks.delta.optimizeWrite.enabled=true          # 自动优化写入
spark.databricks.delta.autoCompact.enabled=true            # 自动小文件合并
spark.databricks.delta.autoCompact.minNumFiles=50          # 触发紧凑的最小文件数

# 控制写入时的 Shuffle 分区数
spark.sql.shuffle.partitions=200

# Iceberg 写入优化
spark.sql.iceberg.write.format=parquet
spark.sql.iceberg.write.target-file-size-bytes=268435456   # 256MB
spark.sql.iceberg.distribution-mode=hash
```

**代码优化：**

```sql
-- Delta Lake - 写入时控制文件大小
INSERT INTO delta.`/delta/table/path`
SELECT /*+ REPARTITION(100) */ * FROM source_data;

-- Iceberg - 写入优化
INSERT INTO catalog.db.table
SELECT /*+ REPARTITION(200) */ * FROM source_data;

-- Hudi - 写入优化（使用 Spark SQL）
CREATE TABLE hudi_table
USING hudi
OPTIONS (
  hoodie.parquet.small.file.limit = '104857600',
  hoodie.parquet.max.file.size = '268435456',
  hoodie.copyonwrite.insert.auto.split = 'true'
)
AS SELECT * FROM source_data;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// Delta Lake - 写入时控制文件大小
df.repartition(100)  // 控制输出文件数 ≈ 100
  .write
  .mode("overwrite")
  .option("maxRecordsPerFile", 500000)  // 每个文件最大记录数
  .format("delta")
  .save("/delta/table/path")

// Iceberg - 写入优化
df.write
  .format("iceberg")
  .option("write-format", "parquet")
  .option("write.target-file-size-bytes", "268435456")  // 256MB
  .mode("append")
  .save("catalog.db.table")

// Hudi - 写入优化
df.write
  .format("hudi")
  .option("hoodie.parquet.small.file.limit", "104857600")  // 100MB
  .option("hoodie.parquet.max.file.size", "268435456")     // 256MB
  .option("hoodie.copyonwrite.insert.auto.split", "true")
  .mode("append")
  .save("/hudi/table/path")
```

</details>

**定期 Compaction：**

```sql
-- Delta Lake Compaction（优化小文件，建议定期调度）
OPTIMIZE delta_table;

-- Delta Lake Z-Ordering（按高频过滤列排序，加速查询）
OPTIMIZE delta_table ZORDER BY (order_date, customer_id);

-- Iceberg 文件合并
CALL catalog.system.rewrite_data_files('db.table');
CALL catalog.system.rewrite_manifests('db.table');

-- Delta Lake VACUUM（清理旧文件，保留最近 168 小时）
VACUUM delta_table RETAIN 168 HOURS;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// Delta Lake Compaction（建议定期调度）
import io.delta.tables._

DeltaTable.forPath(spark, "/delta/table")
  .optimize()
  .executeCompaction()

// Delta Lake Z-Ordering（按高频过滤列排序，提升查询性能）
DeltaTable.forPath(spark, "/delta/table")
  .optimize()
  .executeZOrderBy("order_date", "customer_id")

// Iceberg 文件合并
spark.sql("CALL catalog.system.rewrite_data_files('db.table')")
spark.sql("CALL catalog.system.rewrite_manifests('db.table')")

// Hudi 聚类 — 在写入时配置
.option("hoodie.clustering.inline", "true")
.option("hoodie.clustering.inline.max.commits", "4")
```

</details>

**Delta Log 清理：**

```sql
-- Delta Lake 清理旧日志
-- 保留最近 30 天的日志，超过的自动清理
ALTER TABLE delta_table SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '30 days',
  'delta.deletedFileRetentionDuration' = '7 days'
);

-- 手动 VACUUM
VACUUM delta_table RETAIN 168 HOURS;
```

---

## 场景23：Driver 端 OOM — 结果集过大

### 场景描述

某数据分析平台的即席查询（Ad-hoc Query），用户执行了一条聚合查询后，在 Driver 端调用 `collect()` 将结果拉取到 Driver：

```sql
-- 聚合产生 1000 万行结果，直接 collect 到 Driver 导致 OOM
SELECT user_id, COUNT(*) AS cnt
FROM huge_table
GROUP BY user_id;
```

```scala
val result = spark.sql("SELECT user_id, count(*) FROM huge_table GROUP BY user_id")
val data = result.collect() // 1000 万行结果 — 危险！
data.foreach(println)
```

Driver 端报错：

```
java.lang.OutOfMemoryError: Java heap space
  at org.apache.spark.sql.catalyst.expressions.UnsafeRow.copy
```

或者：

```
org.apache.spark.SparkException: Total size of serialized results is larger than spark.driver.maxResultSize
```

### 排查思路

1. **估算结果集大小**：聚合后的结果集有多少行、每行多大。
2. **检查 Driver 内存配置**：`spark.driver.memory` 和 `spark.driver.maxResultSize`。
3. **分析 collect 调用位置**：确认是否真的需要把全部数据拉到 Driver。
4. **检查 Spark UI**：Driver 端的 GC 时间是否很长。

### 解决办法

**参数调整：**

```properties
# 增大 Driver 内存
spark.driver.memory=16g

# 增大 Driver 端接收结果的最大大小
spark.driver.maxResultSize=8g

# Driver 端额外 Java 参数
spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -Xmx16g
```

**代码优化（避免 collect）：**

```scala
// 不要使用 collect()
// val allData = df.collect()  // 危险！

// 替代方案1：写入文件再处理
df.write.parquet("/output/path")

// 替代方案2：分批处理
df.foreachPartition { partition =>
  partition.foreach { row =>
    // 在 Executor 上处理每条记录
    processRow(row)
  }
}

// 替代方案3：限制结果数量
df.take(1000) // 而非 collect()

// 替代方案4：使用 toLocalIterator() 分批拉取
val iter = df.toLocalIterator()
while (iter.hasNext && count < maxRows) {
  val row = iter.next()
  // 处理数据
  count += 1
}

// 替代方案5：使用 foreachBatch 或 foreach 写入存储
df.foreachBatch { (batchDF, batchId) =>
  batchDF.write.jdbc(url, table, props)
}
```

**即席查询的最佳实践：**

```sql
-- 使用 LIMIT 限制返回行数
SELECT user_id, count(*) as cnt
FROM huge_table
GROUP BY user_id
ORDER BY cnt DESC
LIMIT 1000;

-- 对于大结果集，写入临时表而非返回 Driver
CREATE OR REPLACE TEMP VIEW temp_result AS
SELECT user_id, count(*) as cnt
FROM huge_table
GROUP BY user_id;
```

---

## 场景24：Kubernetes 上 Spark 的 Pod 频繁驱逐

### 场景描述

某团队将 Spark 任务迁移到 Kubernetes (Spark on K8s) 上运行。任务配置：
- Executor Pod 的 `memory: 4Gi` 和 `cpu: 2`
- JVM 参数 `-Xmx3g`

运行时发现：
- Executor Pod 被 Kubernetes OOMKiller 杀死，状态变为 `OOMKilled`
- `kubectl describe pod` 显示 `Exit Code: 137`（被 SIGKILL 杀死）
- Pod 频繁重启（CrashLoopBackOff）

原因：JVM 堆内存 3GB + 堆外内存（Metaspace + Direct Buffer + JVM 自身开销）超过了 Pod 的 4GB 限制。

### 排查思路

1. **查看 Pod 事件**：`kubectl describe pod <pod-name>` 查看 OOMKilled 事件。
2. **检查 Pod 资源限制**：对比 `resources.limits.memory` 和 JVM `-Xmx` 设置。
3. **JVM 内存使用分析**：NMT（Native Memory Tracking）检查堆外内存使用。
4. **检查节点资源**：`kubectl top nodes` 查看节点内存压力。
5. **分析驱逐原因**：是内存限制（OOMKilled）还是节点资源压力（Evicted）。

### 解决办法

**资源限制调整：**

```properties
# 关键原则：Pod memory limit ≈ JVM Xmx + JVM 堆外内存 overhead
# overhead 通常需要 Xmx 的 10%-20%

# 如果 Executor 配置了 4g 内存
spark.executor.memory=4g
# K8s 的 memory limit 应该更大约 5-6g

# Executor 内存 overhead
spark.executor.memoryOverhead=2g      # 默认 max(384MB, 0.1 * executorMemory)
# 或者按比例
spark.executor.memoryOverheadFactor=0.2
```

**Kubernetes 特定配置：**

```properties
# Spark on K8s - Executor 配置
spark.kubernetes.executor.request.cores=2
spark.kubernetes.executor.limit.cores=2
spark.kubernetes.executor.memoryOverhead=2g

# 完整的资源计算示例：
# executor.memory = 8g → JVM 堆
# executor.memoryOverhead = 2g → 堆外内存
# Pod memory limit = 8g + 2g = 10g

# 使用 memoryOverhead 而非 off-heap 模式
spark.memory.offHeap.enabled=false    # 不使用额外堆外内存
spark.memory.offHeap.size=0

# K8s Pod 驱逐策略
spark.kubernetes.executor.deleteOnTermination=true
spark.kubernetes.executor.gracePeriod=30  # Pod 优雅退出的等待时间（秒）
```

**提交命令示例：**

```bash
spark-submit \
  --master k8s://https://k8s-api-server:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=spark:3.5.0 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.executor.instances=10 \
  --conf spark.executor.memory=8g \
  --conf spark.executor.memoryOverhead=2g \
  --conf spark.kubernetes.executor.request.cores=2 \
  --conf spark.kubernetes.executor.limit.cores=3 \
  --conf spark.driver.memory=4g \
  --conf spark.driver.memoryOverhead=1g \
  local:///opt/spark/examples/jars/my-app.jar
```

**安全公式：**

```
K8s Pod memory limit >= spark.executor.memory + spark.executor.memoryOverhead
# 其中 memoryOverhead = max(384MB, spark.executor.memory * 0.1)
# 建议额外再加 10%-20% 的 Buffer
```

---

## 场景25：AQE（Adaptive Query Execution）相关调优

### 场景描述

Spark 3.0 引入了 AQE（自适应查询执行），可以在运行时根据实际数据统计信息动态优化执行计划。但默认配置可能不符合所有场景：

- AQE 的 Shuffle 分区合并（CoalescePartitions）在某些场景下合并了不该合并的分区，导致并行度不足。
- 倾斜 Join 优化在数据量较小时反而增加了额外开销。
- 动态 Join 策略切换导致了不必要的 Shuffle 数据重排。
- 初期分区数设置过大导致大量小 Shuffle 文件。

### 排查思路

1. **Spark UI SQL 页签**：查看 AQE 应用的优化：`CoalesceShufflePartitions`、`OptimizeSkewedJoin`、`OptimizeLocalShuffleReader`。
2. **检查执行计划变化**：对比开启/关闭 AQE 的执行计划的差异。
3. **分析实际分区大小**：AQE 的核心是运行时统计信息，检查目标分区大小是否合理。
4. **监控 Shuffle 文件数**：AQE 分区合并后 Shuffle 文件数可能达不到理想值。

### 解决办法

**AQE 完整参数调优：**

```properties
# 1. 开启 AQE（Spark 3.0+）
spark.sql.adaptive.enabled=true

# 2. 分区合并优化（Coalesce Shuffle Partitions）
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.coalescePartitions.initialPartitionNum=400     # 初始分区数（不宜太小）
spark.sql.adaptive.coalescePartitions.minPartitionSize=8MB        # 合并后的最小分区大小
spark.sql.adaptive.coalescePartitions.minPartitionNum=20          # 合并后的最小分区数（保证基本并行度）

# 3. 自动分区合并时的目标大小
spark.sql.adaptive.advisoryPartitionSizeInBytes=128MB             # 建议分区大小

# 4. 倾斜 Join 优化
spark.sql.adaptive.skewJoin.enabled=true                          # 开启自动倾斜处理
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5               # 倾斜因子
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes=256MB # 最小倾斜阈值

# 5. Local Shuffle Reader（网络优化）
spark.sql.adaptive.localShuffleReader.enabled=true                # 减少网络数据读取

# 6. 动态 Join 策略切换
spark.sql.adaptive.autoBroadcastJoinThreshold=-1                  # -1 使用 spark.sql.autoBroadcastJoinThreshold
spark.sql.adaptive.nonEmptyPartitionRatioForBroadcastJoin=0.2     # 非空分区比例阈值
```

**针对不同场景的 AQE 建议：**

| 场景 | 建议 |
|------|------|
| 数据量不确定的即席查询 | 开启 AQE，initialPartitionNum 设大（如 800） |
| 数据量稳定的 ETL | 关闭 AQE，手动设置最优 shuffle.partitions |
| 有严重数据倾斜 | 开启 AQE + skewJoin.enabled |
| 小数据量聚合（<1GB） | 开启 AQE，minPartitionNum 不要太小 |
| 流处理 Structured Streaming | 建议关闭 AQE（Micro-batch 间数据量变化大，AQE 重新优化有开销） |

**关闭 AQE 的场景：**

```properties
# 当数据分布已知且稳定，手动调优更优时
spark.sql.adaptive.enabled=false

# 流处理
spark.sql.adaptive.enabled=false  # Structured Streaming 中建议关闭
```

---

## 场景26：多表 Join 顺序导致的性能问题

### 场景描述

某供应链系统需要 Join 5 张表来计算库存周转率：

```sql
SELECT ...
FROM inventory i
JOIN orders o ON i.product_id = o.product_id
JOIN suppliers s ON i.supplier_id = s.id
JOIN warehouses w ON i.warehouse_id = w.id
JOIN products p ON i.product_id = p.id
WHERE o.order_date >= '2024-01-01'
```

其中：
- `orders` 表：10 亿条（含历史所有订单）
- `products` 表：10 万条（产品主数据）
- `suppliers` 表：5000 条（供应商）
- `warehouses` 表：200 条（仓库）
- `inventory` 表：1 亿条（当前库存）

运行后发现执行计划是：先 join 最大的两张表（inventory × orders），产生巨大中间结果，然后才 join 小表。Shuffle 数据量极大，任务 3 小时未完成。

### 排查思路

1. **查看执行计划**：`EXPLAIN EXTENDED` 或 Spark SQL UI 的 DAG 图。
2. **分析 Join 顺序**：优化器选择的 Join 顺序是否符合"先 Join 小表、先过滤、先聚合"的原则。
3. **检查 CBO**：是否开启了 CBO（Cost-Based Optimizer）来分析统计信息。
4. **检查表统计信息**：是否准确。

### 解决办法

**开启 CBO 自动优化 Join 顺序：**

```properties
# 开启 CBO
spark.sql.cbo.enabled=true
spark.sql.cbo.joinReorder.enabled=true
spark.sql.cbo.joinReorder.dp.star.filter=true  # 星型模型 Join 重排
spark.sql.cbo.joinReorder.dp.threshold=12      # 动态规划深度阈值

# 更新表级统计信息
# ANALYZE TABLE ... COMPUTE STATISTICS
```

**使用 Join Hint 手动指定顺序：**

```sql
SELECT /*+ BROADCAST(p), BROADCAST(s), BROADCAST(w) */ ...
FROM inventory i
JOIN orders o ON i.product_id = o.product_id
JOIN products p ON i.product_id = p.id      -- 先 Broadcast 小表
JOIN suppliers s ON i.supplier_id = s.id    -- 再 Broadcast 小表
JOIN warehouses w ON i.warehouse_id = w.id  -- 最后 Broadcast 小表
```

**代码层面优化 Join 顺序：**

```sql
-- 先过滤 orders 表（减少数据量），再与维度小表 Join，最后 Join 大表
SELECT /*+ BROADCAST(p), BROADCAST(s), BROADCAST(w) */ ...
FROM inventory i
JOIN products p   ON i.product_id = p.id       -- 先 Broadcast Join 小表
JOIN suppliers s  ON i.supplier_id = s.id      -- 再 Broadcast Join 小表
JOIN warehouses w ON i.warehouse_id = w.id     -- 再 Broadcast Join 小表
JOIN (
  SELECT * FROM orders WHERE order_date >= '2024-01-01'  -- 早过滤
) o ON i.product_id = o.product_id;            -- 最后 Join 已过滤的大表
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 先过滤 orders 表（减少数据量）
val recentOrders = orders.filter(col("order_date") >= "2024-01-01")

// 先用小表过滤大表
val inventoryWithProduct = inventory
  .join(broadcast(products), "product_id")       // Broadcast Join (小表)
  .join(broadcast(suppliers), "supplier_id")     // Broadcast Join (小表)
  .join(broadcast(warehouses), "warehouse_id")   // Broadcast Join (小表)

// 最后 Join 大表
val result = inventoryWithProduct
  .join(recentOrders, "product_id")              // SortMerge Join (大表)
```

</details>

**多表 Join 优化原则：**

1. **先过滤后 Join**：每个表在 Join 前先应用过滤条件。
2. **先 Join 小表**：将维度小表先 Broadcast Join。
3. **先聚合后 Join**：如果可能，先对数据进行预聚合减少数据量。
4. **选择正确的 Join 类型**：如果不需要不匹配的记录，使用 `INNER JOIN` 而非 `LEFT JOIN`。

---

## 场景27：RDD/DataFrame 缓存策略不当

### 场景描述

某 ETL 任务中，一个清洗后的 DataFrame 被多次使用（先写入 HDFS，再计算统计指标，再生成报表）。开发者对此 DataFrame 使用了 `cache()`。但后续观察到：

- 缓存命中了（Storage 页签显示 100% Cached），但 Task 仍然慢
- 缓存的数据发生了 Spill（部分数据溢出到磁盘）
- 读取缓存时的反序列化开销很大
- 缓存占用了过多内存，导致后续 Shuffle 操作的内存不足（Execution 内存不够，产生大量 Spill）

另一个场景：使用了 `MEMORY_ONLY` 策略，结果数据量超出内存，Task 需要重新计算（无缓存可用）。

### 排查思路

1. **Spark UI Storage 页签**：查看缓存数据的大小、缓存级别、是否 Spill。
2. **RDD/DataFrame 依赖图**：理解哪些 RDD 被多次使用，哪些只需要一次。
3. **分析数据大小**：使用 `df.count()` 和 `spark.table("view").queryExecution.optimizedPlan.stats` 估算缓存数据量。
4. **内存使用分析**：对比 `storage memory` 和 `execution memory` 的使用情况。

### 解决办法

**选择合适的缓存级别：**

```sql
-- SQL 方式缓存表到内存（默认 MEMORY_AND_DISK 级别）
CACHE TABLE cleaned_data;
-- 或指定惰性缓存
CACHE LAZY TABLE cleaned_data;

-- 从缓存中移除
UNCACHE TABLE cleaned_data;
```

<details>
<summary>DataFrame API 缓存级别设置（点击展开）</summary>

```scala
import org.apache.spark.storage.StorageLevel

// MEMORY_ONLY — 仅内存，内存不够就重算（适合数据 < 内存 60%）
df.persist(StorageLevel.MEMORY_ONLY)

// MEMORY_AND_DISK — 内存不够写到磁盘（适合大多数场景）
df.persist(StorageLevel.MEMORY_AND_DISK)

// MEMORY_AND_DISK_SER — 序列化后存储，省内存但有序列化开销（推荐）
df.persist(StorageLevel.MEMORY_AND_DISK_SER)

// DISK_ONLY — 中等数据量，不需要内存加速（适合大数据但访问频率低）
df.persist(StorageLevel.DISK_ONLY)

// MEMORY_ONLY_2 — 两个节点各存一份副本（高可用场景）
df.persist(StorageLevel.MEMORY_ONLY_2)
```

</details>

**何时使用缓存：**

```sql
-- 场景1：多次使用的中间结果 → 使用缓存
CACHE TABLE cleaned_data AS
SELECT col1, col2, col3
FROM raw_table
WHERE status = 'active';

-- 多次使用缓存的 cleaned_data
SELECT * FROM cleaned_data WHERE col1 > 100;
SELECT col2, COUNT(*) FROM cleaned_data GROUP BY col2;

-- 用完后释放
UNCACHE TABLE cleaned_data;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 场景1：多次使用的中间结果 → 使用
val cleaned = rawDF.filter(...).select(...).cache()
cleaned.write.parquet("/output1")
cleaned.groupBy("key").count().write.parquet("/output2")
cleaned.select("a", "b").write.parquet("/output3")
cleaned.unpersist() // 用完后释放

// 场景2：迭代算法 → 一定要用
var model = initialModel
for (i <- 1 to 100) {
  model = train(df, model)
  model.cache() // 如果模型不大
}

// 场景3：只用一次 → 不要缓存
val result = df.filter(...).groupBy(...).agg(...)
result.write.parquet("/output") // 只使用一次
```

</details>

**参数调整：**

```properties
# 缓存数据压缩（减少磁盘和内存占用）
spark.sql.inMemoryColumnarStorage.compressed=true
spark.sql.inMemoryColumnarStorage.batchSize=20000

# 如果 Storage 内存太高影响 Execution
spark.memory.storageFraction=0.3  # Storage 只占 unified 内存的 30%
```

**使用 Checkpoint 替代 Cache（对于超长血缘链）：**

```scala
// 如果血缘链很长，使用 checkpoint 截断血缘
spark.sparkContext.setCheckpointDir("hdfs:///checkpoint")
val df = complexTransformations...dependOnManySteps()
df.checkpoint() // 物化到磁盘并截断血缘
```

---

## 场景28：Task 调度延迟 — 大量 Task 等待执行

### 场景描述

某日志分析任务配置了 50 个 Executor，每个 Executor 4 核，总计 200 个 Core。Spark 产生了 50000 个 Task。Spark UI 显示：
- 大量 Task 处于 SCHEDULING 状态
- Task Schedule Delay 很高（每个 Task 调度耗时几秒）
- 总任务时间中，调度开销占 30% 以上
- Driver CPU 使用率很高（需要跟踪 50000 个 Task 的状态）

### 排查思路

1. **Spark UI Tasks 页签**：查看 Task 的 Schedule Delay 指标。
2. **分析分区数是否过多**：与总 Core 数对比，理想分区数是 Core 数的 2-3 倍。
3. **Driver 资源**：Driver 的 CPU 和内存是否充足。
4. **Task 大小**：序列化后的 Task（闭包）是否过大。

### 解决办法

**减少 Task 数量：**

```scala
// 增大每个分区的大小，减少分区数
spark.conf.set("spark.sql.files.maxPartitionBytes", "536870912") // 512MB
spark.conf.set("spark.sql.shuffle.partitions", "200") // 而非 2000
```

**增强 Driver：**

```properties
# 调度大量 Task 需要 Driver 有充足的资源
spark.driver.memory=8g
spark.driver.cores=4  # 多核处理调度

# 减小 Task 闭包大小（减少 Driver → Executor 传输开销）
# - 避免在闭包中捕获大对象
# - 使用广播变量代替闭包捕获
```

**代码优化减小闭包：**

```sql
-- 对于大维度表的查询，使用 BROADCAST hint 避免闭包捕获大对象
SELECT /*+ BROADCAST(lookup) */ t.*, l.name
FROM transaction t
JOIN lookup_table l ON t.id = l.id;
```

<details>
<summary>等价的 DataFrame API 写法（点击展开）</summary>

```scala
// 不好的写法：闭包捕获了大对象
val largeLookupMap: Map[Long, String] = loadLargeMap() // 100MB
val result = rdd.map(row => largeLookupMap(row.getLong(0)))

// 好的写法：广播大对象
val broadcastMap = spark.sparkContext.broadcast(largeLookupMap)
val result = rdd.map(row => broadcastMap.value(row.getLong(0)))
```

</details>

**RPC 配置调整：**

```properties
# 减少发送到 Driver 的 RPC 消息大小
spark.rpc.message.maxSize=256

# 减少 Executor 心跳间隔
spark.executor.heartbeatInterval=30s
```

---

## 附录A：通用调优参数速查表

```properties
# ==================== 内存相关 ====================
spark.executor.memory=8g                             # Executor 堆内存
spark.executor.memoryOverhead=2g                     # Executor 堆外内存 (YARN/K8s)
spark.driver.memory=4g                               # Driver 堆内存
spark.driver.memoryOverhead=1g                       # Driver 堆外内存
spark.memory.fraction=0.6                            # Unified 内存占堆的比例
spark.memory.storageFraction=0.5                     # Storage 占 Unified 的比例
spark.memory.offHeap.enabled=false                   # 是否使用堆外内存

# ==================== Shuffle 相关 ====================
spark.sql.shuffle.partitions=200                     # Shuffle 分区数
spark.shuffle.compress=true                          # Shuffle 数据压缩
spark.shuffle.spill.compress=true                    # Spill 数据压缩
spark.io.compression.codec=zstd                      # 压缩算法（lz4/snappy/zstd）
spark.shuffle.service.enabled=true                   # External Shuffle Service
spark.shuffle.sort.bypassMergeThreshold=200          # 旁路排序合并阈值
spark.reducer.maxSizeInFlight=48m                    # 单次 Fetch 数据量上限
spark.shuffle.io.maxRetries=10                       # Shuffle 重试次数
spark.shuffle.io.retryWait=60s                       # 重试等待时间

# ==================== 序列化 ====================
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer=512k
spark.kryoserializer.buffer.max=256m
spark.kryo.registrationRequired=false

# ==================== 动态资源分配 ====================
spark.dynamicAllocation.enabled=false                # 是否开启
spark.dynamicAllocation.minExecutors=5
spark.dynamicAllocation.maxExecutors=100
spark.dynamicAllocation.initialExecutors=20
spark.dynamicAllocation.executorIdleTimeout=120s

# ==================== SQL/DataFrame ====================
spark.sql.adaptive.enabled=true                      # AQE
spark.sql.adaptive.coalescePartitions.enabled=true   # 分区合并
spark.sql.adaptive.skewJoin.enabled=true             # 倾斜 Join
spark.sql.autoBroadcastJoinThreshold=104857600       # 自动广播阈值 (100MB)
spark.sql.broadcastTimeout=600                       # 广播超时
spark.sql.cbo.enabled=true                           # CBO
spark.sql.cbo.joinReorder.enabled=true               # Join 重排
spark.sql.files.maxPartitionBytes=268435456          # 文件分区大小 (256MB)
spark.sql.files.openCostInBytes=16777216             # 打开开销 (16MB)

# ==================== GC ====================
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200
spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:MaxGCPauseMillis=200

# ==================== 推测执行 ====================
spark.speculation=false                              # 是否开启推测执行
spark.speculation.multiplier=3
spark.speculation.quantile=0.9

# ==================== 数据本地性 ====================
spark.locality.wait=5s
spark.locality.wait.process=5s
spark.locality.wait.node=8s
spark.locality.wait.rack=10s

# ==================== 网络 ====================
spark.network.timeout=600s
spark.rpc.askTimeout=600s
spark.storage.blockManagerSlaveTimeoutMs=600s

# ==================== 文件输出 ====================
spark.sql.parquet.filterPushdown=true
spark.sql.orc.filterPushdown=true
spark.sql.hive.convertMetastoreParquet=true
spark.sql.parquet.mergeSchema=false
```

---

## 附录B：不同规模集群的推荐配置

### 小型集群（< 50 节点）

```properties
spark.executor.memory=8g
spark.executor.cores=4
spark.executor.instances=20
spark.sql.shuffle.partitions=200
spark.dynamicAllocation.enabled=true
spark.shuffle.service.enabled=true
spark.sql.adaptive.enabled=true
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

### 中型集群（50-200 节点）

```properties
spark.executor.memory=12g
spark.executor.cores=4
spark.executor.instances=100
spark.sql.shuffle.partitions=800
spark.dynamicAllocation.enabled=false    # ETL 建议关闭
spark.shuffle.service.enabled=true
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.initialPartitionNum=800
spark.memory.fraction=0.7
spark.io.compression.codec=zstd
```

### 大型集群（200+ 节点）

```properties
spark.executor.memory=16g
spark.executor.cores=4
spark.executor.instances=300
spark.sql.shuffle.partitions=2400
spark.dynamicAllocation.enabled=false
spark.shuffle.service.enabled=true
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.initialPartitionNum=2400
spark.sql.autoBroadcastJoinThreshold=209715200  # 200MB
spark.memory.fraction=0.75
spark.memory.storageFraction=0.3
spark.io.compression.codec=zstd
spark.local.dir=/data1/spark,/data2/spark,/data3/spark,/data4/spark
spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35
spark.speculation=false
```

---

## 附录C：常用诊断命令和工具

### Spark UI 关键指标

| 指标 | 含义 | 排查方向 |
|------|------|---------|
| Shuffle Read Size | Shuffle 读取数据量 | 数据倾斜、压缩 |
| Shuffle Write Size | Shuffle 写入数据量 | 减少 Shuffle 数据量 |
| Spill (Memory) | 内存溢写到磁盘 | 增加内存、减少分区数据 |
| Spill (Disk) | 磁盘溢写 | 同上 |
| GC Time | GC 耗时 | 调整 GC 策略 |
| Scheduler Delay | 调度延迟 | 减少 Task 数 |
| Task Deserialization Time | 反序列化耗时 | 减小闭包大小 |
| Input Size / Records | 输入数据量和条数 | 判断分区是否均衡 |

### 常用诊断命令

```bash
# 查看 YARN 应用状态
yarn application -list
yarn application -status <app_id>
yarn logs -applicationId <app_id>

# 查看 HDFS 文件分布
hdfs fsck /path -files -blocks -locations
hdfs dfs -count -h /path
hdfs dfs -du -h /path

# 查看节点资源
free -h                 # 内存
df -h                   # 磁盘
iostat -x 1             # 磁盘 IO
nvidia-smi              # GPU（如果使用）
```

### 常用分析 SQL

```sql
-- 查看表分区信息
SHOW PARTITIONS table_name;
DESCRIBE EXTENDED table_name;

-- 查看表的统计信息
DESCRIBE FORMATTED table_name;

-- 更新统计信息
ANALYZE TABLE table_name COMPUTE STATISTICS;
ANALYZE TABLE table_name COMPUTE STATISTICS FOR COLUMNS col1, col2;

-- 查看执行计划
EXPLAIN EXTENDED SELECT ...;
EXPLAIN COST SELECT ...;    -- Spark 3.0+
```

---

## 总结

Spark 生产调优是一个系统工程，核心调优方向可以从以下维度展开：

**内存层面**：合理配置 Executor 内存、Unified 内存比例、堆外内存，避免 OOM 和过度 GC。

**并行度层面**：Shuffle 分区数应设置为总 Core 数的 2-3 倍，文件分区大小控制在 128MB-256MB。

**Shuffle 层面**：开启压缩、减少 Shuffle 数据量、使用 External Shuffle Service。

**Join 优化层面**：优先 Broadcast Join、合理使用 AQE 自动倾斜处理、多表 Join 注意顺序。

**代码层面**：避免 UDF、优先内置函数、减少 collect()、使用 reduceByKey 替代 groupByKey、Map 端预聚合。

**平台层面**：选择合适的存储格式（Parquet/ORC）、定期合并小文件、更新表统计信息、合理配置 GC。

**监控层面**：善用 Spark UI 分析 Task 分布、数据倾斜、Shuffle 量、GC 时间等关键指标，用数据驱动调优决策。

**核心原则**：
1. 先度量后优化 — 用 Spark UI 和监控数据找到真正的瓶颈
2. 参数调优是辅助，代码和架构优化才是根本
3. 没有银弹 — 每个场景需要根据实际情况选择最合适的优化方案
4. 数据量决定方案 — 小数据量（<10GB）和大数据量（>1TB）的优化方向差异巨大
