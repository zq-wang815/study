# Spark 基础知识储备 — 调优前置

> 本文档面向经验尚浅的数据开发工程师，用通俗的方式解释 Spark 核心概念，为阅读《Spark 生产环境调优完全指南》做知识储备。每个概念都尽量回答三个问题：**它是什么？它为什么重要？它和调优有什么关系？**

---

## 目录

1. [一、Spark 整体架构：谁在干活，谁在指挥](#一spark-整体架构谁在干活谁在指挥)
2. [二、Job → Stage → Task：一个 SQL 是怎么被执行的](#二job--stage--task一个-sql-是怎么被执行的)
3. [三、分区（Partition）与并行度：Spark 并行的基本单位](#三分区partition与并行度spark-并行的基本单位)
4. [四、Shuffle：最昂贵的操作](#四shuffle最昂贵的操作)
5. [五、Spark 内存模型：内存都去哪了](#五spark-内存模型内存都去哪了)
6. [六、Join 的三种策略：两张表是怎么关联的](#六join-的三种策略两张表是怎么关联的)
7. [七、GC（垃圾回收）：JVM 的"保洁员"](#七gc垃圾回收jvm-的保洁员)
8. [八、序列化：对象如何变成字节流](#八序列化对象如何变成字节流)
9. [九、惰性求值与 DAG：为什么 Spark 代码不会立即执行](#九惰性求值与-dag为什么-spark-代码不会立即执行)
10. [十、Spark UI 快速入门：调优的眼睛](#十spark-ui-快速入门调优的眼睛)

---

## 一、Spark 整体架构：谁在干活，谁在指挥

### 1.1 三个角色

可以把 Spark 应用想象成一个**包工头带一群工人干活**的场景：

```
┌─────────────────────────────────────────────────┐
│                   Driver（包工头）                 │
│  - 解析你的代码，制定执行计划                        │
│  - 分配任务给 Executor                             │
│  - 跟踪每个 Task 的进度                             │
│  - 汇总结果                                        │
└─────────────────────────────────────────────────┘
          │                    │
          │  发任务、收结果      │  发任务、收结果
          ▼                    ▼
┌──────────────────┐  ┌──────────────────┐
│  Executor（工人1） │  │  Executor（工人2） │  ...
│  - 真正干活的地方   │  │  - 真正干活的地方   │
│  - 有内存和 CPU    │  │  - 有内存和 CPU    │
│  - 可以跑多个 Task │  │  - 可以跑多个 Task │
└──────────────────┘  └──────────────────┘
          │                    │
          ▼                    ▼
    ┌──────────────────────────────────┐
    │   Cluster Manager（工头中介）       │
    │   YARN / K8s / Standalone         │
    │   负责分配机器给 Executor            │
    └──────────────────────────────────┘
```

| 角色 | 一句话理解 | 调优相关 |
|------|-----------|---------|
| **Driver** | 指挥者，跑在任意一台机器上 | `spark.driver.memory` 配小了容易 OOM（尤其 collect 大结果集时） |
| **Executor** | 干活的 JVM 进程，分布在集群各节点 | `spark.executor.memory` 是调优最频繁的参数 |
| **Cluster Manager** | 资源调度器（YARN/K8s/Standalone） | 决定了你的任务能拿到多少机器、排队多久 |

### 1.2 Executor 内部结构

每个 Executor 是一个 JVM 进程，内部可以并行执行多个 Task：

```
┌──────────────────────────────────────┐
│            Executor (JVM 进程)        │
│  ┌────────┐ ┌────────┐ ┌────────┐   │
│  │ Task 1 │ │ Task 2 │ │ Task 3 │   │  ← 并行数 = executor.cores
│  └────────┘ └────────┘ └────────┘   │
│                                      │
│  ┌────────────────────────────────┐  │
│  │      统一内存（堆内）             │  │
│  │  ┌───────────┬──────────────┐  │  │
│  │  │ Execution │   Storage    │  │  │
│  │  │ (Shuffle) │ (缓存 RDD)    │  │  │
│  │  └───────────┴──────────────┘  │  │
│  └────────────────────────────────┘  │
└──────────────────────────────────────┘
```

**关键认知：**
- `spark.executor.cores` 决定了这个 Executor 同时能跑几个 Task。
- Executor 的总内存被多个 Task **共享**。如果每个 Task 用太多内存，其他 Task 就没得用。

---

## 二、Job → Stage → Task：一个 SQL 是怎么被执行的

### 2.1 层次关系

这是理解 Spark 执行过程**最关键的一张图**：

```
一个 Spark 应用（Application）
    │
    ├── Job（每个 action 操作触发一个 Job）
    │       │
    │       ├── Stage（以 Shuffle 为边界划分）
    │       │       │
    │       │       ├── Task（Stage 内并行执行的最小单元）
    │       │       ├── Task
    │       │       └── Task ...（一个 Stage 的 Task 数 = 分区数）
    │       │
    │       └── Stage
    │               ├── Task
    │               ├── Task
    │               └── Task ...
    │
    └── Job ...
```

**生活类比：**

- **Application** = 一整天的装修工程
- **Job** = 一个具体的活儿（"铺客厅地砖"），每次 `collect()`/`write()`/`count()` 触发一个
- **Stage** = 这个活儿的一个步骤（"先搬砖"→"再铺砖"），每步之间需要把东西重新归整（Shuffle）
- **Task** = 这个步骤里每个人同时干的同样的活，每人负责一片区域（一个 Partition）

### 2.2 Stage 是怎么划分的

**Stage 的边界 = Shuffle 操作**。哪些操作需要 Shuffle？

```
不需要 Shuffle（Stage 内搞定）：          需要 Shuffle（Stage 边界）：
  - map, filter, flatMap                  - groupByKey, reduceByKey
  - select, where（列裁剪、行过滤）          - join（非 Broadcast Join）
  - 单表聚合（Partial 阶段）                - groupBy / agg
                                           - orderBy / sort
                                           - distinct
                                           - Window 函数（PARTITION BY）
                                           - repartition
```

**一个简单的执行计划示例：**

```sql
SELECT dept, AVG(salary) AS avg_salary
FROM employees
WHERE hire_date >= '2020-01-01'
GROUP BY dept;
```

执行过程：

```
Stage 1（Map 端）:                   Stage 2（Reduce 端）:
  ┌──────────┐                         ┌──────────┐
  │ Scan 表   │                         │ 最终聚合  │
  │ Filter   │  ──Shuffle──▶           │ 输出结果  │
  │ 局部聚合  │  (按 dept 重分区)         │          │
  └──────────┘                         └──────────┘
    ↑                                      ↑
  有 N 个 Task                          有 M 个 Task
  （取决于数据分区数）                    （取决于 spark.sql.shuffle.partitions）
```

### 2.3 为什么理解 Job/Stage/Task 很重要

- **数据倾斜** → 某个 Stage 中有一个 Task 特别慢（处理的数据是别人的 100 倍）
- **OOM** → 某个 Task 处理的数据量超过了单个 Executor 能分配的内存
- **Shuffle 失败** → Stage 之间的数据传输出了问题
- **Task 调度慢** → Task 数量太多（几万个），Driver 调度不过来

---

## 三、分区（Partition）与并行度：Spark 并行的基本单位

### 3.1 什么是分区

**分区是 Spark 中数据的基本组织单位**。一个 RDD/DataFrame 的数据被切分成多个分区，分布在不同的 Executor 上。

```
一个 DataFrame，有 4 个分区：

  分区1      分区2      分区3      分区4
  [a,b,c]   [d,e,f]   [g,h,i]   [j,k,l]
    │          │          │          │
    ▼          ▼          ▼          ▼
  Task 1    Task 2    Task 3    Task 4    ← 4 个 Task 并行执行
    │          │          │          │
    ▼          ▼          ▼          ▼
  Executor1  Executor1  Executor2  Executor2
```

### 3.2 分区的核心规则

| 规则 | 说明 |
|------|------|
| **一个分区 = 一个 Task** | 分区数决定了 Task 数，也就决定了最大并行度 |
| **分区太少** | CPU 闲置（Core 比 Task 多），但每个 Task 内存压力大 |
| **分区太多** | Task 调度开销大（几万个 Task 排队），每个 Task 处理的数据太少 |
| **理想分区大小** | 128MB ~ 256MB（和 HDFS block 大小匹配） |
| **理想分区数** | 总 Core 数的 2~3 倍 |

### 3.3 分区数是怎么决定的

**读取阶段（Map 端）：**

```
读取 HDFS 文件时：
  分区数 ≈ 文件总大小 / spark.sql.files.maxPartitionBytes（默认 128MB）

举例：1GB 文件 → 默认 8 个分区 → 最多 8 个 Task 同时读
```

**Shuffle 后（Reduce 端）：**

```
Shuffle 后的分区数 = spark.sql.shuffle.partitions（默认 200）

举例：GROUP BY 后的结果数据无论多大，都是 200 个分区
      → 如果数据只有 10MB，200 个分区太多了（大部分分区为空）
      → 如果数据有 500GB，200 个分区太少了（单个分区 2.5GB，容易 OOM）
```

### 3.4 一个容易犯的错

```sql
-- 假设集群有 100 个 Executor × 4 核 = 400 个 Core
-- 但 spark.sql.shuffle.partitions 用的是默认值 200

-- 结果：只有 200 个 Task 在跑 Shuffle 后的聚合
-- 剩余 200 个 Core 空转，白白浪费一半算力
```

**修正：** `spark.sql.shuffle.partitions` 应设为 400 × 2 = 800 或 400 × 3 = 1200。

---

## 四、Shuffle：最昂贵的操作

### 4.1 Shuffle 是什么

**Shuffle 就是数据在集群中重新分布的过程。** 当需要把具有相同 Key 的数据汇集到同一个节点时，就发生了 Shuffle。

```
Shuffle 过程（以 GROUP BY user_id 为例）：

   Map 端（读数据、做局部聚合）                
  ┌──────────┐  ┌──────────┐               
  │ user_1:10│  │ user_1:20│               
  │ user_2:15│  │ user_2:25│               
  │ user_3:30│  │ user_3:5 │               
  └──────────┘  └──────────┘               
       │              │
       │   写入本地磁盘（Shuffle Write）  ← 每个 Map Task 把数据按 Key 分桶写入磁盘
       │   然后通知 Reduce 端来拉取
       ▼              ▼
  ┌─────────────────────────┐  ← 数据通过网络传输
  │    Shuffle Read          │
  │    从各 Map 端拉取自己    │
  │    负责的 Key 的数据      │
  └─────────────────────────┘
       │
       ▼
   Reduce 端（做最终聚合）
  ┌──────────┐               
  │ user_1:30│ ← 10+20      
  │ user_2:40│ ← 15+25      
  │ user_3:35│ ← 30+5       
  └──────────┘
```

### 4.2 Shuffle 为什么"贵"

| 成本 | 说明 |
|------|------|
| **磁盘 IO** | Map 端必须把数据写到本地磁盘（Shuffle Write），Reduce 端从磁盘读取（Shuffle Read） |
| **网络传输** | 数据在 Map 端和 Reduce 端之间跨节点传输 |
| **CPU** | 数据需要排序（Sort）或哈希（Hash）来确定发给哪个 Reduce Task |
| **内存** | Reduce 端需要内存缓冲区来接收和合并来自多个 Map 端的数据 |

### 4.3 什么操作会触发 Shuffle

```
一定会触发 Shuffle 的操作：
  ✗ GROUP BY（非 Map 端可完成的全量聚合）
  ✗ JOIN（非 Broadcast Join）
  ✗ ORDER BY（全局排序）
  ✗ DISTINCT
  ✗ Window Function（PARTITION BY 指定的列）
  ✗ repartition()

不会触发 Shuffle 的操作：
  ✓ map, flatMap, filter
  ✓ select（列裁剪）
  ✓ where（行过滤）
  ✓ coalesce()（减少分区，不重新分布）
  ✓ 局部聚合（Map 端的预聚合，在 Shuffle Write 之前做）
```

### 4.4 一个关键优化：Map 端预聚合

```
不做 Map 端预聚合（groupByKey）：
  Map Task 输出完整的 (user_1, [10,10,10,10,10])  ← 5 条数据全部发出去
  → Shuffle 数据量大 → 磁盘 IO 和网络开销大

做 Map 端预聚合（reduceByKey / DataFrame agg）：
  Map Task 先在自己的数据里做聚合 (user_1, 50)        ← 只发 1 条出去
  → Shuffle 数据量小 → 性能显著提升
```

**这就是为什么文档中反复说"用 DataFrame API 而非 RDD，用 reduceByKey 而非 groupByKey"。**

---

## 五、Spark 内存模型：内存都去哪了

### 5.1 一张图看懂

```
Executor JVM 堆内存（由 spark.executor.memory 设定，比如 8GB）

┌─────────────────────────────────────────────────┐
│             Reserved Memory（预留，300MB）        │  系统保留，不能动
├─────────────────────────────────────────────────┤
│             User Memory（用户内存，占 (1-fraction)） │  存你的自定义对象
│             比如 8G × 0.25 = 2GB                  │
├─────────────────────────────────────────────────┤
│             Unified Memory（统一内存，占 fraction） │  Spark 管理的内存
│             比如 8G × 0.6 = 4.8GB                 │
│  ┌───────────────────┬──────────────────────┐    │
│  │  Storage 内存      │  Execution 内存       │    │
│  │  缓存 RDD/DF       │  Shuffle 缓冲区       │    │
│  │  默认占 50%        │  聚合/Join 临时内存   │    │
│  │                   │  默认占 50%           │    │
│  │  ← 不够时可以被    │  ← 不够时可以抢占    │    │
│  │    Execution 抢占  │    Storage 的内存    │    │
│  └───────────────────┴──────────────────────┘    │
├─────────────────────────────────────────────────┤
│             JVM Overhead（元空间、线程栈等）       │  不在 spark.executor.memory 内
│             由 spark.executor.memoryOverhead 控制 │
└─────────────────────────────────────────────────┘

堆外内存（Off-Heap）：如果启用，在 JVM 堆之外再分配一块内存
```

### 5.2 关键参数速查

| 参数 | 默认值 | 含义 |
|------|--------|------|
| `spark.executor.memory` | 1g | Executor 的 **JVM 堆内存**大小 |
| `spark.memory.fraction` | 0.6 | 堆内存中给 **Unified Memory** 的比例（剩余的给 User Memory） |
| `spark.memory.storageFraction` | 0.5 | Unified Memory 中给 **Storage** 的比例（剩余的给 Execution） |
| `spark.executor.memoryOverhead` | max(384MB, 0.1 × executor.memory) | **堆外内存**，JVM 自身开销 + 直接内存 |

### 5.3 算一笔实际的内存账

假设配置：`spark.executor.memory = 10g`，其他参数用默认值。

```
Executor JVM 堆 = 10GB

Reserved Memory   = 300MB                     (固定)
User Memory       = 10G × (1-0.6) = 4.0GB     (存你的对象)
Unified Memory    = 10G × 0.6 = 6.0GB         (Spark 管理)
  ├── Storage     = 6G × 0.5 = 3.0GB          (缓存数据)
  └── Execution   = 6G × 0.5 = 3.0GB          (Shuffle/聚合/Join)

堆外内存（memoryOverhead）= max(384MB, 10G × 10%) = 1GB

Container 总内存（YARN）= 10G + 1G = 11GB
```

**这意味着：**
- Shuffle 操作可用的内存只有约 3GB（Execution 内存）。
- 如果 Shuffle 数据量超过 3GB，就会 **Spill（溢写）** 到磁盘。
- 如果缓存数据超过 3GB（Storage 内存），超出部分也会 Spill 或直接丢弃（取决于缓存策略）。
- User Memory 有 4GB 给自定义对象——如果你的 UDF 创建了大量 String/List，这块也会 OOM。

### 5.4 Spill（溢写）是什么

当 Execution 内存不够放下 Shuffle 数据时，Spark 会把部分数据**临时写到磁盘**。这就是 Spill。

```
正常情况：                              Spill 发生：
  数据在内存中处理（纳秒级）     →        数据先写磁盘再读回（毫秒级，慢 1000 倍+）
  Shuffle Spill (Memory) = 0            Shuffle Spill (Memory) > 0
                                        Shuffle Spill (Disk) > 0
```

**在 Spark UI 中看到 Spill 指标很大 → 说明内存不够，需要增加内存或减少单分区数据量。**

---

## 六、Join 的三种策略：两张表是怎么关联的

### 6.1 Broadcast Hash Join（BHJ）— 最快的 Join

**原理：** 把小表**完整广播**到每个 Executor 的内存中，大表不动。每个 Executor 在本地做 Join，无需 Shuffle。

```
         大表（不动，不需要 Shuffle）
         ┌──────┐  ┌──────┐  ┌──────┐
         │Part 1│  │Part 2│  │Part 3│
         └──────┘  └──────┘  └──────┘
              │          │          │
              ▼          ▼          ▼
         ┌──────────────────────────────┐
         │  小表（广播到每个 Executor）     │
         │  全部数据在内存中               │
         │  在本地直接 Join，不需要 Shuffle │
         └──────────────────────────────┘
```

**触发条件：** 小表大小 < `spark.sql.autoBroadcastJoinThreshold`（默认 10MB）。

**为什么快：** 完全避免了 Shuffle，没有磁盘 IO 和网络传输。

**风险：** 如果小表其实不小（比如 500MB），广播会导致 Driver/Executor OOM。

### 6.2 Sort Merge Join（SMJ）— 最常用的 Join

**原理：** 大表和大表 Join 时的默认策略。两张表都按 Join Key 做 Shuffle + 排序，然后在同一个节点上做归并。

```
大表A                               大表B
  │                                   │
  │ Shuffle + Sort by join_key        │ Shuffle + Sort by join_key
  ▼                                   ▼
┌──────────┐                      ┌──────────┐
│ A 的分区1 │  ←──── 本地归并 ────▶ │ B 的分区1 │
│ (key 1-100)│                    │ (key 1-100)│
└──────────┘                      └──────────┘
  │                                   │
  ▼                                   ▼
结果：key 1-100 的 Join 结果
```

**特点：**
- 需要 Shuffle，耗时但能处理任意大小的表。
- 适合一张表在 `autoBroadcastJoinThreshold` 以上的场景。

### 6.3 Shuffle Hash Join（SHJ）— 较少被选择

**原理：** 两张表都按 Join Key 做 Shuffle（和 SMJ 一样），但小的一侧在内存中建 Hash 表做 Join。不需要排序。

**已被 Sort Merge Join 基本取代**（Spark 默认关闭，需手动开启）。

### 6.4 总结对比

| Join 类型 | Shuffle | 适用场景 | 默认触发条件 |
|-----------|---------|---------|-------------|
| Broadcast Hash Join | **不需要**（只在 Driver→Executor 广播时有网络开销） | 一张表很小 | 小表 < 10MB |
| Sort Merge Join | **需要**（两张表都要 Shuffle） | 两张表都大 | 不满足 BHJ 条件时 |
| Shuffle Hash Join | **需要** | 一张表中等大小 | 需手动开启 |

**调优核心思路：尽量让 Join 变成 Broadcast Join。** 如果小表是 200MB，把阈值调到 500MB 就能自动走 BHJ，性能提升 10 倍以上。

---

## 七、GC（垃圾回收）：JVM 的"保洁员"

### 7.1 GC 是什么

JVM 会自动回收不再使用的对象占用的内存，这个机制叫 GC（Garbage Collection）。

```
类比：餐厅的收盘子服务员

  Young GC（Minor GC）：清理"年轻代"
    → 服务员每隔几分钟来收一次桌上的空盘子
    → 速度快（毫秒级），频率高

  Full GC（Major GC）：清理"老年代"
    → 服务员打烊后深度清理整个餐厅
    → 速度慢（秒级），频率低但影响大
    → 期间整个餐厅暂停营业（Stop-The-World）
```

### 7.2 GC 和 Spark 调优的关系

Spark 是**内存密集型**应用，会频繁创建和销毁对象。GC 直接影响 Spark 性能：

```
GC 正常时：
  任务流畅运行，偶尔有暂停但很短

GC 过于频繁时（"GC Overhead Limit Exceeded"）：
  1. Task 执行过程中不断 GC，真正干活的时间很少
  2. Executor 因为 GC 暂停太久，Driver 以为它挂了
  3. Driver 重新分配 Task，整个 Stage 变慢
  4. 极端情况：Executor 被 YARN/K8s 杀掉
```

### 7.3 常用的 GC 策略

| GC 类型 | 特点 | 推荐场景 |
|---------|------|---------|
| **Parallel GC**（Spark 默认） | 吞吐量优先，但暂停时间长 | Executor 内存 < 4GB |
| **G1GC** | 低延迟，可控制暂停时间 | Executor 内存 > 8GB |
| **CMS** | 并发回收，暂停时间短 | Executor 内存 4~8GB（Spark 3.2+ 已废弃） |

### 7.4 为什么大内存要用 G1GC

```
Parallel GC（默认）：
  ┌────┬────┬────┬────┬────┬────┬────┬────┐
  │正常│正常│正常│正常│ 全  │正常│正常│正常│  ← Full GC 时全部暂停 2~3 秒
  └────┴────┴────┴────┴────┴────┴────┴────┘
                              ↑
                         对 Spark 来说太长了

G1GC：
  ┌────┬────┬────┬────┬────┬────┬────┬────┐
  │正常│正常│正常│正常│正常│正常│正常│正常│  ← 把垃圾回收切成小块做
  └────┴────┴────┴────┴────┴────┴────┴────┘
      ↑    ↑    ↑    ↑    ↑    ↑    ↑    ↑
    每次只暂停几十毫秒，对 Spark 影响很小
```

---

## 八、序列化：对象如何变成字节流

### 8.1 为什么要序列化

在分布式系统中，数据经常需要**从一个进程传到另一个进程**。但内存中的对象（如 Java 的 HashMap）不能直接通过网络发送。序列化就是**把对象变成可以传输的字节流**，反序列化就是**把字节流还原成对象**。

```
Executor A 上的对象              Executor B 上的对象
┌──────────┐                    ┌──────────┐
│ Person { │  ──序列化──▶ [0x7F..] ──反序列化──▶ │ Person { │
│  name:"张三"│   (变成字节)      网络传输    (还原)    │  name:"张三"│
│  age:25  │                                      │  age:25  │
│ }        │                    ◀──反方向同理──      │ }        │
└──────────┘                                      └──────────┘
```

### 8.2 Spark 中有两种序列化器

| 序列化器 | 速度 | 序列化后大小 | 易用性 |
|---------|------|-------------|--------|
| **Java Serializer**（默认） | 慢 | 大（包含大量类元信息） | 无需配置 |
| **Kryo Serializer** | 快（10x） | 小（10x） | 需手动注册类 |

**推荐：** 生产环境务必使用 Kryo。

### 8.3 什么时候发生序列化

Spark 中需要序列化的场景：

1. **Shuffle** — 数据从 Map 端序列化写到磁盘，Reduce 端反序列化读取
2. **广播变量** — Driver 序列化后发给各 Executor，Executor 反序列化
3. **缓存** — 使用 `MEMORY_AND_DISK_SER` 策略时，数据先序列化再存储
4. **闭包** — 算子（map/filter 等）中引用的外部变量需要序列化后发给 Executor

### 8.4 闭包序列化 — 一个常见的坑

```scala
// 有问题的代码
class MyProcessor {
  val largeLookupMap: Map[Long, String] = loadFromDB() // 100MB 的 HashMap

  def process(df: DataFrame): DataFrame = {
    // largeLookupMap 是整个 MyProcessor 实例的一部分
    // 闭包捕获了 this，需要序列化整个 MyProcessor
    df.map(row => largeLookupMap(row.getLong(0))) // BAD
  }
}
```

```
这段代码会序列化整个 MyProcessor 实例（包含 100MB 的 largeLookupMap）
然后发给每个 Executor → Task 启动极慢，甚至 Driver OOM
```

**正确做法：** 用广播变量

```scala
val broadcastMap = spark.sparkContext.broadcast(largeLookupMap)
df.map(row => broadcastMap.value(row.getLong(0))) // GOOD
```

---

## 九、惰性求值与 DAG：为什么 Spark 代码不会立即执行

### 9.1 两个操作类型

Spark 操作分为两种：

```
Transformation（转换操作 — 惰性，不触发计算）：
  map, filter, flatMap, groupBy, join, select, where, orderBy ...
  → 只是"记账"，构建一个执行计划，不真正执行

Action（行动操作 — 触发计算）：
  collect, count, take, show, write, save, foreach ...
  → 真正触发执行，把之前的 Transformation 全部跑一遍
```

### 9.2 一个例子

```scala
val df1 = spark.read.parquet("/data/orders")      // 还没读
val df2 = df1.filter(col("amount") > 100)          // 还没过滤
val df3 = df2.groupBy("user_id").agg(sum("amount")) // 还没聚合

df3.show()  // ← 这一行触发真正的执行！
```

在 `show()` 被调用之前，Spark 只是记录了你想要做什么操作。`show()` 触发后，Spark 才真正去读数据、过滤、聚合。

### 9.3 DAG 是什么

Spark 把你写的 Transformation 串成一个**有向无环图（DAG）**，然后找到最优的执行方式。

```
RDD/DataFrame DAG 示例（一个简单的 Word Count）：

  textFile                         ← Stage 1: 读文件
    │
  flatMap(line → words)            ← Stage 1: 分词（无 Shuffle）
    │
  map(word → (word, 1))            ← Stage 1: 转成键值对（无 Shuffle）
    │
  reduceByKey(_ + _)               ← Stage 2: 聚合（需要 Shuffle！Stage 边界）
    │
  collect                          ← Action：触发执行
```

### 9.4 为什么理解惰性求值和 DAG 很重要

- **调试时**：`df.filter(...)` 不会立即执行，需要用 `show()` 或 `count()` 来触发。
- **性能分析时**：Spark UI 中的 DAG 图就是你的执行计划可视化，Stage 边界 = Shuffle 操作。
- **缓存决策时**：如果你的 DataFrame 被多个 Action 使用，应该在 Transformation 链上 cache()，避免重复计算。
- **排查问题时**：看到血缘链很长（如迭代 100 次），要考虑用 `checkpoint` 截断血缘。

---

## 十、Spark UI 快速入门：调优的眼睛

### 10.1 Spark UI 入口

Spark 应用启动后，可以在以下地址访问 UI：
- YARN 模式：从 YARN ResourceManager UI 的 Application 列表点击 "ApplicationMaster"
- 本地模式：`http://localhost:4040`
- 提交日志中会打印 URL

### 10.2 最重要的几个页签

| 页签 | 看什么 | 调什么 |
|------|--------|--------|
| **Jobs** | 每个 Job 的耗时、Stage 数量 | 哪些 Stage 占比最大，优先优化 |
| **Stages** | 每个 Stage 的 Task 数、Shuffle 量、Spill | 数据倾斜、分区数合理性 |
| **Storage** | 缓存的数据量和缓存级别 | 缓存策略是否合理 |
| **Executors** | 每个 Executor 的 Task 数、GC 时间、内存使用 | Executor 数量和配置是否合理 |
| **SQL** | SQL 的 DAG 图、每个算子的耗时和数据量 | Join 策略、扫描数据量 |
| **Streaming** | 每批数据的输入量、处理延迟 | 流处理的背压问题 |

### 10.3 Stage 页签的关键指标

打开一个 Stage 详情页，**Summary Metrics** 是最重要的诊断数据：

```
Summary Metrics for Stage 3:

  Metric              Min     25th    Median  75th    Max
  ──────────────────────────────────────────────────────
  Duration            2s      5s      8s      12s     3h 21m  ← Max 远超 Median = 数据倾斜！
  Shuffle Read Size   50MB    80MB    100MB   120MB   15GB   ← Max 是 Median 的 150 倍 = 倾斜
  Spill (Memory)      0       50MB    100MB   200MB   5GB    ← 大量 Spill = 内存不够
  GC Time             0.1s    0.3s    0.5s    1s      30s    ← GC 时间过长
  Input Size          128MB   130MB   128MB   130MB   128MB  ← 数据分布均匀 ✓
```

**一眼看出问题的口诀：**

| 现象 | 问题 |
|------|------|
| Duration 的 Max >> Median（数十倍差距） | **数据倾斜** |
| Shuffle Read Size 的 Max >> Median | **数据倾斜** |
| Spill 的 Max 很大 | **内存不够或单分区数据过大** |
| GC Time 很高 | **需要调整 GC 策略或增加内存** |
| Duration 整体很高但分布均匀 | **计算量/数据量大，需要优化代码或增加资源** |
| Task 数远大于 Core 数 | **分区过多，调度开销大** |
| Task 数远小于 Core 数 | **分区不足，有 Core 闲置** |

### 10.4 SQL 页签怎么看

SQL 页签展示了 SQL 的 DAG 图，点击每个节点可以看到：

- **Scan** 节点：扫描了多少文件、多少数据、用了什么过滤（PushedFilters）
- **Exchange** 节点（= Shuffle）：数据量、分区数
- **Join** 节点：用的什么 Join 策略（BroadcastHashJoin / SortMergeJoin / CartesianProduct）

如果看到 `CartesianProduct`，基本确定你的 SQL 写错了（忘了 Join 条件）。

---

## 附录：快速对照表

### 调优参数 → 对应概念

| 参数 | 对应概念（本文第几节） |
|------|----------------------|
| `spark.executor.memory` | 第五节：内存模型 |
| `spark.executor.cores` | 第一节：Executor 内部结构 |
| `spark.sql.shuffle.partitions` | 第三节：分区与并行度 / 第四节：Shuffle |
| `spark.sql.autoBroadcastJoinThreshold` | 第六节：Join 策略 |
| `spark.memory.fraction` | 第五节：内存模型 |
| `spark.memory.storageFraction` | 第五节：内存模型 |
| `spark.serializer` | 第八节：序列化 |
| `spark.shuffle.service.enabled` | 第四节：Shuffle |
| `spark.sql.adaptive.enabled` | AQE 自动优化（进阶） |

### 问题 → 先看哪个概念

| 问题现象 | 先看哪节 |
|---------|---------|
| Executor OOM | 第五节（内存模型）+ 第三节（分区） |
| GC 频繁 / GC Overhead | 第七节（GC） |
| Shuffle Fetch 失败 | 第四节（Shuffle） |
| 数据倾斜（长尾 Task） | 第三节（分区）+ 第四节（Shuffle）+ 第六节（Join） |
| Join 慢 | 第六节（Join 策略） |
| 任务启动慢 / Task 数量爆炸 | 第二节（Job/Stage/Task）+ 第三节（分区） |
| Spill 严重 | 第五节（内存模型） |
| collect OOM | 第一节（Driver 角色）+ 第九节（惰性求值） |

---

有了这些基础概念之后，再回头看《Spark 生产环境调优完全指南》，每个调优场景中提到的参数、操作和排查思路，都能在本文找到对应的知识支撑。
