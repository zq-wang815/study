# Flink Kafka Paimon Doris 数据管道

## 架构

```
┌──────────────┐     ┌─────────┐     ┌──────────────────┐     ┌──────────┐
│ DataProducer │ ──→ │  Kafka  │ ──→ │  Flink on YARN   │ ──→ │  Paimon  │
│   (Python)   │     │         │     │   (SQL 流作业)    │     │  (HDFS)  │
└──────────────┘     └─────────┘     └──────────────────┘     └────┬─────┘
                                                                   │
                                                          ┌────────┴─────┐
                                                          │    Doris     │
                                                          │  (外部查询)   │
                                                          └──────────────┘
```

## 补充场景文档

- [Paimon 使用场景全景](./docs/paimon-usage-scenarios.md)

- [Paimon Partial-Update 场景文档：用户画像宽表](./docs/partial-update-user-profile.md)

## 环境

| 组件 | 版本 | 地址 |
|------|------|------|
| Kafka | 2.12-3.9.2 | bigdata:9092 |
| Flink | 1.17.1 | REST bigdata:8083 |
| Paimon | 0.8.2 | HDFS: hdfs://bigdata:9000/paimon |
| Hadoop | 3.1.3 | NameNode: bigdata:9000 |
| Doris | 2.x | FE: bigdata:9030, HTTP: bigdata:8030 |

所有组件部署在 bigdata (172.31.249.211)，路径 `/opt/module/`。

---

## 一、环境准备

### 1.1 HDFS 配置

确保 NameNode 监听所有网卡，允许外部访问。

**core-site.xml**：`fs.defaultFS = hdfs://bigdata:9000`

**hdfs-site.xml**：
```xml
<property>
    <name>dfs.namenode.rpc-bind-host</name>
    <value>0.0.0.0</value>
</property>
<property>
    <name>dfs.replication</name>
    <value>1</value>
</property>
```

重启 HDFS：`sbin/stop-dfs.sh && sbin/start-dfs.sh`

### 1.2 Flink 配置

下载 Paimon jar 到 Flink lib（YARN 模式下会自动随 lib 目录分发到容器）：
```bash
wget -P /opt/module/flink-1.17.1/lib/ \
  https://repo1.maven.org/maven2/org/apache/paimon/paimon-flink-1.17/0.8.2/paimon-flink-1.17-0.8.2.jar
```

**关键**：移除 `flink-table-planner-loader-1.17.1.jar`，替换为 `flink-table-planner_2.12-1.17.1.jar`，否则 Paimon catalog 无法被 Flink 发现。

```bash
rm /opt/module/flink-1.17.1/lib/flink-table-planner-loader-1.17.1.jar
wget -P /opt/module/flink-1.17.1/lib/ \
  https://repo1.maven.org/maven2/org/apache/flink/flink-table-planner_2.12/1.17.1/flink-table-planner_2.12-1.17.1.jar
```

设置 `HADOOP_CLASSPATH` 环境变量（YARN 模式必需，Flink 脚本用它加载 Hadoop/YARN 的 jar）：
```bash
echo 'export HADOOP_CLASSPATH=$(/opt/module/hadoop-3.1.3/bin/hadoop classpath)' >> ~/.bashrc
source ~/.bashrc
```

YARN 无需启动 standalone 集群（`start-cluster.sh`），提交作业时会自动向 YARN 申请资源。

---

## 二、本地构建

### 2.1 项目结构

```
flink/flink-kafka-paimon-doris/
├── pom.xml
└── src/main/
    ├── java/com/study/
    │   ├── KafkaToPaimon.java    # Flink 作业
    │   └── DataProducer.java     # 可选 Java 版数据生成
    └── python/
        └── data_producer.py      # Python 数据生成器（推荐）
```

### 2.2 Maven 依赖管理

所有运行环境已有（Flink lib / Hadoop）的依赖设为 `provided`，通过属性统一控制：

```xml
<properties>
    <provided.scope>provided</provided.scope>
</properties>
```

`provided` 的依赖：
- flink-table-api-java, flink-table-api-java-bridge, flink-table-planner
- flink-streaming-java, flink-clients
- flink-connector-kafka, flink-json
- paimon-flink-1.17, hadoop-client

### 2.3 构建

```bash
cd flink/flink-kafka-paimon-doris
mvn clean package -DskipTests
# jar: target/flink-kafka-paimon-doris-1.0-SNAPSHOT.jar (~2MB)
```

---

## 三、运行

### 3.1 创建 Kafka Topic 并生成数据

```bash
pip install kafka-python
python3 src/main/python/data_producer.py bigdata:9092 demo_orders
```

每 2 秒生成一条随机订单 JSON：
```json
{
  "order_id": 1,
  "user_id": 9180,
  "product_name": "iPhone 15",
  "price": 10348.00,
  "quantity": 1,
  "create_time": "2026-05-13 11:51:36",
  "update_time": "2026-05-13 11:51:36"
}
```

### 3.2 提交 Flink 作业

#### YARN Application 模式（推荐）

```bash
# 1. 上传 jar
scp target/flink-kafka-paimon-doris-1.0-SNAPSHOT.jar bigdata:/opt/module/flink-1.17.1/usrlib/flink-kafka-paimon-doris.jar

# 2. 提交到 YARN（application 模式，main() 在集群内执行）
ssh bigdata "/opt/module/flink-1.17.1/bin/flink run-application -t yarn-application \
  -c com.study.KafkaToPaimon \
  -Dyarn.application.name='KafkaToPaimon' \
  /opt/module/flink-1.17.1/usrlib/flink-kafka-paimon-doris.jar"
```

> Application 模式 vs Per-Job 模式：Application 模式下 `main()` 在 JobManager 容器内执行，客户端提交完即退出，更适合长流作业。无需 `-d`（始终分离模式）。

#### 作业管理

```bash
# 查看 YARN 上的 Flink 应用
yarn application -list

# 查看作业状态
curl http://bigdata:8083/jobs | python3 -m json.tool

# 停止作业
yarn application -kill <application_id>
# 或通过 Dashboard http://bigdata:8083 取消
```

#### Standalone 模式（备选）

```bash
# 先启动集群
/opt/module/flink-1.17.1/bin/start-cluster.sh

# 提交作业
/opt/module/flink-1.17.1/bin/flink run -d \
  -c com.study.KafkaToPaimon /opt/module/flink-1.17.1/usrlib/flink-kafka-paimon-doris.jar
```

作业流程：
1. 创建 Paimon Catalog（warehouse: `hdfs://bigdata:9000/paimon`）
2. 注册 Kafka 源表（topic: `demo_orders`，JSON 格式，`scan.startup.mode = earliest-offset`）
3. 注册 Paimon sink 表（`ods.orders`，主键 order_id，4 个 bucket）
4. `INSERT INTO orders SELECT ... FROM kafka_source` 流式写入

### 3.3 验证

```bash
# Flink 作业状态
curl http://bigdata:8083/jobs | python3 -m json.tool

# HDFS 上的 Paimon 文件
hdfs dfs -ls -R /paimon/ods.db/orders
```

---

## 四、Doris 查询 Paimon

### 4.1 创建 Paimon Catalog

```sql
CREATE CATALOG paimon_ods PROPERTIES (
    "type" = "paimon",
    "paimon.catalog.type" = "filesystem",
    "warehouse" = "hdfs://bigdata:9000/paimon",
    "paimon.catalog.metastore" = "filesystem",
    "paimon.catalog.cache-enabled" = "false",
    "use_meta_cache" = "false"
);
```

> **两个缓存都要关**，`use_meta_cache=false` 是 Doris 内部元数据缓存开关，单独关 `cache-enabled` 不生效。关闭后每次查询自动读 HDFS 最新快照，无需手动 REFRESH。

### 4.2 查询

```sql
SWITCH paimon_ods;
USE ods;

SHOW TABLES;
SELECT COUNT(*) FROM orders;
SELECT order_id, product_name, price, quantity, create_time
FROM orders
WHERE price > 5000
ORDER BY create_time DESC
LIMIT 10;
```

Doris 直接读取 HDFS 上的 Paimon ORC 文件，无需数据导入，查询结果实时反映 Flink 写入的最新数据。

---

## 五、关键踩坑

1. **Flink 1.17 `CREATE CATALOG IF NOT EXISTS` 不支持** — 只能用 `CREATE CATALOG` + try-catch
2. **`flink-table-planner-loader` 隔离类加载** — 导致 Paimon catalog 工厂找不到，需替换为 `flink-table-planner`
3. **kafka-clients 版本冲突** — job jar 和 Flink lib 的 kafka-clients 版本不一致，将所有 Flink 依赖设为 provided，仅打包 slf4j/log4j 即可
4. **HDFS NameNode 默认只绑 127.0.0.1** — 需加 `dfs.namenode.rpc-bind-host=0.0.0.0`
5. **YARN 模式需显式设置 `HADOOP_CLASSPATH`** — 否则 Hadoop 类找不到，无法连接 ResourceManager
