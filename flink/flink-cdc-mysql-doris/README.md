# Flink CDC：MySQL → Doris 实时同步

本项目通过 **Flink 1.17.1 + Flink CDC 2.4.2 + Flink Doris Connector 1.5.2**，把远程服务器上 MySQL 库 `test.user_info` 表的全量数据 + 增量变更（INSERT/UPDATE/DELETE）实时同步到 Doris 的 `test.user_info` 表。在本地 IDE 直接 Run `main` 方法即可启动，无需独立部署 Flink 集群。

---

## 1. 技术选型

| 组件 | 版本 | 说明 |
| --- | --- | --- |
| Java | **8** | Flink 1.17 官方仅保证 Java 8 / 11 兼容，Java 17/21 在 1.17 上未做完整支持 |
| Flink | **1.17.1** | 题目指定 |
| Flink CDC（MySQL Source） | **2.4.2** | 对应 Flink 1.17 的稳定版（`com.ververica:flink-sql-connector-mysql-cdc`） |
| Flink Doris Connector | **1.5.2** | 对应 Flink 1.17 的稳定版（`org.apache.doris:flink-doris-connector-1.17`） |
| 构建工具 | Maven 3.9 | — |

> 为什么不选 Flink CDC 3.x：3.x 主推 YAML Pipeline 方式，需要单独的 Flink 集群来调度，与"本地 IDE 直接 Run"的需求不匹配。2.4.2 走标准 SQL Connector，本地 MiniCluster 直接跑。

---

## 2. 环境要求

### 本地
- JDK 1.8（项目以 Java 8 编译）。Mac 上路径示例：`/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home`
- Maven 3.6+
- 本地必须能直连远程服务器的以下端口：
  - `3306`（MySQL）
  - `18030`（Doris FE HTTP，**注意：本服务器改成了非默认端口**）
  - `9030`（Doris FE MySQL 协议，可选，用来在本地验证）
  - `18040`（Doris BE webserver，Stream Load 写入端，**也是非默认端口**）

### 远程服务器（bigdata，47.103.24.91）
- MySQL 8.0，binlog 已开启（`log_bin=ON / binlog_format=ROW / server_id=1`）。
- **重要：MySQL 配置了 binlog 白名单 `binlog-do-db=test,aio`**，所以只有 `test` 和 `aio` 两个库的写入会进 binlog。本项目演示库就放在 `test`。如要换库，请同步修改 `/etc/my.cnf` 并重启 mysqld。
- Doris：FE/BE 均已启动。`fe.conf` 中 `http_port=18030, query_port=9030`；`be.conf` 中 `webserver_port=18040, heartbeat_service_port=9050`。

---

## 3. 关键坑（你以后排错时大概率会撞到）

### 3.1 Doris FE/BE 不是默认端口
默认 `8030/8040`，但 `/opt/module/doris/fe/conf/fe.conf` 与 `be/conf/be.conf` 改成了 `18030/18040`。代码与 README 全部以 18030/18040 为准。

### 3.2 Doris BE 注册地址是 127.0.0.1
`be.conf` 里 `priority_networks=127.0.0.1/32`，因此 BE 以 `127.0.0.1:18040` 注册到 FE。Flink Doris Connector 写数据时会向 FE 拉 BE 列表，默认得到的就是 `127.0.0.1:18040`，从本机访问会失败。

**解决：在 Doris sink 的 WITH 参数里显式加 `'benodes' = '47.103.24.91:18040'`，绕过 FE 返回的内网地址，不需要重启 Doris。**

### 3.3 MySQL binlog 白名单导致同步不到增量
`/etc/my.cnf` 里有：
```
binlog-do-db=test
binlog-do-db=aio
```
表示只有 `test` 和 `aio` 库的变更会写 binlog。如果你把库名换成别的（比如 `flinkcdc`），全量没问题，但是 **永远收不到 binlog 事件**，并且不会报错——只能通过 `SHOW MASTER STATUS` 看 Position 是否在变化来排查。

如要使用其它库名，需要把 `binlog-do-db=<新库名>` 加进 `/etc/my.cnf`，然后重启 mysqld。

---

## 4. 项目结构

```
flink-cdc-mysql-doris/
├── pom.xml
├── README.md
└── src/main
    ├── java/com/example/cdc/MysqlToDorisJob.java   # 入口 main
    └── resources/log4j2.xml
```

`MysqlToDorisJob.java` 只做一件事：用 Flink Table API 注册 MySQL CDC 源表 + Doris 目标表，然后 `INSERT INTO doris_sink SELECT ... FROM mysql_source`。

---

## 5. 一次性准备：库表 DDL

### 5.1 MySQL 端（库必须是 `test`）

```sql
-- ssh bigdata 后执行：mysql -uroot -p'Wzq19940920..'
CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET utf8mb4;
USE test;
DROP TABLE IF EXISTS user_info;
CREATE TABLE user_info (
  id BIGINT NOT NULL AUTO_INCREMENT,
  name VARCHAR(64) NOT NULL,
  age INT,
  email VARCHAR(128),
  create_time DATETIME DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO user_info(name,age,email) VALUES
  ('alice',20,'alice@example.com'),
  ('bob',25,'bob@example.com'),
  ('carol',30,'carol@example.com');
```

### 5.2 Doris 端（Unique Key 模型，支持 update/delete）

```sql
-- ssh bigdata 后执行：mysql -h127.0.0.1 -P9030 -uroot -p'Wzq19940920..'
CREATE DATABASE IF NOT EXISTS test;
USE test;
DROP TABLE IF EXISTS user_info;
CREATE TABLE user_info (
  id BIGINT NOT NULL,
  name VARCHAR(64),
  age INT,
  email VARCHAR(128),
  create_time DATETIME,
  update_time DATETIME
) UNIQUE KEY(id)
DISTRIBUTED BY HASH(id) BUCKETS 3
PROPERTIES (
  'replication_num' = '1',
  'enable_unique_key_merge_on_write' = 'true'
);
```

---

## 6. 本地启动

### 方式一：IDEA 直接 Run
1. 用 IntelliJ IDEA 打开 `flink-cdc-mysql-doris` 目录。
2. File → Project Structure → Project，**SDK 选 1.8**，Language level 选 8。
3. 等 Maven 把依赖拉完。
4. 打开 `com.example.cdc.MysqlToDorisJob`，右键 `Run 'MysqlToDorisJob.main()'`。
5. 看到 `Starting Flink Mini Cluster` 之后保持运行即可。

> 如果是 IDEA 2022+，跑的时候报 `module java.base does not opens java.util ...`，那是因为 IDEA 默认用了高版本 JDK。把 Run/Debug Configurations → JRE 切到 JDK 8 即可。

### 方式二：命令行
```bash
cd flink-cdc-mysql-doris
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home \
  mvn -DskipTests exec:java -Dexec.mainClass=com.example.cdc.MysqlToDorisJob
```

### 方式三：打 fat jar（不需要 Flink 集群也能直接 java -jar）
```bash
mvn -DskipTests clean package
JAVA_HOME=/Library/Java/JavaVirtualMachines/temurin-8.jdk/Contents/Home \
  java -jar target/flink-cdc-mysql-doris.jar
```

---

## 7. 验证全量 + 增量

启动后等到日志出现类似 `Binlog offset on checkpoint ...` 即说明全量已结束、进入 binlog 流式阶段。

### 7.1 验证全量
```bash
ssh bigdata "mysql -h127.0.0.1 -P9030 -uroot -p'Wzq19940920..' \
  -e 'SELECT * FROM test.user_info ORDER BY id;'"
```
应能看到 alice / bob / carol 3 行。

### 7.2 验证增量
在 MySQL 端制造变更：
```bash
ssh bigdata "mysql -uroot -p'Wzq19940920..' test <<'SQL'
INSERT INTO user_info(name,age,email) VALUES ('dave',40,'dave@example.com');
UPDATE user_info SET age=21, email='alice+new@example.com' WHERE name='alice';
DELETE FROM user_info WHERE name='bob';
SQL"
```
等约 5–10 秒（一个 checkpoint 周期）后再次查询 Doris，应该看到：
- 新增 `dave`
- `alice` 的 age=21、email 已变
- `bob` 不见了

---

## 8. 核心配置说明（`MysqlToDorisJob.java`）

### MySQL CDC Source
```sql
'connector'        = 'mysql-cdc'
'hostname'         = '47.103.24.91'
'port'             = '3306'
'username'         = 'root'
'password'         = 'Wzq19940920..'
'database-name'    = 'test'
'table-name'       = 'user_info'
'server-id'        = '6400-6404'        -- 多并行度下每个 reader 分配一个 id，避免与现有 slave 冲突
'server-time-zone' = 'Asia/Shanghai'
'scan.startup.mode'= 'initial'          -- 先全量再增量；如只要增量，改成 'latest-offset'
```

### Doris Sink
```sql
'connector'                            = 'doris'
'fenodes'                              = '47.103.24.91:18030'        -- 注意不是 8030
'benodes'                              = '47.103.24.91:18040'        -- 必须显式指定，绕过 BE 内网地址
'table.identifier'                     = 'test.user_info'
'username'                             = 'root'
'password'                             = 'Wzq19940920..'
'sink.label-prefix'                    = 'test_user_info_yyyyMMddHHmmss'  -- 代码里用启动时间戳拼出来，详见 9.1
'sink.enable-2pc'                      = 'true'                      -- 2PC 保证 exactly-once
'sink.enable-delete'                   = 'true'                      -- 透传 DELETE
'sink.properties.format'               = 'json'
'sink.properties.read_json_by_line'    = 'true'
```

### Checkpoint
```java
env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
```
5 秒一次 checkpoint。Doris sink 的 2PC commit 是绑在 checkpoint 上的，所以变更落到 Doris 的延迟基本就是一个 checkpoint 周期。如想更低延迟把 5000 改小即可，代价是 IO 压力变大。

---

## 9. 常见问题

**Q1. 报 `unknowndatabases, dbName=xxx`。**
A. Doris 里没有这个库，或者 `table.identifier` 配错了。先在 Doris 把库表建好（见 5.2）。

**Q2. 全量同步成功，但 MySQL 改数据 Doris 没动。**
A. 99% 是 MySQL binlog 白名单问题。先在服务器上 `SHOW MASTER STATUS;`，做一次 DML 看 Position 是否变化。不变就是 binlog 没记到，需要把库名加到 `binlog-do-db` 或者把 `binlog-do-db` 整段去掉。

**Q3. 报 `Connection refused: /127.0.0.1:18040`。**
A. 没加 `benodes` 参数，FE 把内网注册地址回给了客户端。把 `'benodes' = '47.103.24.91:18040'` 加上即可。

### 9.1 Q4：报 `Load status is Label Already Exists and load job finished, change you label prefix or restore from latest savepoint!`

本地反复 Run → Stop → Run 时常见。Doris 2PC 把 `label-prefix + subtaskIndex + checkpointId` 作为 Stream Load 的 label，作业重启时 Doris Writer 会用这个 label 去 abort 上次残留的事务；但如果那些事务早就 commit 完成了，Doris 端返回 "already finished, can't abort"。

**本项目的处理：给 `sink.label-prefix` 追加一个启动时间戳**，这样每次 JVM 启动都是一个全新的 prefix，不会撞上上次任何已完成的 label。代码里长这样：

```java
String labelPrefix = "test_user_info_"
        + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
```

对开发 / 本地调试非常友好；生产环境如果是从 checkpoint/savepoint 恢复，会把当时的 label-prefix 一起恢复出来，2PC 仍然完整，不影响 exactly-once 语义。

**Q5. IDEA 里 `package com.ververica.cdc... does not exist`。**
A. Maven 没拉到依赖。`mvn -U dependency:resolve` 一下，或在 IDEA 里 Reload All Maven Projects。

**Q6. 多并行度下读 binlog 报 server-id 冲突。**
A. `server-id` 是范围（`6400-6404`），范围长度必须 ≥ source 并行度。当前并行度 1，足够。

---

## 10. 已验证的运行结果

- 启动时间：约 30 秒（包含 MiniCluster 初始化）。
- 全量加载：3 行 → 落 Doris 用时约 10 秒。
- 增量延迟：INSERT/UPDATE/DELETE 在 MySQL 提交后约 5–10 秒（一个 checkpoint 周期）进 Doris。
- EXACTLY_ONCE：Doris sink 开启 2PC，全程 checkpoint 成功。
