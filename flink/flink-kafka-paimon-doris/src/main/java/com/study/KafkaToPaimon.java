package com.study;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * Flink job: Kafka → Paimon
 *
 * Reads JSON order events from Kafka, writes to a Paimon table.
 * The Paimon warehouse is configured via PAIMON_WAREHOUSE env var,
 * defaulting to local filesystem for testing.
 *
 * Usage:
 *   export PAIMON_WAREHOUSE=file:///tmp/paimon-warehouse   # local test
 *   export PAIMON_WAREHOUSE=hdfs://localhost:9000/paimon    # with HDFS
 *   mvn exec:java -Dexec.mainClass="com.study.KafkaToPaimon"
 */
public class KafkaToPaimon {

    private static final String KAFKA_BROKERS = "bigdata:9092";
    private static final String TOPIC = "demo_orders";
    private static final String CONSUMER_GROUP = "paimon-kafka-to-paimon";

    public static void main(String[] args) throws Exception {
        String warehouse = System.getenv().getOrDefault(
                "PAIMON_WAREHOUSE", "hdfs://bigdata:9000/paimon");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10_000);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // ── 1. Create Paimon catalog ──
        String catalogDdl = "CREATE CATALOG paimon WITH (\n"
                + "    'type' = 'paimon',\n"
                + "    'warehouse' = '" + warehouse + "'\n"
                + ")";
        try {
            tableEnv.executeSql(catalogDdl);
        } catch (Exception e) {
            // catalog may already exist, ignore
            System.out.println("Catalog may already exist: " + e.getMessage());
        }
        tableEnv.executeSql("USE CATALOG paimon");
        tableEnv.executeSql("CREATE DATABASE IF NOT EXISTS ods");
        tableEnv.executeSql("USE ods");

        // ── 2. Define Kafka source table ──
        tableEnv.executeSql("CREATE TEMPORARY TABLE kafka_source (\n"
                + "    order_id    INT,\n"
                + "    user_id     INT,\n"
                + "    product_name STRING,\n"
                + "    price       DECIMAL(10,2),\n"
                + "    quantity    INT,\n"
                + "    create_time STRING,\n"
                + "    update_time STRING,\n"
                + "    proc_time AS PROCTIME()\n"
                + ") WITH (\n"
                + "    'connector' = 'kafka',\n"
                + "    'topic' = '" + TOPIC + "',\n"
                + "    'properties.bootstrap.servers' = '" + KAFKA_BROKERS + "',\n"
                + "    'properties.group.id' = '" + CONSUMER_GROUP + "',\n"
                + "    'scan.startup.mode' = 'earliest-offset',\n"
                + "    'format' = 'json',\n"
                + "    'json.fail-on-missing-field' = 'false',\n"
                + "    'json.ignore-parse-errors' = 'true'\n"
                + ")");

        // ── 3. Define Paimon sink table ──
        tableEnv.executeSql("CREATE TABLE IF NOT EXISTS orders (\n"
                + "    order_id    INT,\n"
                + "    user_id     INT,\n"
                + "    product_name STRING,\n"
                + "    price       DECIMAL(10,2),\n"
                + "    quantity    INT,\n"
                + "    create_time STRING,\n"
                + "    update_time STRING,\n"
                + "    PRIMARY KEY (order_id) NOT ENFORCED\n"
                + ") WITH (\n"
                + "    'bucket' = '4',\n"
                + "    'changelog-producer' = 'input'\n"
                + ")");

        // ── 4. Execute INSERT INTO … SELECT ──
        tableEnv.executeSql("INSERT INTO orders\n"
                + "SELECT order_id, user_id, product_name, price, quantity,\n"
                + "       create_time, update_time\n"
                + "FROM kafka_source");

        System.out.println("============================================");
        System.out.println("  Job submitted: Kafka → Paimon");
        System.out.println("  Kafka  : " + KAFKA_BROKERS + " / " + TOPIC);
        System.out.println("  Paimon : " + warehouse + "/ods.db/orders");
        System.out.println("============================================");
    }
}
