package com.example.cdc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaConsumerJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE kafka_order_info (\n" +
                "  order_id BIGINT,\n" +
                "  user_id BIGINT,\n" +
                "  product_name STRING,\n" +
                "  amount DECIMAL(10,2),\n" +
                "  status STRING,\n" +
                "  create_time TIMESTAMP(3),\n" +
                "  update_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'order_info',\n" +
                "  'properties.bootstrap.servers' = 'bigdata:9092',\n" +
                "  'properties.group.id' = 'flink-kafka-consumer',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")"
        );

        // 读取并打印 Kafka 数据
        tEnv.executeSql("SELECT * FROM kafka_order_info").print();

        env.execute("Kafka Consumer Job");
    }
}
