package com.example.cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class OrderSalesAggregationJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        String labelPrefix = "sales_agg_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE kafka_order_sales (\n" +
                "  order_id BIGINT,\n" +
                "  user_id BIGINT,\n" +
                "  product_name STRING,\n" +
                "  price DECIMAL(10,2),\n" +
                "  quantity INT,\n" +
                "  create_time TIMESTAMP(3),\n" +
                "  update_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = 'order_sales',\n" +
                "  'properties.bootstrap.servers' = 'bigdata:9092',\n" +
                "  'properties.group.id' = 'flink-sales-agg',\n" +
                "  'scan.startup.mode' = 'earliest-offset',\n" +
                "  'format' = 'json',\n" +
                "  'json.fail-on-missing-field' = 'false',\n" +
                "  'json.ignore-parse-errors' = 'true'\n" +
                ")"
        );

        tEnv.executeSql(
                "CREATE TABLE doris_product_sales (\n" +
                "  product_name STRING,\n" +
                "  total_sales DECIMAL(15,2),\n" +
                "  PRIMARY KEY (product_name) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '47.103.24.91:18030',\n" +
                "  'benodes' = '47.103.24.91:18040',\n" +
                "  'table.identifier' = 'test.product_sales',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wzq19940920..',\n" +
                "  'sink.label-prefix' = '" + labelPrefix + "',\n" +
                "  'sink.enable-2pc' = 'true',\n" +
                "  'sink.enable-delete' = 'true',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true'\n" +
                ")"
        );

        tEnv.executeSql(
                "INSERT INTO doris_product_sales\n" +
                "SELECT product_name, SUM(price * quantity) AS total_sales\n" +
                "FROM kafka_order_sales\n" +
                "GROUP BY product_name"
        );
    }
}
