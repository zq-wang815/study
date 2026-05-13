package com.example.cdc;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class MysqlToDorisJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);

        String labelPrefix = "test_user_info_"
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                "CREATE TABLE mysql_user_info (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  email STRING,\n" +
                "  create_time TIMESTAMP(0),\n" +
                "  update_time TIMESTAMP(0),\n" +
                "  PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "  'connector' = 'mysql-cdc',\n" +
                "  'hostname' = '47.103.24.91',\n" +
                "  'port' = '3306',\n" +
                "  'username' = 'root',\n" +
                "  'password' = 'Wzq19940920..',\n" +
                "  'database-name' = 'test',\n" +
                "  'table-name' = 'user_info',\n" +
                "  'server-id' = '6400-6404',\n" +
                "  'server-time-zone' = 'Asia/Shanghai',\n" +
                "  'scan.startup.mode' = 'initial'\n" +
                ")"
        );

        tEnv.executeSql(
                "CREATE TABLE doris_user_info (\n" +
                "  id BIGINT,\n" +
                "  name STRING,\n" +
                "  age INT,\n" +
                "  email STRING,\n" +
                "  create_time TIMESTAMP(0),\n" +
                "  update_time TIMESTAMP(0)\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '47.103.24.91:18030',\n" +
                "  'benodes' = '47.103.24.91:18040',\n" +
                "  'table.identifier' = 'test.user_info',\n" +
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
                "INSERT INTO doris_user_info SELECT id, name, age, email, create_time, update_time FROM mysql_user_info"
        );
    }
}
