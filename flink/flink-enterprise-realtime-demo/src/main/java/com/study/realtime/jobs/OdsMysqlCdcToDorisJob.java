package com.study.realtime.jobs;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMysqlCdcToDorisJob {

    private static final String MYSQL_HOST = "47.103.24.91";
    private static final String MYSQL_PORT = "3306";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Wzq19940920..";
    private static final String MYSQL_DATABASE = "test";

    private static final String DORIS_FE = "47.103.24.91:18030";
    private static final String DORIS_BE = "47.103.24.91:18040";
    private static final String DORIS_USERNAME = "root";
    private static final String DORIS_PASSWORD = "Wzq19940920..";
    private static final String DORIS_DATABASE = "rt_demo";

    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = FlinkSqlJobSupport.createTableEnv("rt-demo-ods-mysql-cdc");

        createMysqlUserInfoSource(tableEnv);
        createMysqlProductInfoSource(tableEnv);
        createMysqlShopInfoSource(tableEnv);

        createOdsUserInfoSink(tableEnv);
        createOdsProductInfoSink(tableEnv);
        createOdsShopInfoSink(tableEnv);

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO ods_user_info SELECT * FROM mysql_user_info");
        statementSet.addInsertSql("INSERT INTO ods_product_info SELECT * FROM mysql_product_info");
        statementSet.addInsertSql("INSERT INTO ods_shop_info SELECT * FROM mysql_shop_info");
        statementSet.execute().await();
    }

    private static void createMysqlUserInfoSource(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (user_id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'hostname' = '" + MYSQL_HOST + "',"
                        + " 'port' = '" + MYSQL_PORT + "',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'database-name' = '" + MYSQL_DATABASE + "',"
                        + " 'table-name' = 'rt_user_info',"
                        + " 'server-id' = '6101-6104',"
                        + " 'server-time-zone' = 'Asia/Shanghai',"
                        + " 'scan.startup.mode' = 'initial'"
                        + ")");
    }

    private static void createMysqlProductInfoSource(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (product_id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'hostname' = '" + MYSQL_HOST + "',"
                        + " 'port' = '" + MYSQL_PORT + "',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'database-name' = '" + MYSQL_DATABASE + "',"
                        + " 'table-name' = 'rt_product_info',"
                        + " 'server-id' = '6201-6204',"
                        + " 'server-time-zone' = 'Asia/Shanghai',"
                        + " 'scan.startup.mode' = 'initial'"
                        + ")");
    }

    private static void createMysqlShopInfoSource(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (shop_id) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'mysql-cdc',"
                        + " 'hostname' = '" + MYSQL_HOST + "',"
                        + " 'port' = '" + MYSQL_PORT + "',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'database-name' = '" + MYSQL_DATABASE + "',"
                        + " 'table-name' = 'rt_shop_info',"
                        + " 'server-id' = '6301-6304',"
                        + " 'server-time-zone' = 'Asia/Shanghai',"
                        + " 'scan.startup.mode' = 'initial'"
                        + ")");
    }

    private static void createOdsUserInfoSink(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + dorisOptions("ods_user_info", "ods_user_info_") + ")");
    }

    private static void createOdsProductInfoSink(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + dorisOptions("ods_product_info", "ods_product_info_") + ")");
    }

    private static void createOdsShopInfoSink(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + dorisOptions("ods_shop_info", "ods_shop_info_") + ")");
    }

    static String dorisOptions(String tableName, String labelPrefix) {
        return " 'connector' = 'doris',"
                + " 'fenodes' = '" + DORIS_FE + "',"
                + " 'benodes' = '" + DORIS_BE + "',"
                + " 'table.identifier' = '" + DORIS_DATABASE + "." + tableName + "',"
                + " 'username' = '" + DORIS_USERNAME + "',"
                + " 'password' = '" + DORIS_PASSWORD + "',"
                + " 'sink.label-prefix' = '" + labelPrefix + System.currentTimeMillis() + "',"
                + " 'sink.enable-2pc' = 'true',"
                + " 'sink.enable-delete' = 'true',"
                + " 'sink.properties.format' = 'json',"
                + " 'sink.properties.read_json_by_line' = 'true'";
    }
}
