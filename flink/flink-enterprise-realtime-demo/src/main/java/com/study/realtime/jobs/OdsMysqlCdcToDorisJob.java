package com.study.realtime.jobs;

import com.study.realtime.common.AppConfig;
import com.study.realtime.common.ConnectorSqlOptions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class OdsMysqlCdcToDorisJob {

    private static final String JOB_NAME = "rt-demo-ods-mysql-cdc";

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = AppConfig.load();
        StreamTableEnvironment tableEnv = FlinkSqlJobSupport.createTableEnv(JOB_NAME, appConfig);

        // 1. 注册 MySQL CDC 源表，实时采集维表变更。
        createMysqlUserInfoSource(tableEnv, appConfig);
        createMysqlProductInfoSource(tableEnv, appConfig);
        createMysqlShopInfoSource(tableEnv, appConfig);

        // 2. 注册 Doris ODS 结果表，承接维表的实时同步结果。
        createOdsUserInfoSink(tableEnv, appConfig);
        createOdsProductInfoSink(tableEnv, appConfig);
        createOdsShopInfoSink(tableEnv, appConfig);

        StatementSet statementSet = tableEnv.createStatementSet();
        // 把用户维表 CDC 数据写入 ODS，保留最新用户主数据。
        statementSet.addInsertSql("INSERT INTO ods_user_info SELECT * FROM mysql_user_info");
        // 把商品维表 CDC 数据写入 ODS，供后续查询和核对。
        statementSet.addInsertSql("INSERT INTO ods_product_info SELECT * FROM mysql_product_info");
        // 把店铺维表 CDC 数据写入 ODS，形成完整的维度沉淀层。
        statementSet.addInsertSql("INSERT INTO ods_shop_info SELECT * FROM mysql_shop_info");
        statementSet.execute().await();
    }

    private static void createMysqlUserInfoSource(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // MySQL CDC 用户维表：采集 test.rt_user_info 的全量快照和后续增量变更。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (user_id) NOT ENFORCED"
                        + ") WITH (" + ConnectorSqlOptions.mysqlCdc(appConfig, "rt_user_info", "6101-6104") + ")");
    }

    private static void createMysqlProductInfoSource(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // MySQL CDC 商品维表：采集商品名、类目、价格、所属店铺等信息。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (product_id) NOT ENFORCED"
                        + ") WITH (" + ConnectorSqlOptions.mysqlCdc(appConfig, "rt_product_info", "6201-6204") + ")");
    }

    private static void createMysqlShopInfoSource(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // MySQL CDC 店铺维表：采集店铺基础信息，供宽表和分析层使用。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3),"
                        + " PRIMARY KEY (shop_id) NOT ENFORCED"
                        + ") WITH (" + ConnectorSqlOptions.mysqlCdc(appConfig, "rt_shop_info", "6301-6304") + ")");
    }

    private static void createOdsUserInfoSink(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // Doris ODS 用户维表结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_user_info", "ods_user_info_") + ")");
    }

    private static void createOdsProductInfoSink(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // Doris ODS 商品维表结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_product_info", "ods_product_info_") + ")");
    }

    private static void createOdsShopInfoSink(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // Doris ODS 店铺维表结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_shop_info", "ods_shop_info_") + ")");
    }
}
