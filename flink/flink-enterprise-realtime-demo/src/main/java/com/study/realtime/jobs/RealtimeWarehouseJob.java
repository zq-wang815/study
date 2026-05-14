package com.study.realtime.jobs;

import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RealtimeWarehouseJob {

    private static final String MYSQL_HOST = "47.103.24.91";
    private static final String MYSQL_PORT = "3306";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "Wzq19940920..";
    private static final String MYSQL_DATABASE = "test";

    private static final String KAFKA_BOOTSTRAP = "47.103.24.91:9092";
    private static final String TOPIC_ORDER = "rt_order_events";
    private static final String TOPIC_PAYMENT = "rt_payment_events";
    private static final String TOPIC_PAGE_VIEW = "rt_page_view_events";

    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv = FlinkSqlJobSupport.createTableEnv("rt-demo-main-pipeline");

        createKafkaSources(tableEnv);
        createMysqlDimensionLookupTables(tableEnv);
        createDorisSinks(tableEnv);
        createViews(tableEnv);

        StatementSet statementSet = tableEnv.createStatementSet();
        statementSet.addInsertSql("INSERT INTO ods_order_events SELECT order_id, user_id, product_id, quantity, order_amount, order_status, order_time FROM order_events");
        statementSet.addInsertSql("INSERT INTO ods_payment_events SELECT payment_id, order_id, user_id, product_id, shop_id, payment_amount, payment_status, payment_channel, payment_time FROM payment_events");
        statementSet.addInsertSql("INSERT INTO ods_page_view_events SELECT event_id, user_id, page_type, product_id, shop_id, stay_seconds, view_time FROM page_view_events");
        statementSet.addInsertSql("INSERT INTO dwd_order_wide SELECT * FROM dwd_order_wide_view");

        statementSet.addInsertSql(
                "INSERT INTO dws_trade_metrics_10s "
                        + "SELECT "
                        + " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                        + " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                        + " COUNT(*) AS order_cnt, "
                        + " COUNT(DISTINCT user_id) AS order_user_cnt, "
                        + " CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv "
                        + "FROM TABLE(TUMBLE(TABLE order_events, DESCRIPTOR(row_time), INTERVAL '10' SECONDS)) "
                        + "GROUP BY window_start, window_end");

        statementSet.addInsertSql(
                "INSERT INTO dws_product_sales_10s "
                        + "SELECT "
                        + " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                        + " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                        + " product_id, "
                        + " product_name, "
                        + " COUNT(*) AS order_cnt, "
                        + " CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv "
                        + "FROM TABLE(TUMBLE(TABLE dwd_order_wide_view, DESCRIPTOR(row_time), INTERVAL '10' SECONDS)) "
                        + "GROUP BY window_start, window_end, product_id, product_name");

        statementSet.addInsertSql(
                "INSERT INTO dws_shop_trade_10s "
                        + "SELECT "
                        + " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                        + " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                        + " shop_id, "
                        + " shop_name, "
                        + " COUNT(*) AS order_cnt, "
                        + " CAST(SUM(order_amount) AS DECIMAL(18, 2)) AS order_gmv "
                        + "FROM TABLE(TUMBLE(TABLE dwd_order_wide_view, DESCRIPTOR(row_time), INTERVAL '10' SECONDS)) "
                        + "GROUP BY window_start, window_end, shop_id, shop_name");

        statementSet.addInsertSql(
                "INSERT INTO alert_payment_failure_1m "
                        + "SELECT "
                        + " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                        + " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                        + " shop_id, "
                        + " CAST(COUNT(*) AS BIGINT) AS total_cnt, "
                        + " CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) AS BIGINT) AS fail_cnt, "
                        + " CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(10, 4)) AS fail_rate, "
                        + " 'PAYMENT_FAILURE_RATE_HIGH' AS alert_type, "
                        + " CONCAT('shop_id=', CAST(shop_id AS STRING), ', fail_rate=', CAST(CAST(SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS DECIMAL(10, 4)) AS STRING)) AS alert_message "
                        + "FROM TABLE(TUMBLE(TABLE payment_events, DESCRIPTOR(row_time), INTERVAL '1' MINUTE)) "
                        + "GROUP BY window_start, window_end, shop_id "
                        + "HAVING COUNT(*) >= 3 AND SUM(CASE WHEN payment_status = 'FAILED' THEN 1 ELSE 0 END) * 1.0 / COUNT(*) >= 0.30");

        statementSet.addInsertSql(
                "INSERT INTO alert_high_traffic_1m "
                        + "SELECT "
                        + " DATE_FORMAT(window_start, 'yyyy-MM-dd HH:mm:ss') AS window_start, "
                        + " DATE_FORMAT(window_end, 'yyyy-MM-dd HH:mm:ss') AS window_end, "
                        + " page_type, "
                        + " COALESCE(product_id, -1) AS product_id, "
                        + " COALESCE(shop_id, -1) AS shop_id, "
                        + " CAST(COUNT(*) AS BIGINT) AS pv_cnt, "
                        + " 'HIGH_TRAFFIC' AS alert_type, "
                        + " CONCAT('page_type=', page_type, ', pv_cnt=', CAST(COUNT(*) AS STRING)) AS alert_message "
                        + "FROM TABLE(TUMBLE(TABLE page_view_events, DESCRIPTOR(row_time), INTERVAL '1' MINUTE)) "
                        + "GROUP BY window_start, window_end, page_type, product_id, shop_id "
                        + "HAVING COUNT(*) >= 6");

        statementSet.execute().await();
    }

    private static void createKafkaSources(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE order_events ("
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " product_id BIGINT,"
                        + " quantity INT,"
                        + " order_amount DECIMAL(10, 2),"
                        + " order_status STRING,"
                        + " order_time STRING,"
                        + " row_time AS TO_TIMESTAMP(order_time),"
                        + " pt AS PROCTIME(),"
                        + " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND"
                        + ") WITH (" + kafkaOptions(TOPIC_ORDER, "rt-order-group") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE payment_events ("
                        + " payment_id BIGINT,"
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " payment_amount DECIMAL(10, 2),"
                        + " payment_status STRING,"
                        + " payment_channel STRING,"
                        + " payment_time STRING,"
                        + " row_time AS TO_TIMESTAMP(payment_time),"
                        + " pt AS PROCTIME(),"
                        + " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND"
                        + ") WITH (" + kafkaOptions(TOPIC_PAYMENT, "rt-payment-group") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE page_view_events ("
                        + " event_id BIGINT,"
                        + " user_id BIGINT,"
                        + " page_type STRING,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " stay_seconds INT,"
                        + " view_time STRING,"
                        + " row_time AS TO_TIMESTAMP(view_time),"
                        + " pt AS PROCTIME(),"
                        + " WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND"
                        + ") WITH (" + kafkaOptions(TOPIC_PAGE_VIEW, "rt-pv-group") + ")");
    }

    private static void createMysqlDimensionLookupTables(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH ("
                        + " 'connector' = 'jdbc',"
                        + " 'url' = 'jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE
                        + "?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8',"
                        + " 'table-name' = 'rt_user_info',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'lookup.cache.max-rows' = '1000',"
                        + " 'lookup.cache.ttl' = '10 min'"
                        + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH ("
                        + " 'connector' = 'jdbc',"
                        + " 'url' = 'jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE
                        + "?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8',"
                        + " 'table-name' = 'rt_product_info',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'lookup.cache.max-rows' = '1000',"
                        + " 'lookup.cache.ttl' = '10 min'"
                        + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH ("
                        + " 'connector' = 'jdbc',"
                        + " 'url' = 'jdbc:mysql://" + MYSQL_HOST + ":" + MYSQL_PORT + "/" + MYSQL_DATABASE
                        + "?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=UTF-8',"
                        + " 'table-name' = 'rt_shop_info',"
                        + " 'username' = '" + MYSQL_USERNAME + "',"
                        + " 'password' = '" + MYSQL_PASSWORD + "',"
                        + " 'lookup.cache.max-rows' = '1000',"
                        + " 'lookup.cache.ttl' = '10 min'"
                        + ")");
    }

    private static void createViews(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TEMPORARY VIEW dwd_order_wide_view AS "
                        + "SELECT "
                        + " o.order_id, o.user_id, u.user_name, u.user_level, u.city AS user_city, "
                        + " o.product_id, p.product_name, p.category, p.price, "
                        + " p.shop_id, s.shop_name, s.shop_level, s.city AS shop_city, "
                        + " o.quantity, o.order_amount, o.order_status, o.order_time, o.row_time "
                        + "FROM order_events AS o "
                        + "LEFT JOIN mysql_user_info FOR SYSTEM_TIME AS OF o.pt AS u "
                        + "ON o.user_id = u.user_id "
                        + "LEFT JOIN mysql_product_info FOR SYSTEM_TIME AS OF o.pt AS p "
                        + "ON o.product_id = p.product_id "
                        + "LEFT JOIN mysql_shop_info FOR SYSTEM_TIME AS OF o.pt AS s "
                        + "ON p.shop_id = s.shop_id");
    }

    private static void createDorisSinks(StreamTableEnvironment tableEnv) {
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_order_events ("
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " product_id BIGINT,"
                        + " quantity INT,"
                        + " order_amount DECIMAL(10, 2),"
                        + " order_status STRING,"
                        + " order_time STRING"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("ods_order_events", "ods_order_events_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_payment_events ("
                        + " payment_id BIGINT,"
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " payment_amount DECIMAL(10, 2),"
                        + " payment_status STRING,"
                        + " payment_channel STRING,"
                        + " payment_time STRING"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("ods_payment_events", "ods_payment_events_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_page_view_events ("
                        + " event_id BIGINT,"
                        + " user_id BIGINT,"
                        + " page_type STRING,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " stay_seconds INT,"
                        + " view_time STRING"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("ods_page_view_events", "ods_page_view_events_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dwd_order_wide ("
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " user_city STRING,"
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " shop_city STRING,"
                        + " quantity INT,"
                        + " order_amount DECIMAL(10, 2),"
                        + " order_status STRING,"
                        + " order_time STRING,"
                        + " row_time TIMESTAMP(3)"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("dwd_order_wide", "dwd_order_wide_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_trade_metrics_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " order_cnt BIGINT,"
                        + " order_user_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("dws_trade_metrics_10s", "dws_trade_metrics_10s_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_product_sales_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " order_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("dws_product_sales_10s", "dws_product_sales_10s_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_shop_trade_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " order_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("dws_shop_trade_10s", "dws_shop_trade_10s_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE alert_payment_failure_1m ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " shop_id BIGINT,"
                        + " total_cnt BIGINT,"
                        + " fail_cnt BIGINT,"
                        + " fail_rate DECIMAL(10, 4),"
                        + " alert_type STRING,"
                        + " alert_message STRING"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("alert_payment_failure_1m", "alert_payment_failure_1m_") + ")");

        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE alert_high_traffic_1m ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " page_type STRING,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " pv_cnt BIGINT,"
                        + " alert_type STRING,"
                        + " alert_message STRING"
                        + ") WITH (" + OdsMysqlCdcToDorisJob.dorisOptions("alert_high_traffic_1m", "alert_high_traffic_1m_") + ")");
    }

    private static String kafkaOptions(String topic, String groupId) {
        return " 'connector' = 'kafka',"
                + " 'topic' = '" + topic + "',"
                + " 'properties.bootstrap.servers' = '" + KAFKA_BOOTSTRAP + "',"
                + " 'properties.group.id' = '" + groupId + "',"
                + " 'scan.startup.mode' = 'earliest-offset',"
                + " 'format' = 'json',"
                + " 'json.ignore-parse-errors' = 'true'";
    }
}
