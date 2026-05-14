package com.study.realtime.jobs;

import com.study.realtime.common.AppConfig;
import com.study.realtime.common.ConnectorSqlOptions;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class RealtimeWarehouseJob {

    private static final String JOB_NAME = "rt-demo-main-pipeline";

    public static void main(String[] args) throws Exception {
        AppConfig appConfig = AppConfig.load();
        StreamTableEnvironment tableEnv = FlinkSqlJobSupport.createTableEnv(JOB_NAME, appConfig);

        // 1. 注册 Kafka 事实流源表：订单、支付、浏览。
        createKafkaSources(tableEnv, appConfig);
        // 2. 注册 MySQL 维表 lookup 表：用于实时补齐用户、商品、店铺维度。
        createMysqlDimensionLookupTables(tableEnv, appConfig);
        // 3. 注册 Doris 结果表：覆盖 ODS、DWD、DWS、告警层。
        createDorisSinks(tableEnv, appConfig);
        // 4. 定义订单宽表视图，供后续宽表落地和指标聚合复用。
        createViews(tableEnv);

        StatementSet statementSet = tableEnv.createStatementSet();
        // ODS 明细层：把 Kafka 原始订单事件直接写入 Doris，方便追溯原始订单数据。
        statementSet.addInsertSql("INSERT INTO ods_order_events SELECT order_id, user_id, product_id, quantity, order_amount, order_status, order_time FROM order_events");
        // ODS 明细层：把支付流原样落入 Doris，作为支付明细事实表。
        statementSet.addInsertSql("INSERT INTO ods_payment_events SELECT payment_id, order_id, user_id, product_id, shop_id, payment_amount, payment_status, payment_channel, payment_time FROM payment_events");
        // ODS 明细层：把页面浏览行为落入 Doris，便于后续排查和流量分析。
        statementSet.addInsertSql("INSERT INTO ods_page_view_events SELECT event_id, user_id, page_type, product_id, shop_id, stay_seconds, view_time FROM page_view_events");
        // DWD 宽表层：使用订单事实流 + MySQL lookup 维表，生成可直接查询的订单宽表。
        statementSet.addInsertSql("INSERT INTO dwd_order_wide SELECT * FROM dwd_order_wide_view");

        // DWS 指标层：按 10 秒窗口统计核心交易指标，包括下单量、下单人数和 GMV。
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

        // DWS 指标层：基于订单宽表按商品维度聚合，统计商品销量和销售额。
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

        // DWS 指标层：基于订单宽表按店铺维度聚合，统计店铺订单数和 GMV。
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

        // Alert 告警层：按 1 分钟窗口统计店铺支付失败率，命中阈值后输出告警记录。
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

        // Alert 告警层：按 1 分钟窗口统计页面流量，识别短时间高访问热点。
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

    private static void createKafkaSources(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // 订单事实流：row_time 用于事件时间窗口，pt 用于维表 lookup join。
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
                        + ") WITH (" + ConnectorSqlOptions.kafka(
                        appConfig,
                        appConfig.getRequired("demo.kafka.topic.order"),
                        appConfig.getRequired("demo.kafka.group.order")) + ")");

        // 支付事实流：保留 shop_id，方便直接做支付失败率告警。
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
                        + ") WITH (" + ConnectorSqlOptions.kafka(
                        appConfig,
                        appConfig.getRequired("demo.kafka.topic.payment"),
                        appConfig.getRequired("demo.kafka.group.payment")) + ")");

        // 浏览事实流：用于页面热点和高流量告警分析。
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
                        + ") WITH (" + ConnectorSqlOptions.kafka(
                        appConfig,
                        appConfig.getRequired("demo.kafka.topic.page-view"),
                        appConfig.getRequired("demo.kafka.group.page-view")) + ")");
    }

    private static void createMysqlDimensionLookupTables(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // 用户维表：通过 JDBC lookup 实时补齐用户名、会员等级、城市等维度。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_user_info ("
                        + " user_id BIGINT,"
                        + " user_name STRING,"
                        + " user_level STRING,"
                        + " city STRING,"
                        + " register_time TIMESTAMP(3),"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.mysqlJdbcLookup(appConfig, "rt_user_info") + ")");

        // 商品维表：补齐商品名、类目、价格以及所属店铺。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_product_info ("
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " category STRING,"
                        + " price DECIMAL(10, 2),"
                        + " shop_id BIGINT,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.mysqlJdbcLookup(appConfig, "rt_product_info") + ")");

        // 店铺维表：补齐店铺名称、等级和城市，用于店铺指标和宽表展示。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE mysql_shop_info ("
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " shop_level STRING,"
                        + " city STRING,"
                        + " update_time TIMESTAMP(3)"
                        + ") WITH (" + ConnectorSqlOptions.mysqlJdbcLookup(appConfig, "rt_shop_info") + ")");
    }

    private static void createViews(StreamTableEnvironment tableEnv) {
        // 订单宽表视图：
        // 把订单流分别关联用户、商品、店铺三张维表，形成后续统一复用的宽表语义层。
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

    private static void createDorisSinks(StreamTableEnvironment tableEnv, AppConfig appConfig) {
        // ODS 订单明细落表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_order_events ("
                        + " order_id BIGINT,"
                        + " user_id BIGINT,"
                        + " product_id BIGINT,"
                        + " quantity INT,"
                        + " order_amount DECIMAL(10, 2),"
                        + " order_status STRING,"
                        + " order_time STRING"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_order_events", "ods_order_events_") + ")");

        // ODS 支付明细落表。
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
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_payment_events", "ods_payment_events_") + ")");

        // ODS 浏览明细落表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE ods_page_view_events ("
                        + " event_id BIGINT,"
                        + " user_id BIGINT,"
                        + " page_type STRING,"
                        + " product_id BIGINT,"
                        + " shop_id BIGINT,"
                        + " stay_seconds INT,"
                        + " view_time STRING"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "ods_page_view_events", "ods_page_view_events_") + ")");

        // DWD 订单宽表结果表。
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
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "dwd_order_wide", "dwd_order_wide_") + ")");

        // DWS 核心交易指标结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_trade_metrics_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " order_cnt BIGINT,"
                        + " order_user_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "dws_trade_metrics_10s", "dws_trade_metrics_10s_") + ")");

        // DWS 商品销售指标结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_product_sales_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " product_id BIGINT,"
                        + " product_name STRING,"
                        + " order_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "dws_product_sales_10s", "dws_product_sales_10s_") + ")");

        // DWS 店铺交易指标结果表。
        FlinkSqlJobSupport.executeSql(tableEnv,
                "CREATE TABLE dws_shop_trade_10s ("
                        + " window_start STRING,"
                        + " window_end STRING,"
                        + " shop_id BIGINT,"
                        + " shop_name STRING,"
                        + " order_cnt BIGINT,"
                        + " order_gmv DECIMAL(18, 2)"
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "dws_shop_trade_10s", "dws_shop_trade_10s_") + ")");

        // 店铺支付失败率告警结果表。
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
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "alert_payment_failure_1m", "alert_payment_failure_1m_") + ")");

        // 页面高流量告警结果表。
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
                        + ") WITH (" + ConnectorSqlOptions.doris(appConfig, "alert_high_traffic_1m", "alert_high_traffic_1m_") + ")");
    }
}
